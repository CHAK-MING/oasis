use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use oasis_core::error::CoreError;

use crate::application::ports::repositories::{AgentConfigRepository, NodeRepository};

// 使用 serde 映射 TOML 配置文件的结构
#[derive(Deserialize)]
struct AgentConfig {
    #[serde(default)]
    labels: HashMap<String, toml::Value>,

    #[serde(flatten)]
    runtime_config: HashMap<String, toml::Value>,
}

pub struct ManageAgentConfigUseCase {
    node_repo: Arc<dyn NodeRepository>,
    config_repo: Arc<dyn AgentConfigRepository>,
}

impl ManageAgentConfigUseCase {
    pub fn new(
        node_repo: Arc<dyn NodeRepository>,
        config_repo: Arc<dyn AgentConfigRepository>,
    ) -> Self {
        Self {
            node_repo,
            config_repo,
        }
    }

    /// 将 TOML 值扁平化为 key-value 映射
    pub fn flatten_toml_value(
        prefix: String,
        value: &toml::Value,
        result: &mut HashMap<String, String>,
    ) {
        match value {
            toml::Value::String(s) => {
                result.insert(prefix, s.clone());
            }
            toml::Value::Integer(i) => {
                result.insert(prefix, i.to_string());
            }
            toml::Value::Float(f) => {
                result.insert(prefix, f.to_string());
            }
            toml::Value::Boolean(b) => {
                result.insert(prefix, b.to_string());
            }
            toml::Value::Datetime(dt) => {
                result.insert(prefix, dt.to_string());
            }
            toml::Value::Array(arr) => {
                if arr.iter().all(|v| v.is_str()) {
                    let joined = arr
                        .iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(",");
                    result.insert(prefix, joined);
                } else {
                    for (i, item) in arr.iter().enumerate() {
                        let key = format!("{}.{}", prefix, i);
                        Self::flatten_toml_value(key, item, result);
                    }
                }
            }
            toml::Value::Table(table) => {
                for (k, v) in table {
                    let key = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{}.{}", prefix, k)
                    };
                    Self::flatten_toml_value(key, v, result);
                }
            }
        }
    }

    /// 将 TOML 配置应用到匹配到的节点
    pub async fn apply_toml_config(
        &self,
        selector: &str,
        toml_data: &[u8],
    ) -> Result<u64, CoreError> {
        let toml_str = std::str::from_utf8(toml_data).map_err(|e| CoreError::Internal {
            message: format!("Config bytes not valid UTF-8: {}", e),
        })?;

        let config: AgentConfig = toml::from_str(toml_str).map_err(|e| CoreError::Config {
            message: format!("Invalid config (TOML expected): {}", e),
        })?;

        // 1. Handle [labels] section
        if !config.labels.is_empty() {
            let nodes = self.node_repo.find_by_selector(selector).await?;
            if !nodes.is_empty() {
                let mut labels_to_set = HashMap::new();
                for (k, v) in config.labels {
                    // Convert TOML value to a simple string for labels
                    let value_str = match v {
                        toml::Value::String(s) => s,
                        toml::Value::Integer(i) => i.to_string(),
                        toml::Value::Float(f) => f.to_string(),
                        toml::Value::Boolean(b) => b.to_string(),
                        toml::Value::Datetime(dt) => dt.to_string(),
                        toml::Value::Array(arr) => arr
                            .iter()
                            .map(|val| val.to_string().trim_matches('"').to_string())
                            .collect::<Vec<_>>()
                            .join(","),
                        toml::Value::Table(_) => continue, // Skip tables in labels
                    };
                    labels_to_set.insert(k, value_str);
                }

                for node in &nodes {
                    let mut new_labels = node.labels.labels.clone();
                    new_labels.extend(labels_to_set.clone());
                    self.node_repo.update_labels(&node.id, new_labels).await?;
                }
            }
        }

        // 2. Flatten and apply the rest of the runtime configuration
        let mut flat_runtime_config = HashMap::new();
        for (key, value) in config.runtime_config {
            Self::flatten_toml_value(key, &value, &mut flat_runtime_config);
        }

        if flat_runtime_config.is_empty() {
            // If only labels were updated, we can return the count of affected nodes.
            return Ok(self.node_repo.find_by_selector(selector).await?.len() as u64);
        }

        self.apply_config(selector, &flat_runtime_config).await
    }

    pub async fn apply_config(
        &self,
        selector: &str,
        flat_kv: &std::collections::HashMap<String, String>,
    ) -> Result<u64, CoreError> {
        // 解析并获取匹配的 agent 列表
        let nodes = self.node_repo.find_by_selector(selector).await?;
        let agent_ids: Vec<String> = nodes.into_iter().map(|n| n.id).collect();
        self.config_repo.apply_bulk(&agent_ids, flat_kv).await
    }

    pub async fn get(&self, agent_id: &str, key: &str) -> Result<Option<String>, CoreError> {
        self.config_repo.get(agent_id, key).await
    }

    pub async fn set(&self, agent_id: &str, key: &str, value: &str) -> Result<(), CoreError> {
        self.config_repo.set(agent_id, key, value).await
    }

    pub async fn del(&self, agent_id: &str, key: &str) -> Result<(), CoreError> {
        self.config_repo.del(agent_id, key).await
    }

    /// 列出 agent 的配置键
    pub async fn list_keys(
        &self,
        agent_id: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, CoreError> {
        self.config_repo.list_keys(agent_id, prefix).await
    }

    /// 获取 agent 的配置摘要
    pub async fn get_summary(
        &self,
        agent_id: &str,
    ) -> Result<std::collections::HashMap<String, String>, CoreError> {
        let config = self.config_repo.get_all(agent_id).await?;
        let mut summary = std::collections::HashMap::new();

        // 生成配置摘要
        summary.insert("total_keys".to_string(), config.len().to_string());
        summary.insert("agent_id".to_string(), agent_id.to_string());

        // 按类别统计配置
        let mut categories = std::collections::HashMap::new();
        for key in config.keys() {
            if let Some(category) = key.split('.').next() {
                *categories.entry(category.to_string()).or_insert(0) += 1;
            }
        }

        for (category, count) in categories {
            summary.insert(format!("category_{}", category), count.to_string());
        }

        Ok(summary)
    }

    /// 备份 agent 配置
    pub async fn backup_config(
        &self,
        agent_id: &str,
        format: &str,
    ) -> Result<(Vec<u8>, String), CoreError> {
        let config = self.config_repo.get_all(agent_id).await?;

        let (data, actual_format) = match format.to_lowercase().as_str() {
            "toml" => {
                let mut toml_table = toml::Table::new();
                for (key, value) in config {
                    let mut current = &mut toml_table;
                    let parts: Vec<&str> = key.split('.').collect();

                    for (i, part) in parts.iter().enumerate() {
                        if i == parts.len() - 1 {
                            // 最后一个部分，设置值
                            current.insert(part.to_string(), toml::Value::String(value.clone()));
                        } else {
                            // 中间部分，创建嵌套表
                            current = current
                                .entry(part.to_string())
                                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                                .as_table_mut()
                                .ok_or_else(|| CoreError::Internal {
                                    message: "Failed to create TOML table".to_string(),
                                })?;
                        }
                    }
                }
                let toml_str = toml::to_string(&toml_table).map_err(|e| CoreError::Internal {
                    message: format!("Failed to serialize TOML: {}", e),
                })?;
                (toml_str.into_bytes(), "toml".to_string())
            }
            "json" => {
                let json_value =
                    serde_json::to_value(&config).map_err(|e| CoreError::Internal {
                        message: format!("Failed to serialize JSON: {}", e),
                    })?;
                let json_str =
                    serde_json::to_string_pretty(&json_value).map_err(|e| CoreError::Internal {
                        message: format!("Failed to format JSON: {}", e),
                    })?;
                (json_str.into_bytes(), "json".to_string())
            }
            "yaml" => {
                let yaml_str = serde_yaml::to_string(&config).map_err(|e| CoreError::Internal {
                    message: format!("Failed to serialize YAML: {}", e),
                })?;
                (yaml_str.into_bytes(), "yaml".to_string())
            }
            _ => {
                return Err(CoreError::Config {
                    message: format!("Unsupported format: {}", format),
                });
            }
        };

        Ok((data, actual_format))
    }

    /// 恢复 agent 配置
    pub async fn restore_config(
        &self,
        agent_id: &str,
        data: &[u8],
        format: &str,
        overwrite: bool,
    ) -> Result<(u64, u64), CoreError> {
        let config: std::collections::HashMap<String, String> = match format.to_lowercase().as_str()
        {
            "toml" => {
                let toml_str = std::str::from_utf8(data).map_err(|e| CoreError::Internal {
                    message: format!("Invalid UTF-8: {}", e),
                })?;
                let toml_value: toml::Value =
                    toml::from_str(toml_str).map_err(|e| CoreError::Config {
                        message: format!("Invalid TOML: {}", e),
                    })?;

                let mut config = std::collections::HashMap::new();
                Self::flatten_toml_value("".to_string(), &toml_value, &mut config);
                config
            }
            "json" => {
                let json_str = std::str::from_utf8(data).map_err(|e| CoreError::Internal {
                    message: format!("Invalid UTF-8: {}", e),
                })?;
                serde_json::from_str(json_str).map_err(|e| CoreError::Config {
                    message: format!("Invalid JSON: {}", e),
                })?
            }
            "yaml" => {
                let yaml_str = std::str::from_utf8(data).map_err(|e| CoreError::Internal {
                    message: format!("Invalid UTF-8: {}", e),
                })?;
                serde_yaml::from_str(yaml_str).map_err(|e| CoreError::Config {
                    message: format!("Invalid YAML: {}", e),
                })?
            }
            _ => {
                return Err(CoreError::Config {
                    message: format!("Unsupported format: {}", format),
                });
            }
        };

        let mut restored = 0;
        let mut skipped = 0;

        for (key, value) in config {
            if !overwrite {
                // 检查键是否已存在
                if let Ok(Some(_)) = self.config_repo.get(agent_id, &key).await {
                    skipped += 1;
                    continue;
                }
            }

            self.config_repo.set(agent_id, &key, &value).await?;
            restored += 1;
        }

        Ok((restored, skipped))
    }

    /// 验证配置
    pub async fn validate_config(
        &self,
        agent_id: &str,
        data: &[u8],
        format: &str,
    ) -> Result<(bool, Vec<String>, Vec<String>), CoreError> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // 尝试解析配置
        let _config: std::collections::HashMap<String, String> =
            match format.to_lowercase().as_str() {
                "toml" => {
                    let toml_str = std::str::from_utf8(data).map_err(|e| {
                        errors.push(format!("Invalid UTF-8: {}", e));
                        CoreError::Config {
                            message: format!("Invalid UTF-8: {}", e),
                        }
                    })?;
                    toml::from_str(toml_str).map_err(|e| {
                        errors.push(format!("Invalid TOML: {}", e));
                        CoreError::Config {
                            message: format!("Invalid TOML: {}", e),
                        }
                    })?
                }
                "json" => {
                    let json_str = std::str::from_utf8(data).map_err(|e| {
                        errors.push(format!("Invalid UTF-8: {}", e));
                        CoreError::Config {
                            message: format!("Invalid UTF-8: {}", e),
                        }
                    })?;
                    serde_json::from_str(json_str).map_err(|e| {
                        errors.push(format!("Invalid JSON: {}", e));
                        CoreError::Config {
                            message: format!("Invalid JSON: {}", e),
                        }
                    })?
                }
                "yaml" => {
                    let yaml_str = std::str::from_utf8(data).map_err(|e| {
                        errors.push(format!("Invalid UTF-8: {}", e));
                        CoreError::Config {
                            message: format!("Invalid UTF-8: {}", e),
                        }
                    })?;
                    serde_yaml::from_str(yaml_str).map_err(|e| {
                        errors.push(format!("Invalid YAML: {}", e));
                        CoreError::Config {
                            message: format!("Invalid YAML: {}", e),
                        }
                    })?
                }
                _ => {
                    errors.push(format!("Unsupported format: {}", format));
                    return Ok((false, errors, warnings));
                }
            };

        // 检查 agent 是否存在
        let nodes = self
            .node_repo
            .find_by_selector(&format!("agent_id == \"{}\"", agent_id))
            .await?;
        if nodes.is_empty() {
            warnings.push(format!("Agent {} not found", agent_id));
        }

        Ok((errors.is_empty(), errors, warnings))
    }

    /// 比较配置差异
    pub async fn diff_config(
        &self,
        agent_id: &str,
        from_data: &[u8],
        to_data: &[u8],
        format: &str,
    ) -> Result<Vec<ConfigDiffItem>, CoreError> {
        let from_config: std::collections::HashMap<String, String> =
            self.parse_config_data(from_data, format)?;
        let to_config: std::collections::HashMap<String, String> =
            self.parse_config_data(to_data, format)?;

        let mut changes = Vec::new();

        // 找出新增和修改的键
        for (key, new_value) in &to_config {
            match from_config.get(key) {
                Some(old_value) if old_value != new_value => {
                    changes.push(ConfigDiffItem {
                        key: key.clone(),
                        operation: "modified".to_string(),
                        old_value: Some(old_value.clone()),
                        new_value: Some(new_value.clone()),
                    });
                }
                None => {
                    changes.push(ConfigDiffItem {
                        key: key.clone(),
                        operation: "added".to_string(),
                        old_value: None,
                        new_value: Some(new_value.clone()),
                    });
                }
                _ => {} // 值相同，无变化
            }
        }

        // 找出删除的键
        for key in from_config.keys() {
            if !to_config.contains_key(key) {
                changes.push(ConfigDiffItem {
                    key: key.clone(),
                    operation: "deleted".to_string(),
                    old_value: Some(from_config[key].clone()),
                    new_value: None,
                });
            }
        }

        Ok(changes)
    }

    /// 解析配置数据
    fn parse_config_data(
        &self,
        data: &[u8],
        format: &str,
    ) -> Result<std::collections::HashMap<String, String>, CoreError> {
        match format.to_lowercase().as_str() {
            "toml" => {
                let toml_str = std::str::from_utf8(data).map_err(|e| CoreError::Internal {
                    message: format!("Invalid UTF-8: {}", e),
                })?;
                let toml_value: toml::Value =
                    toml::from_str(toml_str).map_err(|e| CoreError::Config {
                        message: format!("Invalid TOML: {}", e),
                    })?;

                let mut config = std::collections::HashMap::new();
                Self::flatten_toml_value("".to_string(), &toml_value, &mut config);
                Ok(config)
            }
            "json" => {
                let json_str = std::str::from_utf8(data).map_err(|e| CoreError::Internal {
                    message: format!("Invalid UTF-8: {}", e),
                })?;
                serde_json::from_str(json_str).map_err(|e| CoreError::Config {
                    message: format!("Invalid JSON: {}", e),
                })
            }
            "yaml" => {
                let yaml_str = std::str::from_utf8(data).map_err(|e| CoreError::Internal {
                    message: format!("Invalid UTF-8: {}", e),
                })?;
                serde_yaml::from_str(yaml_str).map_err(|e| CoreError::Config {
                    message: format!("Invalid YAML: {}", e),
                })
            }
            _ => Err(CoreError::Config {
                message: format!("Unsupported format: {}", format),
            }),
        }
    }

    /// 清空选择器下所有 agent 的全部配置键（不可回滚）
    pub async fn clear_for_selector(&self, selector: &str) -> Result<(u64, u64), CoreError> {
        let nodes = self.node_repo.find_by_selector(selector).await?;
        let agent_ids: Vec<String> = nodes.into_iter().map(|n| n.id).collect();
        let mut cleared_agents: u64 = 0;
        let mut cleared_keys: u64 = 0;
        for aid in agent_ids {
            let count = self.config_repo.clear_for_agent(&aid).await?;
            if count > 0 {
                cleared_agents += 1;
                cleared_keys += count;
            }
        }
        Ok((cleared_agents, cleared_keys))
    }
}

/// 配置差异项
#[derive(Debug)]
pub struct ConfigDiffItem {
    pub key: String,
    pub operation: String, // "added", "modified", "deleted"
    pub old_value: Option<String>,
    pub new_value: Option<String>,
}
