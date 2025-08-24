use std::collections::HashMap;
use std::sync::Arc;

use crate::application::use_cases::queries::config_format_handler::{
    ConfigFormat, ConfigSerializer,
};
use oasis_core::error::CoreError;

use crate::application::ports::repositories::{AgentConfigRepository, NodeRepository};

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

    /// 将 TOML 配置应用到匹配到的节点
    pub async fn apply_toml_config(
        &self,
        selector: &str,
        toml_data: &[u8],
    ) -> Result<u64, CoreError> {
        // 统一使用 ConfigSerializer 扁平化
        let flat = ConfigSerializer::deserialize(toml_data, &ConfigFormat::Toml)?;

        // 拆分 labels.* 与 runtime kv
        let mut labels_to_set: HashMap<String, String> = HashMap::new();
        let mut runtime_kv: HashMap<String, String> = HashMap::new();
        for (k, v) in flat.into_iter() {
            if let Some(rest) = k.strip_prefix("labels.") {
                labels_to_set.insert(rest.to_string(), v);
            } else {
                runtime_kv.insert(k, v);
            }
        }

        // 应用标签
        if !labels_to_set.is_empty() {
            let nodes = self.node_repo.find_by_selector(selector).await?;
            if !nodes.is_empty() {
                for node in &nodes {
                    let mut new_labels = node.labels.labels.clone();
                    new_labels.extend(labels_to_set.clone());
                    self.node_repo
                        .update_labels(node.id.as_str(), new_labels)
                        .await?;
                }
            }
        }

        // 应用运行时配置
        if runtime_kv.is_empty() {
            return Ok(self.node_repo.find_by_selector(selector).await?.len() as u64);
        }
        self.apply_config(selector, &runtime_kv).await
    }

    pub async fn apply_config(
        &self,
        selector: &str,
        flat_kv: &std::collections::HashMap<String, String>,
    ) -> Result<u64, CoreError> {
        // 解析并获取匹配的 agent 列表
        let nodes = self.node_repo.find_by_selector(selector).await?;
        let agent_ids: Vec<String> = nodes.into_iter().map(|n| n.id.to_string()).collect();
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

        let config_format = ConfigFormat::from_str(format)?;
        let (data, actual_format) = ConfigSerializer::serialize(&config, &config_format)?;

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
        // 使用统一的格式处理器
        let config_format = ConfigFormat::from_str(format)?;
        let config = ConfigSerializer::deserialize(data, &config_format)?;

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

        // 使用统一的格式处理器 - 消除重复代码！
        let config_format = match ConfigFormat::from_str(format) {
            Ok(fmt) => fmt,
            Err(_) => {
                errors.push(format!("Unsupported format: {}", format));
                return Ok((false, errors, warnings));
            }
        };

        // 使用统一验证器
        let _validation_result = match ConfigSerializer::validate(data, &config_format) {
            Ok(warnings_from_validation) => {
                warnings.extend(warnings_from_validation);
            }
            Err((is_valid, errors_from_validation, warnings_from_validation)) => {
                errors.extend(errors_from_validation);
                warnings.extend(warnings_from_validation);
                if !is_valid {
                    return Ok((false, errors, warnings));
                }
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

    /// 解析配置数据 - 使用统一的格式处理器
    fn parse_config_data(
        &self,
        data: &[u8],
        format: &str,
    ) -> Result<std::collections::HashMap<String, String>, CoreError> {
        let config_format = ConfigFormat::from_str(format)?;
        ConfigSerializer::deserialize(data, &config_format)
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
