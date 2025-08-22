use async_trait::async_trait;
use oasis_core::config::{CommonConfig, ComponentConfig, ConfigLoadOptions, ConfigSource};
use oasis_core::error::{CoreError, Result};
use oasis_core::selector::NodeAttributes;
use oasis_core::types::AgentId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 统一的 Agent 配置（合并 Bootstrap 和 Runtime）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(flatten)]
    pub common: CommonConfig,

    /// 静态配置（启动时确定）
    pub agent: AgentSection,

    /// 执行器配置（可动态更新）
    pub executor: ExecutorSection,

    /// 安全配置（可动态更新）
    pub security: SecuritySection,

    /// 节点属性（包含 labels，可动态更新）
    #[serde(flatten)]
    pub attributes: NodeAttributes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSection {
    pub agent_id: AgentId,
    pub max_concurrent_tasks: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorSection {
    pub command_timeout_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySection {
    pub enforce_whitelist: bool,
    pub whitelist_paths: Vec<String>,
    pub file_allowed_roots: Vec<String>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self::with_smart_defaults()
    }
}

impl Default for AgentSection {
    fn default() -> Self {
        Self {
            agent_id: AgentConfig::generate_agent_id(),
            max_concurrent_tasks: 8,
        }
    }
}

impl Default for ExecutorSection {
    fn default() -> Self {
        Self {
            command_timeout_sec: 300,
        }
    }
}

impl Default for SecuritySection {
    fn default() -> Self {
        Self {
            enforce_whitelist: false,
            whitelist_paths: vec![],
            file_allowed_roots: vec![],
        }
    }
}

impl AgentConfig {
    /// 智能默认值生成
    pub fn with_smart_defaults() -> Self {
        let agent_id = Self::generate_agent_id();
        let mut attributes = NodeAttributes::new(agent_id.to_string());
        // 将 environment/region 统一进 labels
        let env = detect_environment();
        attributes.labels.insert("environment".to_string(), env);

        Self {
            common: CommonConfig::default(),
            agent: AgentSection {
                agent_id,
                max_concurrent_tasks: 8,
            },
            security: SecuritySection::default(),
            executor: ExecutorSection::default(),
            attributes,
        }
    }

    /// 从 KV 增量更新
    pub fn apply_kv_updates(&mut self, updates: &HashMap<String, String>) -> Result<()> {
        // 使用 serde_json 进行结构化更新
        let mut current_json = serde_json::to_value(&*self)?;

        for (key, value) in updates {
            let json_value = parse_config_value(value)?;

            // 使用点分隔的路径来设置值
            let path_parts: Vec<&str> = key.split('.').collect();
            set_nested_value(&mut current_json, &path_parts, json_value)?;
        }

        *self = serde_json::from_value(current_json)?;

        // 验证更新后的配置
        self.validate()?;
        Ok(())
    }

    /// 智能配置加载
    pub async fn load_smart() -> Result<Self> {
        // Agent 仅使用默认值 + 环境变量覆盖（不读取任何配置文件）
        let mut cfg = Self::with_smart_defaults();

        // 应用环境变量覆盖
        let env_overrides: HashMap<String, String> = std::env::vars()
            .filter(|(k, _)| k.starts_with("OASIS_AGENT_"))
            .map(|(k, v)| (k.to_lowercase().replace('_', "."), v))
            .collect();
        if !env_overrides.is_empty() {
            cfg.apply_kv_updates(&env_overrides)?;
        }

        Ok(cfg)
    }

    /// 检查命令是否被允许（路径前缀白名单）
    pub fn is_command_allowed(&self, command: &str) -> bool {
        if self.security.enforce_whitelist {
            return self
                .security
                .whitelist_paths
                .iter()
                .any(|p| command.starts_with(p));
        }
        true
    }

    /// 从 K/V 对更新配置（兼容旧接口）
    pub fn update_from_kv(&mut self, updates: &HashMap<String, String>) -> Result<()> {
        self.apply_kv_updates(updates)
    }

    /// 生成 Agent ID
    pub fn generate_agent_id() -> AgentId {
        let hostname = hostname::get()
            .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
            .to_string_lossy()
            .to_string();
        AgentId::new(hostname)
    }
}

#[async_trait]
impl ComponentConfig for AgentConfig {
    type Common = CommonConfig;

    async fn load_from_sources(
        sources: &[ConfigSource],
        options: ConfigLoadOptions,
    ) -> Result<Self> {
        oasis_core::config::load_config(sources, options).await
    }

    fn merge_with_common(&mut self, common: Self::Common) {
        self.common = common;
    }

    fn common(&self) -> &Self::Common {
        &self.common
    }

    fn common_mut(&mut self) -> &mut Self::Common {
        &mut self.common
    }

    fn validate_business_rules(&self) -> Result<()> {
        // Agent 特定的业务规则验证
        if self.agent.agent_id.as_str().is_empty() {
            return Err(CoreError::config_error("agent_id cannot be empty"));
        }

        if self.security.enforce_whitelist && self.security.whitelist_paths.is_empty() {
            return Err(CoreError::config_error(
                "whitelist_paths cannot be empty when enforce_whitelist is true",
            ));
        }

        if self.agent.max_concurrent_tasks == 0 {
            return Err(CoreError::config_error(
                "max_concurrent_tasks must be greater than 0",
            ));
        }

        if self.executor.command_timeout_sec == 0 {
            return Err(CoreError::config_error(
                "command_timeout_sec must be greater than 0",
            ));
        }

        Ok(())
    }

    /// 检查配置是否可以热重载
    fn can_hot_reload(&self, other: &Self) -> bool {
        // 不可热重载的配置项
        if self.agent.agent_id != other.agent.agent_id {
            return false; // agent_id 变更需要重启
        }

        if self.common.nats.url != other.common.nats.url {
            return false; // NATS URL 变更需要重启
        }

        if self.common.nats.tls_required != other.common.nats.tls_required {
            return false; // TLS 要求变更需要重启
        }

        // 其他配置可以热重载
        true
    }

    /// 获取配置摘要
    fn summary(&self) -> HashMap<String, String> {
        let mut summary = HashMap::new();

        // 基本信息
        summary.insert("agent_id".to_string(), self.agent.agent_id.to_string());
        if let Some(env) = self.attributes.labels.get("environment") {
            summary.insert("environment".to_string(), env.clone());
        }
        summary.insert("nats_url".to_string(), self.common.nats.url.clone());
        summary.insert(
            "log_level".to_string(),
            self.common.telemetry.log_level.clone(),
        );
        summary.insert(
            "log_format".to_string(),
            self.common.telemetry.log_format.clone(),
        );

        // 执行器配置
        summary.insert(
            "command_timeout_sec".to_string(),
            self.executor.command_timeout_sec.to_string(),
        );
        summary.insert(
            "max_concurrent_tasks".to_string(),
            self.agent.max_concurrent_tasks.to_string(),
        );

        // 安全配置
        summary.insert(
            "enforce_whitelist".to_string(),
            self.security.enforce_whitelist.to_string(),
        );
        summary.insert(
            "whitelist_paths_count".to_string(),
            self.security.whitelist_paths.len().to_string(),
        );
        summary.insert(
            "file_allowed_roots_count".to_string(),
            self.security.file_allowed_roots.len().to_string(),
        );

        // 节点属性
        if let Some(region) = self.attributes.labels.get("region") {
            summary.insert("region".to_string(), region.clone());
        }
        summary.insert(
            "groups_count".to_string(),
            self.attributes.groups.len().to_string(),
        );
        summary.insert(
            "labels_count".to_string(),
            self.attributes.labels.len().to_string(),
        );
        summary.insert("version".to_string(), self.attributes.version.clone());

        summary
    }
}

fn detect_environment() -> String {
    std::env::var("ENVIRONMENT")
        .or_else(|_| std::env::var("ENV"))
        .unwrap_or_else(|_| "prod".to_string())
}

fn parse_config_value(value: &str) -> Result<serde_json::Value> {
    // 尝试解析为 JSON
    if let Ok(json_value) = serde_json::from_str(value) {
        return Ok(json_value);
    }

    // 尝试解析为布尔值
    if let Ok(bool_value) = value.parse::<bool>() {
        return Ok(serde_json::Value::Bool(bool_value));
    }

    // 尝试解析为数字
    if let Ok(num_value) = value.parse::<i64>() {
        return Ok(serde_json::Value::Number(num_value.into()));
    }

    if let Ok(num_value) = value.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(num_value) {
            return Ok(serde_json::Value::Number(number));
        }
    }

    // 默认为字符串
    Ok(serde_json::Value::String(value.to_string()))
}

fn set_nested_value(
    json: &mut serde_json::Value,
    path_parts: &[&str],
    value: serde_json::Value,
) -> Result<()> {
    if path_parts.is_empty() {
        return Err(CoreError::config_error("Empty path not allowed"));
    }

    let mut current = json;

    for (i, part) in path_parts.iter().enumerate() {
        if i == path_parts.len() - 1 {
            // 最后一个部分，设置值
            if let serde_json::Value::Object(obj) = current {
                obj.insert(part.to_string(), value);
                break;
            } else {
                return Err(CoreError::config_error(format!(
                    "Cannot set value at path: {}",
                    path_parts.join(".")
                )));
            }
        } else {
            // 中间部分，确保对象存在
            if let serde_json::Value::Object(obj) = current {
                if !obj.contains_key(*part) {
                    obj.insert(
                        part.to_string(),
                        serde_json::Value::Object(serde_json::Map::new()),
                    );
                }
                current = obj.get_mut(*part).unwrap();
            } else {
                return Err(CoreError::config_error(format!(
                    "Invalid path: {}",
                    path_parts.join(".")
                )));
            }
        }
    }

    Ok(())
}
