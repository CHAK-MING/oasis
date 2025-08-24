use figment::{
    Figment,
    providers::{Env, Serialized},
};
use oasis_core::config::CommonConfig;
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
    #[serde(rename = "agent.id")]
    pub agent_id: AgentId,
    #[serde(rename = "max.concurrent.tasks")]
    pub max_concurrent_tasks: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorSection {
    #[serde(rename = "command.timeout.sec")]
    pub command_timeout_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySection {
    #[serde(rename = "enforce.whitelist")]
    pub enforce_whitelist: bool,
    #[serde(rename = "whitelist.paths")]
    pub whitelist_paths: Vec<String>,
    #[serde(rename = "file.allowed.roots")]
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

            // 直接设置到对应的配置部分
            if let serde_json::Value::Object(obj) = &mut current_json {
                if let Some(section_obj) = obj.get_mut(key.split('.').next().unwrap_or("")) {
                    if let serde_json::Value::Object(section_map) = section_obj {
                        // 移除前缀，设置字段
                        let field_name = key.split('.').skip(1).collect::<Vec<_>>().join(".");
                        section_map.insert(field_name, json_value);
                    }
                }
            }
        }

        *self = serde_json::from_value(current_json)?;

        Ok(())
    }

    /// 智能配置加载
    pub async fn load_smart() -> Result<Self> {
        let figment = Figment::new()
            .merge(Serialized::defaults(AgentConfig::with_smart_defaults()))
            .merge(Env::prefixed("OASIS_AGENT_").split("__"));

        let cfg: AgentConfig = figment.extract().map_err(|e| CoreError::Config {
            message: format!("Failed to extract base config: {}", e),
        })?;

        let _nats_client = oasis_core::transport::NatsClientFactory::for_agent(&cfg.common.nats)
            .await?
            .client;

        let agent_id = cfg.agent.agent_id.clone();
        let _kv_prefix = oasis_core::constants::kv_config_scan_prefix(agent_id.as_str());
        let _kv_bucket = oasis_core::constants::JS_KV_CONFIG.to_string();

        // Here we would need a NATS KV provider for figment, which is not available out-of-the-box.
        // The previous logic for manual KV fetching and merging will be restored and adapted.
        // This part requires careful implementation to merge KV overrides.

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
