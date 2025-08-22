use async_trait::async_trait;
use oasis_core::config::{ConfigListener, ConfigSource};
use oasis_core::error::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::AgentConfig;
use crate::domain::Agent;

/// Agent 配置监听器
pub struct AgentConfigListener {
    agent: Arc<RwLock<Agent>>,
}

impl AgentConfigListener {
    pub fn new(agent: Arc<RwLock<Agent>>) -> Self {
        Self { agent }
    }
}

#[async_trait]
impl ConfigListener for AgentConfigListener {
    type Config = AgentConfig;

    async fn on_config_changed(
        &self,
        old_config: &Self::Config,
        new_config: &Self::Config,
    ) -> Result<()> {
        info!("Agent configuration changed, applying updates...");

        // 检查关键配置变更
        let mut needs_restart = false;

        // 检查 agent_id 变更（需要重启）
        if old_config.agent.agent_id != new_config.agent.agent_id {
            warn!(
                "Agent ID changed from {} to {}, restart required",
                old_config.agent.agent_id, new_config.agent.agent_id
            );
            needs_restart = true;
        }

        // 检查 NATS 连接配置变更（需要重启）
        if old_config.common.nats.url != new_config.common.nats.url
            || old_config.common.nats.tls_required != new_config.common.nats.tls_required
        {
            warn!("NATS connection configuration changed, restart required");
            needs_restart = true;
        }

        // 更新 Agent 的 NodeAttributes
        {
            let mut agent_guard = self.agent.write().await;

            // 更新 attributes 中的可热更新字段（environment/region 统一在 labels）
            agent_guard.attributes.groups = new_config.attributes.groups.clone();
            agent_guard.attributes.labels = new_config.attributes.labels.clone();
            agent_guard.attributes.version = new_config.attributes.version.clone();
            agent_guard.attributes.custom = new_config.attributes.custom.clone();

            info!("Updated agent attributes from configuration");
        }

        // 记录配置变更
        info!("Configuration differences detected");

        if needs_restart {
            warn!("Configuration changes require agent restart to take full effect");
        } else {
            info!("Configuration hot-reloaded successfully");
        }

        Ok(())
    }

    async fn on_config_validation_failed(
        &self,
        _config: &Self::Config,
        errors: Vec<String>,
    ) -> Result<()> {
        error!("Agent configuration validation failed:");
        for error in errors {
            error!("  - {}", error);
        }
        Ok(())
    }

    async fn on_config_load_failed(
        &self,
        source: &ConfigSource,
        error: &oasis_core::error::CoreError,
    ) -> Result<()> {
        error!(
            "Failed to load agent configuration from {}: {}",
            source.description(),
            error
        );
        Ok(())
    }
}
