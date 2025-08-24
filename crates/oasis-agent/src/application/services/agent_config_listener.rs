use async_trait::async_trait;
use oasis_core::error::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::AgentConfig;
use crate::domain::Agent;

/// 本地定义的配置监听接口
#[async_trait]
pub trait AgentLocalConfigListener: Send + Sync {
    async fn on_config_changed(
        &self,
        old_config: &AgentConfig,
        new_config: &AgentConfig,
    ) -> Result<()>;
    async fn on_config_validation_failed(
        &self,
        config: &AgentConfig,
        errors: Vec<String>,
    ) -> Result<()>;
    async fn on_config_load_failed(
        &self,
        source: &str,
        error: &oasis_core::error::CoreError,
    ) -> Result<()>;
}

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
impl AgentLocalConfigListener for AgentConfigListener {
    async fn on_config_changed(
        &self,
        old_config: &AgentConfig,
        new_config: &AgentConfig,
    ) -> Result<()> {
        info!("Agent configuration changed, applying updates...");

        let mut needs_restart = false;

        if old_config.agent.agent_id != new_config.agent.agent_id {
            warn!(
                "Agent ID changed from {} to {}, restart required",
                old_config.agent.agent_id, new_config.agent.agent_id
            );
            needs_restart = true;
        }

        if old_config.common.nats.url != new_config.common.nats.url {
            warn!("NATS connection configuration changed, restart required");
            needs_restart = true;
        }

        {
            let mut agent_guard = self.agent.write().await;
            agent_guard.attributes.groups = new_config.attributes.groups.clone();
            agent_guard.attributes.labels = new_config.attributes.labels.clone();
            agent_guard.attributes.version = new_config.attributes.version.clone();
            agent_guard.attributes.custom = new_config.attributes.custom.clone();
            info!("Updated agent attributes from configuration");
        }

        if needs_restart {
            warn!("Configuration changes require agent restart to take full effect");
        } else {
            info!("Configuration hot-reloaded successfully");
        }

        Ok(())
    }

    async fn on_config_validation_failed(
        &self,
        _config: &AgentConfig,
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
        source: &str,
        error: &oasis_core::error::CoreError,
    ) -> Result<()> {
        error!(
            "Failed to load agent configuration from {}: {}",
            source, error
        );
        Ok(())
    }
}
