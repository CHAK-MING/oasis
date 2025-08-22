use anyhow::Result;
use async_trait::async_trait;
use oasis_core::types::AgentId;

use crate::config::AgentConfig;

#[async_trait]
pub trait ConfigRepository: Send + Sync {
    async fn fetch_config(&self, agent_id: &AgentId) -> Result<AgentConfig>;
}
