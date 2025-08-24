use crate::config::AgentConfig;
use async_trait::async_trait;
use oasis_core::error::Result;
use oasis_core::types::AgentId;

#[async_trait]
pub trait ConfigRepository: Send + Sync {
    async fn fetch_config(&self, agent_id: &AgentId) -> Result<AgentConfig>;
}

pub mod fact_collector;
pub mod fact_repository;
