use anyhow::Result;
use oasis_core::{agent::AgentFacts, types::AgentId};

#[async_trait::async_trait]
pub trait FactRepositoryPort: Send + Sync {
    async fn publish_facts(&self, agent_id: &AgentId, facts: &AgentFacts) -> Result<()>;
}
