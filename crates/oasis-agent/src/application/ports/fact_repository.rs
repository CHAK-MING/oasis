use async_trait::async_trait;
use oasis_core::{error::Result, types::AgentFacts};

/// 系统信息发布接口
#[async_trait]
pub trait FactRepositoryPort: Send + Sync {
    /// 发布 AgentFacts
    async fn publish_agent_facts(&self, facts: &AgentFacts) -> Result<()>;
}
