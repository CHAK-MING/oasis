use async_trait::async_trait;
use oasis_core::error::Result;

use crate::domain::models::SystemFacts;

/// 系统信息发布接口
#[async_trait]
pub trait FactRepositoryPort: Send + Sync {
    /// 发布系统信息
    async fn publish_facts(&self, facts: &SystemFacts) -> Result<()>;
}
