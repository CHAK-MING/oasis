use anyhow::Result;

use crate::domain::models::SystemFacts;

#[async_trait::async_trait]
pub trait FactCollectorPort: Send + Sync {
    async fn collect(&self) -> Result<SystemFacts>;
}
