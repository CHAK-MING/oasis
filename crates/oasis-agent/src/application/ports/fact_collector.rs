use async_trait::async_trait;
use oasis_core::error::Result;

use crate::domain::models::SystemFacts;

/// 系统信息收集器接口
#[async_trait]
pub trait FactCollectorPort: Send + Sync {
    /// 收集系统信息
    async fn collect(&self) -> Result<SystemFacts>;
}
