use async_trait::async_trait;
use oasis_core::error::Result;
use oasis_core::types::AgentId;

use crate::config::AgentConfig;

/// 配置仓库接口 - 抽象化配置的获取方式
#[async_trait]
pub trait ConfigRepository: Send + Sync {
    /// 获取指定 Agent 的配置
    async fn fetch_config(&self, agent_id: &AgentId) -> Result<AgentConfig>;
}
