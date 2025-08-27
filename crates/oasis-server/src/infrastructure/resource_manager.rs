use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// 基础设施资源管理器
pub struct InfrastructureResourceManager;

impl InfrastructureResourceManager {
    /// 初始化所有基础设施资源
    pub async fn initialize(
        jetstream: &async_nats::jetstream::Context,
        _shutdown_token: CancellationToken,
    ) -> Result<Vec<tokio::task::JoinHandle<()>>> {
        info!("Initializing infrastructure resources");

        // 确保JetStream资源
        crate::infrastructure::messaging::streams::ensure_streams(jetstream).await?;

        // 统一由 bootstrap::start_kv_watchers 启动 watchers；此处不再重复
        info!("Infrastructure resources initialized successfully");
        Ok(Vec::new())
    }
}
