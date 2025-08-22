use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::ServerConfig;

/// 基础设施资源管理器
pub struct InfrastructureResourceManager;

impl InfrastructureResourceManager {
    /// 初始化所有基础设施资源
    pub async fn initialize(
        jetstream: &async_nats::jetstream::Context,
        config: &ServerConfig,
        shutdown_token: CancellationToken,
    ) -> Result<Vec<tokio::task::JoinHandle<()>>> {
        info!("Initializing infrastructure resources");

        // 确保JetStream资源
        crate::infrastructure::messaging::streams::ensure_streams(jetstream, config).await?;

        // 创建KV watchers
        let kv_handles = crate::infrastructure::messaging::kv::ensure_kv_and_watch(
            jetstream,
            config,
            shutdown_token,
        )
        .await?;

        info!("Infrastructure resources initialized successfully");
        Ok(kv_handles)
    }
}
