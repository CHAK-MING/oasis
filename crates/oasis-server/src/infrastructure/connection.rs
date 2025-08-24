use anyhow::Result;
use oasis_core::transport::NatsClientFactory;

/// NATS 连接管理器 - 使用统一的连接逻辑
///
/// 这个结构体现在是一个简单的包装器，使用 oasis-core 中的统一 NATS 客户端
pub struct NatsConnectionManager;

impl NatsConnectionManager {
    /// 创建带 TLS + JWT 的 JetStream 上下文
    pub async fn create_jetstream_with_tls(
        nats_config: &oasis_core::config::NatsConfig,
    ) -> Result<async_nats::jetstream::Context> {
        let unified_client = NatsClientFactory::for_server(nats_config).await?;
        Ok(unified_client.jetstream)
    }
}
