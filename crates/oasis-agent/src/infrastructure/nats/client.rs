use anyhow::Result;
use oasis_core::transport::NatsClientFactory;

/// NATS 客户端 - 使用统一的连接逻辑
///
/// 这个结构体现在是一个简单的包装器，使用 oasis-core 中的统一 NATS 客户端
#[derive(Clone)]
pub struct NatsClient {
    pub client: async_nats::Client,
    pub jetstream: async_nats::jetstream::Context,
}

impl NatsClient {
    /// 连接到 NATS 服务器
    ///
    /// 使用统一的连接逻辑，确保与 server 端的一致性
    pub async fn connect(config: &oasis_core::config::NatsConfig) -> Result<Self> {
        let unified_client = NatsClientFactory::for_agent(config).await?;

        Ok(Self {
            client: unified_client.client,
            jetstream: unified_client.jetstream,
        })
    }
}
