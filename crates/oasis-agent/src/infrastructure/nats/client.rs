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
    /// 连接到 NATS 服务器（使用配置对象）
    pub async fn connect_with_oasis_config(cfg: &oasis_core::config::OasisConfig) -> Result<Self> {
        let client = NatsClientFactory::connect_with_config(&cfg.nats).await?;
        let jetstream = async_nats::jetstream::new(client.clone());
        Ok(Self { client, jetstream })
    }

    /// 连接到 NATS 服务器（使用显式证书路径参数，兼容现有调用）
    pub async fn connect_with_config(
        nats_url: &str,
        nats_ca: &str,
        nats_client_cert: &str,
        nats_client_key: &str,
    ) -> Result<Self> {
        let client = NatsClientFactory::connect_with_certs(
            nats_url,
            nats_ca,
            nats_client_cert,
            nats_client_key,
        )
        .await?;
        let jetstream = async_nats::jetstream::new(client.clone());
        Ok(Self { client, jetstream })
    }
}
