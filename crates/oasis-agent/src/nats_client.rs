use anyhow::Result;
use async_nats::{Client, jetstream};
use oasis_core::{
    config::{NatsConfig, TlsConfig},
    constants::*,
    nats::NatsClientFactory,
};

#[derive(Clone)]
pub struct NatsClient {
    pub client: Client,
    pub jetstream: jetstream::Context,
}

impl NatsClient {
    pub async fn connect_with_oasis_config(nats: &NatsConfig, tls: &TlsConfig) -> Result<Self> {
        // 使用 core 中的通用连接方式，支持 TLS
        let nats_client_with_jetstream =
            NatsClientFactory::create_nats_client_with_jetstream(nats, tls).await?;

        Ok(Self {
            client: nats_client_with_jetstream.client,
            jetstream: nats_client_with_jetstream.jetstream,
        })
    }

    /// 确保必要的 JetStream 资源存在
    pub async fn ensure_resources(&self) -> Result<()> {
        // 这里只是尝试获取，如果不存在会失败，但是由 Server 负责创建
        // Agent 不负责创建 JetStream 资源
        let _ = self.jetstream.get_stream(JS_STREAM_TASKS).await;
        let _ = self.jetstream.get_stream(JS_STREAM_RESULTS).await;
        let _ = self.jetstream.get_stream(JS_STREAM_FILES).await;
        let _ = self.jetstream.get_key_value(JS_KV_AGENT_INFOS).await;
        let _ = self.jetstream.get_key_value(JS_KV_AGENT_HEARTBEAT).await;
        let _ = self.jetstream.get_key_value(JS_KV_AGENT_LABELS).await;

        Ok(())
    }
}
