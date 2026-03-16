use async_nats::{Client, jetstream};
use oasis_core::{
    config::{NatsConfig, TlsConfig},
    constants::*,
    error::Result,
    nats::NatsClientFactory,
};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

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
        let _ = self.jetstream.get_stream(JS_STREAM_GROUP_TASKS).await;
        let _ = self.jetstream.get_stream(JS_STREAM_RESULTS).await;
        let _ = self.jetstream.get_stream(JS_STREAM_FILES).await;
        let _ = self.jetstream.get_key_value(JS_KV_AGENT_INFOS).await;
        let _ = self.jetstream.get_key_value(JS_KV_AGENT_HEARTBEAT).await;
        let _ = self.jetstream.get_key_value(JS_KV_AGENT_LABELS).await;
        let _ = self.jetstream.get_key_value(JS_KV_FILE_APPLY_RESULTS).await;

        Ok(())
    }
}

pub struct ManagedClientState<T: Clone> {
    current: RwLock<T>,
    generation_tx: watch::Sender<u64>,
}

impl<T: Clone> ManagedClientState<T> {
    fn new(initial: T, generation: u64) -> Self {
        let (generation_tx, _) = watch::channel(generation);
        Self {
            current: RwLock::new(initial),
            generation_tx,
        }
    }

    async fn current(&self) -> T {
        self.current.read().await.clone()
    }

    async fn replace(&self, next: T) {
        *self.current.write().await = next;
        let next_generation = self.generation_tx.borrow().saturating_add(1);
        let _ = self.generation_tx.send(next_generation);
    }

    pub fn subscribe_generation(&self) -> watch::Receiver<u64> {
        self.generation_tx.subscribe()
    }
}

#[derive(Clone)]
pub struct ManagedNatsClient {
    nats: NatsConfig,
    tls: TlsConfig,
    state: Arc<ManagedClientState<Arc<NatsClient>>>,
}

impl ManagedNatsClient {
    pub async fn connect_with_oasis_config(nats: &NatsConfig, tls: &TlsConfig) -> Result<Self> {
        let client = Arc::new(NatsClient::connect_with_oasis_config(nats, tls).await?);
        Ok(Self {
            nats: nats.clone(),
            tls: tls.clone(),
            state: Arc::new(ManagedClientState::new(client, 0)),
        })
    }

    pub async fn current(&self) -> Arc<NatsClient> {
        self.state.current().await
    }

    pub fn subscribe_generation(&self) -> watch::Receiver<u64> {
        self.state.subscribe_generation()
    }

    pub async fn reconnect(&self) -> Result<()> {
        let next = Arc::new(NatsClient::connect_with_oasis_config(&self.nats, &self.tls).await?);
        self.state.replace(next).await;
        Ok(())
    }

    pub async fn ensure_resources(&self) -> Result<()> {
        self.current().await.ensure_resources().await
    }
}

#[cfg(test)]
mod managed_tests {
    use super::*;

    #[tokio::test]
    async fn managed_state_notifies_generation_updates() {
        let state = ManagedClientState::new(Arc::new(1_u64), 0);
        let mut rx = state.subscribe_generation();

        state.replace(Arc::new(2_u64)).await;

        rx.changed().await.expect("generation update");
        assert_eq!(*rx.borrow(), 1);
        assert_eq!(*state.current().await, 2);
    }
}
