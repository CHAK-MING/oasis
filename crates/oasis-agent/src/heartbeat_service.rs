use crate::nats_client::NatsClient;
use anyhow::Result;
use oasis_core::{constants::{kv_key_heartbeat, JS_KV_AGENT_HEARTBEAT}, core_types::AgentId};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct HeartbeatService {
    agent_id: AgentId,
    nats_client: NatsClient,
    shutdown_token: CancellationToken,
}

impl HeartbeatService {
    pub fn new(
        agent_id: AgentId,
        nats_client: NatsClient,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            agent_id,
            nats_client,
            shutdown_token,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting heartbeat service");
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.send_heartbeat().await {
                        error!("Failed to send heartbeat: {}", e);
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    info!("Heartbeat service shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn send_heartbeat(&self) -> Result<()> {
        let kv = self.nats_client.jetstream.get_key_value(JS_KV_AGENT_HEARTBEAT).await?;
        let key = kv_key_heartbeat(self.agent_id.as_str());
        let timestamp = chrono::Utc::now().timestamp();
        
        kv.put(&key, timestamp.to_string().into()).await?;
        debug!("Sent heartbeat for agent: {}", self.agent_id);

        Ok(())
    }
}