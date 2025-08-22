use crate::domain::agent::Agent;
use crate::infrastructure::{nats::client::NatsClient, nats::publisher::NatsPublisher};
use oasis_core::agent::{AgentHeartbeat, AgentStatus as CoreAgentStatus};
use oasis_core::backoff::{execute_with_backoff, network_publish_backoff};
use oasis_core::rate_limit::{RateLimiterCollection, rate_limited_operation};
use oasis_core::shutdown::GracefulShutdown;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use tracing::{error, info};

pub struct HeartbeatService {
    agent: Arc<RwLock<Agent>>,
    nats_client: NatsClient,
    limiters: Arc<RateLimiterCollection>,
    shutdown: GracefulShutdown,
}

impl HeartbeatService {
    pub fn new(
        agent: Arc<RwLock<Agent>>,
        nats_client: NatsClient,
        limiters: Arc<RateLimiterCollection>,
        shutdown: GracefulShutdown,
    ) -> Self {
        Self {
            agent,
            nats_client,
            limiters,
            shutdown,
        }
    }

    pub async fn run(self) {
        let mut timer = interval(Duration::from_secs(15));
        let publisher = NatsPublisher::new(self.nats_client.clone());
        let mut sequence = 0u64;

        info!("Starting heartbeat service");

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!("Heartbeat service received shutdown");
                    break;
                }
                _ = timer.tick() => {
                    if let Err(e) = self.send_heartbeat(&publisher, sequence).await {
                        error!("Failed to send heartbeat: {}", e);
                    } else {
                        sequence = sequence.wrapping_add(1);
                    }
                }
            }
        }
    }

    async fn send_heartbeat(&self, publisher: &NatsPublisher, sequence: u64) -> anyhow::Result<()> {
        // 收集 agent 信息
        let (agent_id, status) = {
            let agent = self.agent.read().await;
            (agent.id.clone(), agent.status.clone())
        };

        // 构建心跳
        let heartbeat = AgentHeartbeat {
            agent_id: oasis_core::types::AgentId::from(agent_id),
            status: match status {
                crate::domain::agent::AgentStatus::Initializing => CoreAgentStatus::Offline,
                crate::domain::agent::AgentStatus::Running => CoreAgentStatus::Online,
                crate::domain::agent::AgentStatus::Draining => CoreAgentStatus::Busy,
                crate::domain::agent::AgentStatus::Shutdown => CoreAgentStatus::Offline,
            },
            last_seen: chrono::Utc::now().timestamp(),
            sequence,
        };

        // 发布心跳
        // 限流 + 退避 发布心跳
        let limiters = self.limiters.clone();
        rate_limited_operation(
            &limiters.heartbeat,
            || async {
                execute_with_backoff(
                    || async {
                        publisher
                            .publish_heartbeat(&heartbeat)
                            .await
                            .map_err(|e| e.to_string())
                    },
                    network_publish_backoff(),
                )
                                    .await
                    .map_err(|e| crate::error::CoreError::Internal {
                        message: e.to_string(),
                    })
            },
            None,
            "heartbeat.publish",
        )
        .await?;

        info!("Heartbeat sent (sequence: {})", sequence);
        Ok(())
    }
}
