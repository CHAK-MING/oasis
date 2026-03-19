use async_nats::jetstream::Context;
use oasis_core::{
    error::{CoreError, ErrorSeverity, Result},
    event_types::OasisEvent,
    rate_limit::{RateLimiterCollection, rate_limited_operation},
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EventBus {
    jetstream: Arc<Context>,
    rate_limits: Arc<RateLimiterCollection>,
}

impl EventBus {
    pub fn new(jetstream: Arc<Context>, rate_limits: Arc<RateLimiterCollection>) -> Self {
        Self {
            jetstream,
            rate_limits,
        }
    }

    pub async fn publish(&self, event: &OasisEvent) -> Result<()> {
        rate_limited_operation(
            &self.rate_limits.nats,
            || async {
                let payload = serde_json::to_vec(event).map_err(|e| CoreError::Serialization {
                    message: format!("Failed to serialize event payload: {}", e),
                    severity: ErrorSeverity::Error,
                })?;
                let mut headers = async_nats::HeaderMap::new();
                headers.insert("Nats-Msg-Id", event.event_id.clone());

                let ack = self
                    .jetstream
                    .publish_with_headers(event.subject(), headers, payload.into())
                    .await?;
                ack.await?;
                Ok(())
            },
            None,
            "event_bus_publish",
        )
        .await
    }
}
