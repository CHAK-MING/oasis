use async_nats::jetstream::Context;
use oasis_core::{
    error::{CoreError, ErrorSeverity, Result},
    event_types::OasisEvent,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EventBus {
    jetstream: Arc<Context>,
}

impl EventBus {
    pub fn new(jetstream: Arc<Context>) -> Self {
        Self { jetstream }
    }

    pub async fn publish(&self, event: &OasisEvent) -> Result<()> {
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
    }
}
