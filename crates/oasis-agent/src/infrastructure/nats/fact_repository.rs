use async_trait::async_trait;
use oasis_core::error::{CoreError, Result};

use crate::application::ports::fact_repository::FactRepositoryPort;
use crate::domain::models::SystemFacts;

/// NATS KV 事实仓库实现
pub struct NatsFactRepository {
    client: async_nats::Client,
    bucket: String,
    prefix: String,
}

impl NatsFactRepository {
    pub fn new(client: async_nats::Client, bucket: String, prefix: String) -> Self {
        Self {
            client,
            bucket,
            prefix,
        }
    }

    async fn get_kv_store(&self) -> Result<async_nats::jetstream::kv::Store> {
        let js = async_nats::jetstream::new(self.client.clone());
        js.get_key_value(&self.bucket)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to create KV store: {}", e),
            })
    }
}

#[async_trait]
impl FactRepositoryPort for NatsFactRepository {
    async fn publish_facts(&self, facts: &SystemFacts) -> Result<()> {
        let store = self.get_kv_store().await?;
        let key = format!("{}{}", self.prefix, "facts");
        let value = serde_json::to_vec(facts)
            .map_err(|e| CoreError::Serialization {
                message: format!("Failed to serialize facts: {}", e),
            })?;

        store.put(&key, value.into())
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to publish facts: {}", e),
            })?;

        Ok(())
    }
}
