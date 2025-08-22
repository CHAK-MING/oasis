use async_trait::async_trait;
use oasis_core::error::{CoreError, Result};
use oasis_core::types::AgentId;

use crate::application::ports::ConfigRepository;
use crate::config::AgentConfig;

/// NATS KV 配置仓库实现
pub struct NatsConfigRepository {
    client: async_nats::Client,
    bucket: String,
    prefix: String,
}

impl NatsConfigRepository {
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
impl ConfigRepository for NatsConfigRepository {
    async fn fetch_config(&self, agent_id: &AgentId) -> Result<AgentConfig> {
        let store = self.get_kv_store().await?;
        let key = format!("{}{}", self.prefix, agent_id.as_str());

        match store.get(&key).await {
            Ok(Some(entry)) => {
                let config: AgentConfig = serde_json::from_slice(&entry)
                    .map_err(|e| CoreError::Config {
                        message: format!("Failed to deserialize config: {}", e),
                    })?;
                Ok(config)
            }
            Ok(None) => Err(CoreError::Config {
                message: format!("Config not found for agent: {}", agent_id),
            }),
            Err(e) => Err(CoreError::Nats {
                message: format!("Failed to fetch config: {}", e),
            }),
        }
    }
}
