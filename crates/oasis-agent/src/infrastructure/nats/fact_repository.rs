use crate::application::ports::fact_repository::FactRepositoryPort;
use async_nats::Client;
use oasis_core::{agent::AgentFacts, error::Result};

pub struct NatsFactRepository {
    client: Client,
    bucket_name: String,
    key: String,
}

impl NatsFactRepository {
    pub fn new(client: Client, bucket_name: String, key: String) -> Self {
        Self {
            client,
            bucket_name,
            key,
        }
    }
}

#[async_trait::async_trait]
impl FactRepositoryPort for NatsFactRepository {
    async fn publish_agent_facts(&self, facts: &AgentFacts) -> Result<()> {
        // 转为 Proto 并使用 Protobuf (prost) 编码
        let proto: oasis_core::proto::AgentFacts = facts.into();
        let data = oasis_core::proto_impls::encoding::to_vec(&proto);

        // 发布到 NATS KV
        let js = async_nats::jetstream::new(self.client.clone());
        let kv = js.get_key_value(&self.bucket_name).await.map_err(|e| {
            oasis_core::error::CoreError::Nats {
                message: format!("Failed to get KV bucket {}: {}", self.bucket_name, e),
            }
        })?;

        kv.put(&self.key, data.into())
            .await
            .map_err(|e| oasis_core::error::CoreError::Nats {
                message: format!("Failed to put facts to KV: {}", e),
            })?;

        Ok(())
    }
}
