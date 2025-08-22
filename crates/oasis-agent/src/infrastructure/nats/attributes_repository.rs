use anyhow::{Context, Result};
use async_nats::jetstream;
use oasis_core::{agent::AgentLabels, constants, selector::NodeAttributes, types::AgentId};

pub struct NatsAttributesRepository {
    js: jetstream::Context,
}

impl NatsAttributesRepository {
    pub fn new(client: &async_nats::Client) -> Self {
        Self {
            js: jetstream::new(client.clone()),
        }
    }

    pub async fn publish_attributes(
        &self,
        agent_id: &AgentId,
        attributes: &NodeAttributes,
    ) -> Result<()> {
        let kv = self
            .js
            .get_key_value(constants::JS_KV_NODE_LABELS)
            .await
            .context("bind to node labels KV bucket")?;

        // 将 NodeAttributes 中的 labels 以 AgentLabels 结构（MessagePack）写入，
        // 与服务端读取/监听的格式保持一致
        let key = constants::kv_key_labels(agent_id.as_str());
        let agent_labels = AgentLabels {
            agent_id: agent_id.clone(),
            labels: attributes.labels.clone(),
            updated_at: chrono::Utc::now().timestamp(),
            updated_by: "oasis-agent".to_string(),
        };
        let payload = rmp_serde::to_vec(&agent_labels).context("serialize AgentLabels")?;
        kv.put(&key, payload.into())
            .await
            .context("kv put AgentLabels")?;
        Ok(())
    }
}
