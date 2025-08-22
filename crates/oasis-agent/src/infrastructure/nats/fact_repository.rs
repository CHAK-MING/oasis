use anyhow::{Context, Result};
use async_nats::jetstream;
use oasis_core::{agent::AgentFacts, constants, types::AgentId};

use crate::application::ports::fact_repository::FactRepositoryPort;

pub struct NatsFactRepository {
    js: jetstream::Context,
}

impl NatsFactRepository {
    pub fn new(client: &async_nats::Client) -> Self {
        Self {
            js: jetstream::new(client.clone()),
        }
    }
}

#[async_trait::async_trait]
impl FactRepositoryPort for NatsFactRepository {
    async fn publish_facts(&self, agent_id: &AgentId, facts: &AgentFacts) -> Result<()> {
        let kv = self
            .js
            .get_key_value(constants::JS_KV_NODE_FACTS)
            .await
            .context("bind to node facts KV bucket")?;

        let key = constants::kv_key_facts(agent_id.as_str());
        let payload = rmp_serde::to_vec(facts).context("serialize AgentFacts")?;

        kv.put(&key, payload.into())
            .await
            .context("kv put AgentFacts")?;
        Ok(())
    }
}
