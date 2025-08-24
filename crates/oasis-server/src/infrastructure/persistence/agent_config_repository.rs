use async_trait::async_trait;
use oasis_core::constants::KV_CONFIG_AGENT_KEY_PREFIX;
use oasis_core::constants::{JS_KV_CONFIG, kv_config_key};
use oasis_core::error::CoreError;

use crate::application::ports::repositories::AgentConfigRepository as AgentConfigRepositoryPort;
use crate::infrastructure::persistence::utils as persist;

pub struct NatsAgentConfigRepository {
    jetstream: async_nats::jetstream::Context,
}

impl NatsAgentConfigRepository {
    pub fn new(jetstream: async_nats::jetstream::Context) -> Self {
        Self { jetstream }
    }

    async fn ensure_kv(&self) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        persist::ensure_kv(&self.jetstream, JS_KV_CONFIG, "OASIS-CONFIG store").await
    }
}

#[async_trait]
impl AgentConfigRepositoryPort for NatsAgentConfigRepository {
    async fn apply_bulk(
        &self,
        agent_ids: &[String],
        flat_kv: &std::collections::HashMap<String, String>,
    ) -> Result<u64, CoreError> {
        let kv = self.ensure_kv().await?;
        for agent in agent_ids {
            for (k, v) in flat_kv {
                let key = kv_config_key(agent, k);
                kv.put(&key, v.clone().into_bytes().into())
                    .await
                    .map_err(persist::map_nats_err)?;
            }
        }
        Ok(agent_ids.len() as u64)
    }

    async fn get(&self, agent_id: &str, key: &str) -> Result<Option<String>, CoreError> {
        let kv = self.ensure_kv().await?;
        let key = kv_config_key(agent_id, key);
        match kv.get(&key).await {
            Ok(Some(bytes)) => Ok(String::from_utf8(bytes.to_vec()).ok()),
            Ok(None) => Ok(None),
            Err(e) => Err(persist::map_nats_err(e)),
        }
    }

    async fn set(&self, agent_id: &str, key: &str, value: &str) -> Result<(), CoreError> {
        let kv = self.ensure_kv().await?;
        let key = kv_config_key(agent_id, key);
        kv.put(&key, value.as_bytes().to_vec().into())
            .await
            .map_err(persist::map_nats_err)?;
        Ok(())
    }

    async fn del(&self, agent_id: &str, key: &str) -> Result<(), CoreError> {
        let kv = self.ensure_kv().await?;
        let key = kv_config_key(agent_id, key);
        kv.delete(&key).await.map_err(persist::map_nats_err)?;
        Ok(())
    }

    async fn list_keys(
        &self,
        agent_id: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, CoreError> {
        let kv = self.ensure_kv().await?;
        let agent_prefix = format!("{}.{agent_id}.", KV_CONFIG_AGENT_KEY_PREFIX);
        let keys = kv.keys().await.map_err(persist::map_nats_err)?;
        use futures::TryStreamExt;
        let all: Vec<String> = keys.try_collect().await.map_err(persist::map_nats_err)?;

        let filtered_keys: Vec<String> = all
            .into_iter()
            .filter(|k| k.starts_with(&agent_prefix))
            .filter_map(|k| {
                // 移除前缀，只返回键名部分
                let key_part = k.strip_prefix(&agent_prefix)?;
                if let Some(filter_prefix) = prefix {
                    if key_part.starts_with(filter_prefix) {
                        Some(key_part.to_string())
                    } else {
                        None
                    }
                } else {
                    Some(key_part.to_string())
                }
            })
            .collect();

        Ok(filtered_keys)
    }

    async fn get_all(
        &self,
        agent_id: &str,
    ) -> Result<std::collections::HashMap<String, String>, CoreError> {
        let kv = self.ensure_kv().await?;
        let agent_prefix = format!("{}.{agent_id}.", KV_CONFIG_AGENT_KEY_PREFIX);
        let keys = kv.keys().await.map_err(persist::map_nats_err)?;
        use futures::TryStreamExt;
        let all: Vec<String> = keys.try_collect().await.map_err(persist::map_nats_err)?;

        // 并发拉取每个 key，避免 N+1 串行
        use futures::{StreamExt as _, stream};
        let fetches = all
            .into_iter()
            .filter(|k| k.starts_with(&agent_prefix))
            .map(|k| {
                let kv = kv.clone();
                async move {
                    let value_opt = match kv.get(&k).await {
                        Ok(Some(bytes)) => String::from_utf8(bytes.to_vec()).ok(),
                        _ => None,
                    };
                    (k, value_opt)
                }
            });

        let results: Vec<(String, Option<String>)> =
            stream::iter(fetches).buffer_unordered(16).collect().await;

        let mut config = std::collections::HashMap::new();
        for (k, value_opt) in results {
            if let Some(key_part) = k.strip_prefix(&agent_prefix) {
                if let Some(value) = value_opt {
                    config.insert(key_part.to_string(), value);
                }
            }
        }

        Ok(config)
    }

    // 批量清空能力暂停暴露；如需恢复请通过用例/RPC明确驱动
}
