use async_trait::async_trait;
use oasis_core::constants::KV_CONFIG_AGENT_KEY_PREFIX;
use oasis_core::constants::{JS_KV_CONFIG, kv_config_key};
use oasis_core::error::CoreError;

use crate::application::ports::repositories::AgentConfigRepository as AgentConfigRepositoryPort;

pub struct NatsAgentConfigRepository {
    jetstream: async_nats::jetstream::Context,
}

impl NatsAgentConfigRepository {
    pub fn new(jetstream: async_nats::jetstream::Context) -> Self {
        Self { jetstream }
    }

    async fn ensure_kv(&self) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        match self.jetstream.get_key_value(JS_KV_CONFIG).await {
            Ok(kv) => Ok(kv),
            Err(_) => self
                .jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: JS_KV_CONFIG.to_string(),
                    history: 10,
                    ..Default::default()
                })
                .await
                .map_err(|e| CoreError::Nats {
                    message: e.to_string(),
                }),
        }
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
                    .map_err(|e| CoreError::Nats {
                        message: e.to_string(),
                    })?;
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
            Err(e) => Err(CoreError::Nats {
                message: e.to_string(),
            }),
        }
    }

    async fn set(&self, agent_id: &str, key: &str, value: &str) -> Result<(), CoreError> {
        let kv = self.ensure_kv().await?;
        let key = kv_config_key(agent_id, key);
        kv.put(&key, value.as_bytes().to_vec().into())
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?;
        Ok(())
    }

    async fn del(&self, agent_id: &str, key: &str) -> Result<(), CoreError> {
        let kv = self.ensure_kv().await?;
        let key = kv_config_key(agent_id, key);
        kv.delete(&key).await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;
        Ok(())
    }

    async fn list_keys(
        &self,
        agent_id: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, CoreError> {
        let kv = self.ensure_kv().await?;
        let agent_prefix = format!("{}.{agent_id}.", KV_CONFIG_AGENT_KEY_PREFIX);
        let keys = kv.keys().await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;
        use futures::TryStreamExt;
        let all: Vec<String> = keys.try_collect().await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;

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
        let keys = kv.keys().await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;
        use futures::TryStreamExt;
        let all: Vec<String> = keys.try_collect().await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;

        let mut config = std::collections::HashMap::new();
        for k in all.into_iter().filter(|k| k.starts_with(&agent_prefix)) {
            if let Some(key_part) = k.strip_prefix(&agent_prefix) {
                if let Ok(Some(bytes)) = kv.get(&k).await {
                    if let Ok(value) = String::from_utf8(bytes.to_vec()) {
                        config.insert(key_part.to_string(), value);
                    }
                }
            }
        }

        Ok(config)
    }

    async fn clear_for_agent(&self, agent_id: &str) -> Result<u64, CoreError> {
        let kv = self.ensure_kv().await?;
        // 列出所有键并筛选本 agent 前缀
        let prefix = format!("{}.{agent_id}.", KV_CONFIG_AGENT_KEY_PREFIX);
        let mut deleted: u64 = 0;
        let keys = kv.keys().await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;
        use futures::TryStreamExt;
        let all: Vec<String> = keys.try_collect().await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;
        for k in all.into_iter().filter(|k| k.starts_with(&prefix)) {
            kv.purge(&k).await.map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?;
            deleted += 1;
        }
        Ok(deleted)
    }
}
