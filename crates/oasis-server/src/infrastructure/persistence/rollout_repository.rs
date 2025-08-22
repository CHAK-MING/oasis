use async_trait::async_trait;
use futures::TryStreamExt;
use oasis_core::error::CoreError;

use crate::application::ports::repositories::RolloutRepository;
use crate::domain::models::rollout::Rollout;

/// 灰度发布仓储实现 - 基于NATS KV存储
pub struct NatsRolloutRepository {
    jetstream: async_nats::jetstream::Context,
}

impl NatsRolloutRepository {
    pub fn new(jetstream: async_nats::jetstream::Context) -> Self {
        Self { jetstream }
    }

    /// 确保rollout KV存储存在
    async fn ensure_rollout_store(&self) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        match self.jetstream.get_key_value("rollouts").await {
            Ok(store) => Ok(store),
            Err(_) => {
                let cfg = async_nats::jetstream::kv::Config {
                    bucket: "rollouts".to_string(),
                    description: "Rollout state storage".to_string(),
                    max_value_size: 10 * 1024 * 1024, // 10MB
                    history: 10,
                    max_age: std::time::Duration::from_secs(30 * 24 * 60 * 60), // 30天
                    max_bytes: 1024 * 1024 * 1024,                              // 1GB
                    storage: async_nats::jetstream::stream::StorageType::File,
                    num_replicas: 1,
                    ..Default::default()
                };
                self.jetstream
                    .create_key_value(cfg)
                    .await
                    .map_err(|e| CoreError::Nats {
                        message: e.to_string(),
                    })
            }
        }
    }
}

#[async_trait]
impl RolloutRepository for NatsRolloutRepository {
    async fn create(&self, rollout: Rollout) -> Result<String, CoreError> {
        let store = self.ensure_rollout_store().await?;
        let key = format!("rollout.{}", rollout.id);

        let data = serde_json::to_vec(&rollout).map_err(|e| CoreError::Serialization {
            message: e.to_string(),
        })?;

        store
            .put(&key, data.into())
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?;

        Ok(rollout.id)
    }

    async fn get(&self, id: &str) -> Result<Rollout, CoreError> {
        let store = self.ensure_rollout_store().await?;
        let key = format!("rollout.{}", id);

        let entry = store.get(&key).await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;

        let entry_data = entry.ok_or_else(|| CoreError::Agent {
            agent_id: id.to_string(),
            message: "Rollout not found".to_string(),
        })?;

        let rollout: Rollout =
            serde_json::from_slice(&entry_data).map_err(|e| CoreError::Serialization {
                message: e.to_string(),
            })?;

        Ok(rollout)
    }

    async fn update(&self, rollout: Rollout) -> Result<(), CoreError> {
        let store = self.ensure_rollout_store().await?;
        let key = format!("rollout.{}", rollout.id);

        // 读取当前版本与修订号，进行 CAS 校验
        let current_entry = store.entry(&key).await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;

        let (current_revision, current_version) = if let Some(entry) = current_entry {
            let existing: Rollout =
                serde_json::from_slice(&entry.value).map_err(|e| CoreError::Serialization {
                    message: e.to_string(),
                })?;
            (entry.revision, existing.version)
        } else {
            return Err(CoreError::InvalidTask {
                reason: "Rollout not found".to_string(),
                
            });
        };

        // 版本应单调递增：existing.version + 1 == rollout.version
        if rollout.version != current_version + 1 {
            return Err(CoreError::InvalidTask {
                reason: format!(
                    "Rollout version conflict: current={}, new={}",
                    current_version, rollout.version
                ),
                
            });
        }

        let data = serde_json::to_vec(&rollout).map_err(|e| CoreError::Serialization {
            message: e.to_string(),
        })?;

        // 使用 KV 的 update（带期望的 last revision）实现 CAS
        store
            .update(&key, data.into(), current_revision)
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?;

        Ok(())
    }

    async fn list(&self) -> Result<Vec<Rollout>, CoreError> {
        let store = self.ensure_rollout_store().await?;
        let keys = store
            .keys()
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?;

        let mut rollouts = Vec::new();
        for key in keys {
            if let Some(rollout_id) = key.strip_prefix("rollout.") {
                if let Ok(rollout) = self.get(rollout_id).await {
                    rollouts.push(rollout);
                }
            }
        }

        Ok(rollouts)
    }

    async fn delete(&self, id: &str) -> Result<(), CoreError> {
        let store = self.ensure_rollout_store().await?;
        let key = format!("rollout:{}", id);

        store.delete(&key).await.map_err(|e| CoreError::Nats {
            message: e.to_string(),
        })?;

        Ok(())
    }

    async fn list_active(&self) -> Result<Vec<Rollout>, CoreError> {
        let all_rollouts = self.list().await?;
        let active_rollouts: Vec<Rollout> = all_rollouts
            .into_iter()
            .filter(|rollout| rollout.is_active())
            .collect();

        Ok(active_rollouts)
    }
}
