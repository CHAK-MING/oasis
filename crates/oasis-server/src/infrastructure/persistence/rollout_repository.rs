use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use oasis_core::error::CoreError;

use crate::application::ports::repositories::RolloutRepository;
use crate::domain::models::rollout::Rollout;
use crate::infrastructure::persistence::utils as persist;
use crate::interface::grpc::converters::{from_proto_rollout, to_proto_rollout};
use oasis_core::proto;
use oasis_core::proto_impls::encoding;
use prost::Message;

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
        // 统一入口 ensure_kv_buckets() 已在系统启动阶段被调用；此处仅获取
        self.jetstream
            .get_key_value(oasis_core::JS_KV_ROLLOUTS)
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })
    }
}

#[async_trait]
impl RolloutRepository for NatsRolloutRepository {
    async fn create(&self, rollout: Rollout) -> Result<String, CoreError> {
        let store = self.ensure_rollout_store().await?;
        let key = format!("rollout.{}", rollout.id);

        let msg = to_proto_rollout(&rollout);
        let data = encoding::to_vec(&msg);

        store
            .put(&key, data.into())
            .await
            .map_err(persist::map_nats_err)?;

        Ok(rollout.id)
    }

    async fn get(&self, id: &str) -> Result<Rollout, CoreError> {
        let store = self.ensure_rollout_store().await?;
        let key = format!("rollout.{}", id);

        let entry = store.get(&key).await.map_err(persist::map_nats_err)?;

        let entry_data = entry.ok_or_else(|| CoreError::Agent {
            agent_id: id.to_string().into(),
            message: "Rollout not found".to_string(),
        })?;

        let msg =
            proto::RolloutMsg::decode(entry_data.as_ref()).map_err(|e| CoreError::Internal {
                message: format!("decode RolloutMsg error: {}", e),
            })?;
        let rollout: Rollout = from_proto_rollout(&msg)?;

        Ok(rollout)
    }

    async fn update(&self, rollout: Rollout) -> Result<(), CoreError> {
        let store = self.ensure_rollout_store().await?;
        let key = format!("rollout.{}", rollout.id);

        // 读取当前版本与修订号，进行 CAS 校验
        let current_entry = store.entry(&key).await.map_err(persist::map_nats_err)?;

        let (current_revision, current_version) = if let Some(entry) = current_entry {
            let existing_msg = proto::RolloutMsg::decode(entry.value.as_ref()).map_err(|e| {
                CoreError::Internal {
                    message: format!("decode RolloutMsg error: {}", e),
                }
            })?;
            let existing: Rollout = from_proto_rollout(&existing_msg)?;
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

        let msg = to_proto_rollout(&rollout);
        let data = encoding::to_vec(&msg);

        // 使用 KV 的 update（带期望的 last revision）实现 CAS
        store
            .update(&key, data.into(), current_revision)
            .await
            .map_err(persist::map_nats_err)?;

        Ok(())
    }

    async fn list(&self) -> Result<Vec<Rollout>, CoreError> {
        let store = self.ensure_rollout_store().await?;
        let keys = store
            .keys()
            .await
            .map_err(persist::map_nats_err)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(persist::map_nats_err)?;

        let mut tasks = futures::stream::FuturesUnordered::new();
        for key in keys {
            if let Some(rollout_id) = key.strip_prefix("rollout.") {
                let store = self.jetstream.clone();
                let id = rollout_id.to_string();
                tasks.push(async move {
                    match store.get_key_value(oasis_core::JS_KV_ROLLOUTS).await {
                        Ok(kv) => match kv.get(&format!("rollout.{}", id)).await {
                            Ok(Some(entry)) => proto::RolloutMsg::decode(entry.as_ref())
                                .ok()
                                .and_then(|m| {
                                    crate::interface::grpc::converters::from_proto_rollout(&m).ok()
                                }),
                            _ => None,
                        },
                        Err(_) => None,
                    }
                });
            }
        }

        let mut rollouts = Vec::new();
        while let Some(opt) = tasks.next().await {
            if let Some(r) = opt {
                rollouts.push(r);
            }
        }
        Ok(rollouts)
    }

    // 删除能力下沉为内部方法，如需对外暴露再通过用例驱动

    async fn list_active(&self) -> Result<Vec<Rollout>, CoreError> {
        let all_rollouts = self.list().await?;
        let active_rollouts: Vec<Rollout> = all_rollouts
            .into_iter()
            .filter(|rollout| rollout.is_active())
            .collect();

        Ok(active_rollouts)
    }
}
