pub mod agent_config_repository;
pub mod file_repository;
pub mod node_repository;
pub mod rollout_repository;
pub mod task_repository;

/// 通用持久化工具：NATS KV/ObjectStore 初始化、错误映射、JSON编解码
pub mod utils {
    use oasis_core::error::CoreError;

    pub fn map_nats_err<E: std::fmt::Display>(e: E) -> CoreError {
        CoreError::Nats {
            message: e.to_string(),
        }
    }

    pub fn to_json_vec<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, CoreError> {
        serde_json::to_vec(value).map_err(|e| CoreError::Serialization {
            message: e.to_string(),
        })
    }

    pub fn from_json_slice<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, CoreError> {
        serde_json::from_slice(bytes).map_err(|e| CoreError::Serialization {
            message: e.to_string(),
        })
    }

    pub async fn ensure_kv(
        js: &async_nats::jetstream::Context,
        bucket: &str,
        description: &str,
    ) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        match js.get_key_value(bucket).await {
            Ok(store) => Ok(store),
            Err(_) => {
                let cfg = async_nats::jetstream::kv::Config {
                    bucket: bucket.to_string(),
                    description: description.to_string(),
                    max_age: std::time::Duration::from_secs(30 * 24 * 60 * 60),
                    max_bytes: 1024 * 1024 * 1024,
                    storage: async_nats::jetstream::stream::StorageType::File,
                    num_replicas: 1,
                    ..Default::default()
                };
                js.create_key_value(cfg).await.map_err(map_nats_err)
            }
        }
    }

    pub async fn ensure_kv_with_config(
        js: &async_nats::jetstream::Context,
        cfg: async_nats::jetstream::kv::Config,
    ) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        let bucket = cfg.bucket.clone();
        match js.get_key_value(&bucket).await {
            Ok(store) => Ok(store),
            Err(_) => js.create_key_value(cfg).await.map_err(map_nats_err),
        }
    }
}

pub use file_repository::*;
pub use node_repository::*;
pub use rollout_repository::*;
pub use task_repository::*;
