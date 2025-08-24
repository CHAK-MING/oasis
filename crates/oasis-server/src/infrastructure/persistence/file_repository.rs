use async_trait::async_trait;
use futures::StreamExt;
use oasis_core::error::CoreError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::application::ports::repositories::FileRepository;
use crate::domain::models::file::{FileInfo, FileUploadResult};
use crate::infrastructure::persistence::utils as persist;

const OS_META_PREFIX: &str = "__meta"; // sidecar metadata objects to avoid large downloads

/// 文件元数据，存储在 KV Store 中
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileMetadata {
    name: String,
    size: u64,
    checksum: String,
    content_type: String,
    uploaded_at: i64,
}

/// 文件仓储实现 - 基于NATS对象存储
pub struct NatsFileRepository {
    jetstream: async_nats::jetstream::Context,
}

impl NatsFileRepository {
    pub fn new(jetstream: async_nats::jetstream::Context) -> Self {
        Self { jetstream }
    }

    /// 确保对象存储存在
    async fn ensure_object_store(
        &self,
    ) -> Result<async_nats::jetstream::object_store::ObjectStore, CoreError> {
        match self
            .jetstream
            .get_object_store(oasis_core::JS_OBJ_ARTIFACTS)
            .await
        {
            Ok(store) => Ok(store),
            Err(_) => {
                let cfg = async_nats::jetstream::object_store::Config {
                    bucket: oasis_core::JS_OBJ_ARTIFACTS.to_string(),
                    description: Some("File artifacts storage".to_string()),
                    max_bytes: 1024 * 1024 * 1024 * 10, // 10GB
                    max_age: std::time::Duration::from_secs(30 * 24 * 60 * 60),
                    storage: async_nats::jetstream::stream::StorageType::File,
                    num_replicas: 1,
                    ..Default::default()
                };
                self.jetstream
                    .create_object_store(cfg)
                    .await
                    .map_err(persist::map_nats_err)
            }
        }
    }

    /// 确保文件元数据 KV Store 存在
    async fn ensure_metadata_store(&self) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        persist::ensure_kv(&self.jetstream, "file-metadata", "File metadata storage").await
    }

    // 内部辅助删除方法
    pub async fn _delete_internal(&self, name: &str) -> Result<(), CoreError> {
        let store = self.ensure_object_store().await?;
        let metadata_store = self.ensure_metadata_store().await?;

        // 删除对象存储中的文件
        if let Err(e) = store.delete(name).await {
            return Err(CoreError::Nats {
                message: e.to_string(),
            });
        }

        // 删除对象存储中的 sidecar 元数据
        let meta_object_name = format!("{}/{}.json", OS_META_PREFIX, name);
        if let Err(e) = store.delete(&meta_object_name).await {
            tracing::warn!(
                file = %name,
                sidecar = %meta_object_name,
                error = %e,
                "Failed to delete object-store metadata sidecar"
            );
        }

        // 删除 KV Store 中的元数据
        if let Err(e) = metadata_store.delete(name).await {
            tracing::warn!("Failed to delete metadata for file '{}': {}", name, e);
        }

        Ok(())
    }
}

#[async_trait]
impl FileRepository for NatsFileRepository {
    async fn upload(&self, name: &str, data: Vec<u8>) -> Result<FileUploadResult, CoreError> {
        let store = self.ensure_object_store().await?;
        let metadata_store = self.ensure_metadata_store().await?;

        // 计算校验和
        let checksum_hex = format!("{:x}", Sha256::digest(&data));

        // 上传文件（主体对象）
        let mut reader = data.as_slice();
        store
            .put(name, &mut reader)
            .await
            .map_err(persist::map_nats_err)?;

        // 存储文件元数据到 KV Store
        let metadata = FileMetadata {
            name: name.to_string(),
            size: data.len() as u64,
            checksum: checksum_hex.clone(),
            content_type: "application/octet-stream".to_string(),
            uploaded_at: chrono::Utc::now().timestamp(),
        };

        let metadata_json = serde_json::to_string(&metadata).map_err(|e| CoreError::Nats {
            message: format!("Failed to serialize metadata: {}", e),
        })?;

        metadata_store
            .put(name, metadata_json.into())
            .await
            .map_err(persist::map_nats_err)?;

        // 额外写入对象存储的元数据 sidecar，避免 KV 缺失时需要下载大文件
        // 路径示例："__meta/<name>.json"
        let meta_object_name = format!("{}/{}.json", OS_META_PREFIX, name);
        if let Ok(meta_reader_vec) = serde_json::to_vec(&metadata) {
            let mut meta_reader = meta_reader_vec.as_slice();
            // 注意：ObjectStore::put 的第一个参数要求实现 Into<ObjectMetadata>，&str 满足该约束
            if let Err(e) = store.put(meta_object_name.as_str(), &mut meta_reader).await {
                tracing::warn!(
                    file = %name,
                    sidecar = %meta_object_name,
                    error = %e,
                    "Failed to write object-store metadata sidecar"
                );
            }
        }

        Ok(FileUploadResult {
            object_name: name.to_string(),
            size: data.len() as u64,
            checksum: checksum_hex,
        })
    }

    async fn get_info(&self, name: &str) -> Result<Option<FileInfo>, CoreError> {
        let store = self.ensure_object_store().await?;
        let metadata_store = self.ensure_metadata_store().await?;

        // 首先尝试从 KV Store 获取元数据（性能最优）
        if let Ok(Some(entry)) = metadata_store.get(name).await {
            if let Ok(metadata) = serde_json::from_slice::<FileMetadata>(&entry) {
                return Ok(Some(FileInfo {
                    name: metadata.name,
                    size: metadata.size,
                    checksum: metadata.checksum,
                    content_type: metadata.content_type,
                }));
            }
        }

        // 其次尝试从对象存储的 sidecar 元数据读取（避免下载大文件）
        let meta_object_name = format!("{}/{}.json", OS_META_PREFIX, name);
        if let Ok(mut meta_obj) = store.get(meta_object_name.as_str()).await {
            let mut meta_buf = Vec::new();
            if meta_obj.read_to_end(&mut meta_buf).await.is_ok() {
                if let Ok(metadata) = serde_json::from_slice::<FileMetadata>(&meta_buf) {
                    return Ok(Some(FileInfo {
                        name: metadata.name,
                        size: metadata.size,
                        checksum: metadata.checksum,
                        content_type: metadata.content_type,
                    }));
                }
            }
        }

        // 如果 KV Store 中没有元数据，尝试从对象存储获取信息
        match store.get(name).await {
            Ok(mut object) => {
                // 获取对象元数据
                let info = object.info();
                let size = info.size as u64;

                // 如果对象有元数据中的校验和，直接使用
                if let Some(checksum) = info.headers.as_ref().and_then(|h| h.get("sha256")) {
                    Ok(Some(FileInfo {
                        name: name.to_string(),
                        size,
                        checksum: checksum.to_string(),
                        content_type: "application/octet-stream".to_string(),
                    }))
                } else {
                    // 如果没有元数据中的校验和，需要读取数据计算（性能较差）
                    let mut data = Vec::new();
                    object
                        .read_to_end(&mut data)
                        .await
                        .map_err(persist::map_nats_err)?;

                    let checksum = Sha256::digest(&data);
                    let checksum_hex = format!("{:x}", checksum);

                    Ok(Some(FileInfo {
                        name: name.to_string(),
                        size: data.len() as u64,
                        checksum: checksum_hex,
                        content_type: "application/octet-stream".to_string(),
                    }))
                }
            }
            Err(_) => Ok(None),
        }
    }

    async fn clear_all(&self) -> Result<u64, CoreError> {
        let store = self.ensure_object_store().await?;
        let metadata_store = self.ensure_metadata_store().await?;

        let mut deleted: u64 = 0;
        let mut iter = store.list().await.map_err(persist::map_nats_err)?;

        while let Some(res) = iter.next().await {
            match res {
                Ok(info) => {
                    // 删除对象存储中的文件
                    if store.delete(&info.name).await.is_ok() {
                        // 删除对应的元数据
                        if let Err(e) = metadata_store.delete(&info.name).await {
                            tracing::warn!(
                                "Failed to delete metadata for file '{}': {}",
                                info.name,
                                e
                            );
                        }
                        deleted += 1;
                    }
                }
                Err(_e) => continue,
            }
        }

        // 清理可能残留的元数据（文件已删除但元数据还在的情况）
        let mut metadata_iter = metadata_store.keys().await.map_err(persist::map_nats_err)?;

        while let Some(key) = metadata_iter.next().await {
            if let Ok(key) = key {
                // 检查对应的文件是否还存在
                if store.get(&key).await.is_err() {
                    // 文件不存在，删除元数据
                    if let Err(e) = metadata_store.delete(&key).await {
                        tracing::warn!(
                            "Failed to delete orphaned metadata for key '{}': {}",
                            key,
                            e
                        );
                    }
                }
            }
        }

        Ok(deleted)
    }
}
