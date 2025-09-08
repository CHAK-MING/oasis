//! File Service - 直接管理文件数据
//! 使用 oasis-core 的统一类型，提供简洁的文件管理接口

use async_nats::jetstream::{Context, object_store::ObjectStore};
use oasis_core::core_types::AgentId;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use futures::StreamExt;
use oasis_core::backoff::{execute_with_backoff, fast_backoff, network_publish_backoff};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use oasis_core::{FILES_SUBJECT_PREFIX, file_types::*};

/// 文件仓库 - 管理文件的上传、存储和分发
pub struct FileService {
    jetstream: Arc<Context>,
}

impl FileService {
    pub async fn new(jetstream: Arc<Context>) -> Result<Self> {
        // 确保对象存储存在
        Self::ensure_object_store(&jetstream).await?;

        Ok(Self { jetstream })
    }

    /// 确保对象存储已创建
    async fn ensure_object_store(js: &Context) -> Result<()> {
        match js
            .get_object_store(oasis_core::constants::JS_OBJ_ARTIFACTS)
            .await
        {
            Ok(_) => {
                debug!(
                    "Object store {} already exists",
                    oasis_core::constants::JS_OBJ_ARTIFACTS
                );
                Ok(())
            }
            Err(_) => {
                error!(
                    "Failed to get object store: {}",
                    oasis_core::constants::JS_OBJ_ARTIFACTS
                );
                Err(CoreError::Internal {
                    message: format!(
                        "Failed to get object store: {}",
                        oasis_core::constants::JS_OBJ_ARTIFACTS
                    ),
                    severity: ErrorSeverity::Error,
                })
            }
        }
    }

    /// 上传文件到对象存储 - 返回统一类型的结果
    async fn upload_internal(
        &self,
        source_path: &String,
        data: Vec<u8>,
    ) -> Result<FileOperationResult> {
        debug!("Uploading file: {} ({} bytes)", source_path, data.len());

        // 生成路径hash和对象key
        let mut path_hasher = Sha256::new();
        path_hasher.update(source_path.as_bytes());
        let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
        let filename = std::path::Path::new(source_path)
            .file_name()
            .unwrap()
            .to_string_lossy();
        let revision = chrono::Utc::now().timestamp() as u64;
        let object_key = format!("{}/{}.v{}", path_hash, filename, revision);

        // 计算文件的 SHA256
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let checksum = format!("{:x}", hasher.finalize());

        // 获取对象存储
        let store = self.get_object_store().await?;

        let put_res = execute_with_backoff(
            || async {
                store
                    .put(object_key.as_str(), &mut data.as_slice())
                    .await
                    .map_err(|e| CoreError::Internal {
                        message: format!("Failed to upload file: {}", e),
                        severity: ErrorSeverity::Error,
                    })
            },
            fast_backoff(),
        )
        .await;

        match put_res {
            Ok(info) => {
                info!(
                    "Successfully uploaded file: {} -> {} (nuid: {}, {} bytes, SHA256: {})",
                    source_path, object_key, info.nuid, info.size, checksum
                );

                Ok(FileOperationResult::success(
                    format!(
                        "File uploaded successfully: {} -> {} (nuid: {}, {} bytes, SHA256: {})",
                        source_path, object_key, info.nuid, info.size, checksum
                    ),
                    revision,
                ))
            }
            Err(e) => {
                error!("Failed to upload file {}: {}", source_path, e);
                Err(e)
            }
        }
    }

    /// 应用到对应的 agents
    async fn apply_internal(
        &self,
        config: &oasis_core::proto::FileConfigMsg,
        agent_ids: Vec<AgentId>,
    ) -> Result<FileOperationResult> {
        info!(
            "Applying file {} to {} agents: {:?}",
            config.source_path,
            agent_ids.len(),
            agent_ids
        );

        // 并行向每个 Agent 发送文件应用任务
        let concurrency_limit = 32usize;
        let futs = agent_ids.into_iter().map(|aid| async move {
            let res = self.send_file_apply_task(&config, &aid).await;
            (aid, res)
        });

        use futures::stream;
        let mut applied_agents = Vec::new();
        let mut failed_agents = Vec::new();

        stream::iter(futs)
            .buffer_unordered(concurrency_limit)
            .for_each(|(agent_id, res)| {
                if res.is_ok() {
                    applied_agents.push(agent_id.clone());
                    info!("Sent file apply task to agent: {}", agent_id);
                } else if let Err(e) = res {
                    failed_agents.push(agent_id.clone());
                    error!(
                        "Failed to send file apply task to agent {}: {}",
                        agent_id, e
                    );
                }
                std::future::ready(())
            })
            .await;

        let message = if failed_agents.is_empty() {
            format!(
                "File {} sent to {} agents successfully",
                config.source_path,
                applied_agents.len()
            )
        } else {
            format!(
                "File {} sent to {}/{} agents (failed: {})",
                config.source_path,
                applied_agents.len(),
                applied_agents.len() + failed_agents.len(),
                failed_agents.len()
            )
        };

        let success = failed_agents.is_empty();

        // 成功后更新当前版本指针
        if success {
            if let Err(e) = self
                .set_active_revision(&config.source_path, config.revision)
                .await
            {
                warn!("Failed to update active revision pointer: {}", e);
            }
        }

        Ok(FileOperationResult {
            success,
            message,
            revision: config.revision,
        })
    }

    async fn send_file_apply_task(
        &self,
        config: &oasis_core::proto::FileConfigMsg,
        agent_id: &AgentId,
    ) -> Result<()> {
        // 序列化配置为 protobuf
        use prost::Message;
        let data = config.encode_to_vec();

        // 构建 subject：files.{agent_id}
        let subject = format!("{}.{}", FILES_SUBJECT_PREFIX, agent_id);

        // 设置去重头部
        let mut headers = async_nats::HeaderMap::new();
        let dedupe_key = format!(
            "file-{}@{}@{}",
            config.source_path, config.revision, agent_id
        );
        headers.insert("Nats-Msg-Id", dedupe_key);

        // 发布并等待 ACK
        execute_with_backoff(
            || async {
                let ack = self
                    .jetstream
                    .publish_with_headers(subject.clone(), headers.clone(), data.clone().into())
                    .await
                    .map_err(|e| CoreError::Internal {
                        message: format!("Failed to publish file task: {}", e),
                        severity: ErrorSeverity::Error,
                    })?;
                ack.await.map_err(|e| CoreError::Internal {
                    message: format!("Failed to confirm file task publish: {}", e),
                    severity: ErrorSeverity::Error,
                })
            },
            network_publish_backoff(),
        )
        .await?;

        debug!("Published file apply task to subject: {}", subject);
        Ok(())
    }

    /// 清空所有文件
    async fn clear_all_internal(&self) -> Result<usize> {
        debug!("Clearing all files from object store");

        let store = self.get_object_store().await?;

        // 列出所有对象
        let mut count = 0usize;
        let mut objects = Vec::new();

        // 收集所有对象名称
        let mut list = store.list().await.map_err(|e| CoreError::Internal {
            message: format!("Failed to list objects: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        use futures::StreamExt;
        while let Some(result) = list.next().await {
            match result {
                Ok(info) => {
                    objects.push(info.name.clone());
                }
                Err(e) => {
                    warn!("Error listing object: {}", e);
                }
            }
        }

        // 删除所有对象
        for name in objects {
            match store.delete(&name).await {
                Ok(_) => {
                    debug!("Deleted object: {}", name);
                    count += 1;
                }
                Err(e) => {
                    warn!("Failed to delete object {}: {}", name, e);
                }
            }
        }

        info!("Cleared {} files from object store", count);
        Ok(count)
    }

    /// 获取对象存储实例
    async fn get_object_store(&self) -> Result<ObjectStore> {
        self.jetstream
            .get_object_store(oasis_core::constants::JS_OBJ_ARTIFACTS)
            .await
            .map_err(|e| CoreError::Internal {
                message: format!("Failed to get object store: {}", e),
                severity: ErrorSeverity::Error,
            })
    }

    /// 更新指定源文件的当前版本指针（写入 `{path_hash}/{filename}.current` 对象，内容为 revision）
    async fn set_active_revision(&self, source_path: &str, revision: u64) -> Result<()> {
        let mut path_hasher = Sha256::new();
        path_hasher.update(source_path.as_bytes());
        let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
        let filename = std::path::Path::new(source_path)
            .file_name()
            .unwrap()
            .to_string_lossy();
        let pointer_key = format!("{}/{}.current", path_hash, filename);

        let content = revision.to_string().into_bytes();
        let store = self.get_object_store().await?;
        store
            .put(pointer_key.as_str(), &mut content.as_slice())
            .await
            .map_err(|e| CoreError::Internal {
                message: format!("Failed to update active revision pointer: {}", e),
                severity: ErrorSeverity::Error,
            })?;
        Ok(())
    }

    /// 读取指定源文件的当前版本指针（从 `{path_hash}/{filename}.current` 读取 revision）
    async fn get_active_revision(&self, source_path: &str) -> Result<Option<u64>> {
        let mut path_hasher = Sha256::new();
        path_hasher.update(source_path.as_bytes());
        let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
        let filename = std::path::Path::new(source_path)
            .file_name()
            .unwrap()
            .to_string_lossy();
        let pointer_key = format!("{}/{}.current", path_hash, filename);

        let store = self.get_object_store().await?;
        match store.get(pointer_key.as_str()).await {
            Ok(mut obj) => {
                let mut buf = Vec::new();
                use tokio::io::AsyncReadExt;
                obj.read_to_end(&mut buf)
                    .await
                    .map_err(|e| CoreError::Internal {
                        message: format!("Failed to read active revision pointer: {}", e),
                        severity: ErrorSeverity::Error,
                    })?;
                let s = String::from_utf8_lossy(&buf);
                let rev = s.trim().parse::<u64>().ok();
                Ok(rev)
            }
            Err(_) => Ok(None),
        }
    }
}

impl FileService {
    /// 上传文件到Object Store
    pub async fn upload(&self, source_path: &String, data: Vec<u8>) -> Result<FileOperationResult> {
        self.upload_internal(source_path, data).await
    }

    /// 下发文件到对应的 agents
    pub async fn apply(
        &self,
        config: &oasis_core::proto::FileConfigMsg,
        agent_ids: Vec<AgentId>,
    ) -> Result<FileOperationResult> {
        self.apply_internal(config, agent_ids).await
    }

    /// 清空所有文件
    pub async fn clear_all(&self) -> Result<usize> {
        self.clear_all_internal().await
    }

    /// 获取特定文件的历史版本
    pub async fn get_file_history(&self, source_path: &str) -> Result<Option<FileHistory>> {
        debug!("Getting file history for: {}", source_path);

        // 生成路径hash和文件名
        let mut path_hasher = Sha256::new();
        path_hasher.update(source_path.as_bytes());
        let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
        let filename = std::path::Path::new(source_path)
            .file_name()
            .unwrap()
            .to_string_lossy();

        let store = self.get_object_store().await?;

        // 使用 list() 获取所有对象，然后过滤出我们关心的文件
        let mut list = store.list().await.map_err(|e| CoreError::Internal {
            message: format!("Failed to list objects: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        let mut versions = Vec::new();
        let mut current_version = None;

        // 构建匹配模式：{path_hash}/{filename}.v*
        let pattern_prefix = format!("{}/{}.v", path_hash, filename);

        // 收集所有版本信息
        while let Some(result) = list.next().await {
            match result {
                Ok(object_info) => {
                    // 只处理匹配我们文件模式的对象
                    if object_info.name.starts_with(&pattern_prefix) {
                        // 从对象名称中提取时间戳
                        let timestamp_str = object_info
                            .name
                            .strip_prefix(&pattern_prefix)
                            .unwrap_or("0");
                        let timestamp = timestamp_str.parse::<i64>().unwrap_or(0);

                        let version = FileVersion {
                            name: filename.to_string(),
                            revision: timestamp as u64, // 使用时间戳作为版本号
                            size: object_info.size as u64,
                            checksum: object_info.digest.unwrap_or_default(),
                            created_at: object_info
                                .modified
                                .map(|t| t.unix_timestamp())
                                .unwrap_or(timestamp),
                            is_current: false, // 稍后确定当前版本
                        };

                        versions.push(version);
                    }
                }
                Err(e) => {
                    warn!("Error getting object info: {}", e);
                }
            }
        }

        if versions.is_empty() {
            return Ok(None);
        }

        // 按时间戳排序（最新的在前）
        versions.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        // 读取当前指针；若不存在则默认最新为当前
        let active_revision = self.get_active_revision(source_path).await?;
        if let Some(active) = active_revision {
            for v in &mut versions {
                v.is_current = v.revision == active;
            }
            current_version = Some(active);
        } else if let Some(latest_version) = versions.first_mut() {
            latest_version.is_current = true;
            current_version = Some(latest_version.revision);
        }

        let file_history = FileHistory {
            name: filename.to_string(),
            versions,
            current_version: current_version.unwrap_or(0),
        };

        Ok(Some(file_history))
    }

    /// 回滚文件到指定版本
    pub async fn rollback_file(
        &self,
        config: &oasis_core::proto::FileConfigMsg,
        agent_ids: Vec<AgentId>,
    ) -> Result<FileOperationResult> {
        debug!(
            "Rolling back file {} to revision {}",
            config.source_path, config.revision
        );

        // 下发到对应的 Agent
        self.apply_internal(config, agent_ids).await?;

        // 更新当前版本指针为回滚到的版本
        if let Err(e) = self
            .set_active_revision(&config.source_path, config.revision)
            .await
        {
            warn!(
                "Failed to update active revision pointer after rollback: {}",
                e
            );
        }

        info!(
            "Successfully rolled back file {} to revision {}",
            config.source_path, config.revision
        );
        Ok(FileOperationResult::success(
            format!(
                "File {} rolled back to revision {} and redeployed to target: {}",
                config.source_path,
                config.revision,
                config
                    .target
                    .as_ref()
                    .map(|t| t.expression.as_str())
                    .unwrap_or("")
            ),
            config.revision,
        ))
    }
}
