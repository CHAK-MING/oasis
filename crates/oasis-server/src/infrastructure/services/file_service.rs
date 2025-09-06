//! File Service - 直接管理文件数据
//! 使用 oasis-core 的统一类型，提供简洁的文件管理接口

use async_nats::jetstream::{Context, object_store::ObjectStore};
use oasis_core::core_types::AgentId;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

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
    async fn upload_internal(&self, name: &str, data: Vec<u8>) -> Result<FileOperationResult> {
        debug!("Uploading file: {} ({} bytes)", name, data.len());

        // 计算文件的 SHA256
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let checksum = format!("{:x}", hasher.finalize());

        // 获取对象存储
        let store = self.get_object_store().await?;

        match store.put(name, &mut data.as_slice()).await {
            Ok(info) => {
                info!(
                    "Successfully uploaded file: {} ({} bytes, SHA256: {})",
                    name, info.size, checksum
                );

                Ok(FileOperationResult::success(format!(
                    "File uploaded successfully: {} ({} bytes, SHA256: {})",
                    name, info.size, checksum
                )))
            }
            Err(e) => {
                error!("Failed to upload file {}: {}", name, e);
                Err(CoreError::Internal {
                    message: format!("Failed to upload file: {}", e),
                    severity: ErrorSeverity::Error,
                })
            }
        }
    }

    /// 应用到对应的 agents
    async fn apply_internal(
        &self,
        config: Option<oasis_core::proto::FileApplyConfigMsg>,
        agent_ids: Vec<AgentId>,
    ) -> Result<FileOperationResult> {
        let config = config.ok_or_else(|| CoreError::InvalidTask {
            reason: "FileApplyConfig is required".to_string(),
            severity: ErrorSeverity::Error,
        })?;

        info!(
            "Applying file {} to {} agents: {:?}",
            config.name,
            agent_ids.len(),
            agent_ids
        );

        // 验证文件是否存在于 Object Store
        let store = self.get_object_store().await?;
        let _object_info = store
            .info(&config.name)
            .await
            .map_err(|e| CoreError::Internal {
                message: format!("File not found in object store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        // 验证配置
        let internal_config =
            FileApplyConfig::try_from(&config).map_err(|e| CoreError::InvalidTask {
                reason: format!("Invalid file apply config: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        internal_config.validate()?;

        // 向每个 Agent 发送文件应用任务
        let mut applied_agents = Vec::new();
        let mut failed_agents = Vec::new();

        for agent_id in agent_ids {
            match self.send_file_apply_task(&config, &agent_id).await {
                Ok(_) => {
                    applied_agents.push(agent_id.clone());
                    info!("Sent file apply task to agent: {}", agent_id);
                }
                Err(e) => {
                    failed_agents.push(agent_id.clone());
                    error!(
                        "Failed to send file apply task to agent {}: {}",
                        agent_id, e
                    );
                }
            }
        }

        let message = if failed_agents.is_empty() {
            format!(
                "File {} sent to {} agents successfully",
                config.name,
                applied_agents.len()
            )
        } else {
            format!(
                "File {} sent to {}/{} agents (failed: {})",
                config.name,
                applied_agents.len(),
                applied_agents.len() + failed_agents.len(),
                failed_agents.len()
            )
        };

        let success = failed_agents.is_empty();

        Ok(FileOperationResult { success, message })
    }

    async fn send_file_apply_task(
        &self,
        config: &oasis_core::proto::FileApplyConfigMsg,
        agent_id: &AgentId,
    ) -> Result<()> {
        // 序列化配置为 protobuf
        use prost::Message;
        let data = config.encode_to_vec();

        // 构建 subject：files.{agent_id}
        let subject = format!("{}.{}", FILES_SUBJECT_PREFIX, agent_id);

        // 设置去重头部
        let mut headers = async_nats::HeaderMap::new();
        let dedupe_key = format!("file-{}@{}", config.name, agent_id);
        headers.insert("Nats-Msg-Id", dedupe_key);

        // 发布消息到 JS_STREAM_FILES
        let ack = self
            .jetstream
            .publish_with_headers(subject.clone(), headers, data.into())
            .await
            .map_err(|e| CoreError::Internal {
                message: format!("Failed to publish file task: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        // 等待确认
        ack.await.map_err(|e| CoreError::Internal {
            message: format!("Failed to confirm file task publish: {}", e),
            severity: ErrorSeverity::Error,
        })?;

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
}

impl FileService {
    /// 上传文件到Object Store
    pub async fn upload(&self, name: &str, data: Vec<u8>) -> Result<FileOperationResult> {
        self.upload_internal(name, data).await
    }

    /// 下发文件到对应的 agents
    pub async fn apply(
        &self,
        config: Option<oasis_core::proto::FileApplyConfigMsg>,
        agent_ids: Vec<AgentId>,
    ) -> Result<FileOperationResult> {
        self.apply_internal(config, agent_ids).await
    }

    /// 清空所有文件
    pub async fn clear_all(&self) -> Result<usize> {
        self.clear_all_internal().await
    }
}
