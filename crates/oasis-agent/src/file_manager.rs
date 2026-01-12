use crate::nats_client::NatsClient;
use async_nats::jetstream;
use futures::StreamExt;
use oasis_core::{
    constants::*,
    core_types::AgentId,
    error::{CoreError, ErrorSeverity, Result},
    file_types::FileConfig,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{os::unix::fs::PermissionsExt, path::Path};
use tokio::fs;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct FileManager {
    agent_id: AgentId,
    nats_client: NatsClient,
    shutdown_token: CancellationToken,
}

impl FileManager {
    pub fn new(
        agent_id: AgentId,
        nats_client: NatsClient,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            agent_id,
            nats_client,
            shutdown_token,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting file manager");

        // 创建文件任务消费者
        let consumer = self.create_file_consumer().await?;
        let mut messages = consumer.messages().await?;

        loop {
            tokio::select! {
                Some(msg_result) = messages.next() => {
                    match msg_result {
                        Ok(msg) => {
                            if let Err(e) = self.process_file_message(msg).await {
                                error!("Failed to process file message: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("Error receiving file message: {}", e);
                        }
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    info!("File manager shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// 创建文件任务消费者
    async fn create_file_consumer(
        &self,
    ) -> Result<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
        let stream = self
            .nats_client
            .jetstream
            .get_stream(JS_STREAM_FILES)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to get files stream: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        // 创建持久化消费者，只接收发给此Agent的文件任务
        let consumer_name = format!("agent-files-{}", self.agent_id);
        let filter_subject = format!("{}.{}", FILES_SUBJECT_PREFIX, self.agent_id);

        let consumer = stream
            .create_consumer(
                oasis_core::nats::ConsumerConfigBuilder::new(
                    consumer_name.clone(),
                    filter_subject.clone(),
                )
                .max_deliver(1)
                .ack_wait_secs(300)
                .build(),
            )
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to create file consumer: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        info!(
            "Created file consumer: {} for subject: {}",
            consumer_name, filter_subject
        );

        Ok(consumer)
    }

    /// 处理文件消息
    async fn process_file_message(&self, msg: jetstream::Message) -> Result<()> {
        debug!("Received file message: {:?}", msg.info());

        // 解析文件应用配置
        let file_config = match oasis_core::proto::FileConfigMsg::decode(msg.payload.as_ref()) {
            Ok(file_config_msg) => {
                // 从 proto 转换为内部类型
                FileConfig::try_from(file_config_msg).map_err(|e| {
                    error!("Failed to convert FileConfigMsg to FileConfig: {}", e);
                    e
                })?
            }
            Err(e) => {
                error!("Failed to decode file apply config message: {}", e);
                // 文件任务失败不重试，直接 ack
                if let Err(ack_err) = msg.ack().await {
                    warn!("Failed to ack failed file message: {}", ack_err);
                }
                return Err(CoreError::Serialization {
                    message: format!("Failed to decode file apply config: {}", e),
                    severity: ErrorSeverity::Error,
                });
            }
        };

        debug!(
            "Processing file: {} -> {}",
            file_config.source_path, file_config.destination_path
        );

        // 执行文件部署
        let result = self.apply_file(&file_config).await;

        match &result {
            Ok(_) => {
                info!(
                    "Successfully applied file: {} -> {}",
                    file_config.source_path, file_config.destination_path
                );
                if let Err(e) = msg.ack().await {
                    warn!("Failed to ack file message: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to apply file {}: {}", file_config.source_path, e);
                // 文件任务失败不重试，直接 ack
                if let Err(ack_err) = msg.ack().await {
                    warn!("Failed to ack failed file message: {}", ack_err);
                }
            }
        }

        result
    }

    /// 应用文件到本地系统
    async fn apply_file(&self, config: &FileConfig) -> Result<()> {
        // 验证配置
        config.validate().map_err(|e| CoreError::Validation {
            message: format!("Invalid config: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        // 1. 从 Object Store 下载文件
        let file_data = self.download_from_object_store(config).await?;

        // 2. 应用到本地文件系统
        self.deploy_file_to_local(&file_data, config).await?;

        Ok(())
    }

    /// 从 Object Store 下载文件
    async fn download_from_object_store(&self, config: &FileConfig) -> Result<Vec<u8>> {
        debug!("Downloading file {}", config.source_path);

        let object_store = self
            .nats_client
            .jetstream
            .get_object_store(JS_OBJ_ARTIFACTS)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to get object store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        let mut path_hasher = Sha256::new();
        path_hasher.update(config.source_path.as_bytes());
        let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
        let filename = std::path::Path::new(&config.source_path)
            .file_name()
            .ok_or_else(|| CoreError::Validation {
                message: "Invalid source path: no filename".to_string(),
                severity: ErrorSeverity::Error,
            })?
            .to_string_lossy();
        let object_key = format!("{}/{}.v{}", path_hash, filename, config.revision);

        let mut object = object_store
            .get(object_key)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to get object from store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        let mut data = Vec::new();

        use tokio::io::AsyncReadExt;
        object
            .read_to_end(&mut data)
            .await
            .map_err(|e| CoreError::Io {
                message: format!("Failed to read object data: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        info!(
            "Downloaded file: {} ({} bytes)",
            config.source_path,
            data.len()
        );

        Ok(data)
    }

    /// 部署文件到本地文件系统
    async fn deploy_file_to_local(&self, data: &[u8], config: &FileConfig) -> Result<()> {
        let dest_path = Path::new(&config.destination_path);

        info!(
            "Deploying file to: {} (size: {} bytes)",
            dest_path.display(),
            data.len()
        );

        self.direct_write(dest_path, data).await?;

        // 设置权限
        if let Some(mode_str) = &config.mode {
            self.set_file_mode(dest_path, mode_str).await?;
        }

        // 设置所有者
        if let Some(owner_str) = &config.owner {
            self.set_file_owner(dest_path, owner_str).await?;
        }

        info!("File deployed successfully: {}", dest_path.display());
        Ok(())
    }

    /// 直接写入文件
    async fn direct_write(&self, dest_path: &Path, data: &[u8]) -> Result<()> {
        debug!("Direct write to: {}", dest_path.display());

        // 确保目标目录存在
        if let Some(parent_dir) = dest_path.parent() {
            fs::create_dir_all(parent_dir)
                .await
                .map_err(|e| CoreError::Io {
                    message: format!("Failed to create parent directory: {}", e),
                    severity: ErrorSeverity::Error,
                })?;
        }

        fs::write(dest_path, data)
            .await
            .map_err(|e| CoreError::Io {
                message: format!("Failed to write file: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        Ok(())
    }

    /// 设置文件权限
    async fn set_file_mode(&self, path: &Path, mode_str: &str) -> Result<()> {
        if mode_str.is_empty() {
            return Ok(());
        }

        // 解析八进制权限
        let mode =
            u32::from_str_radix(mode_str.trim_start_matches("0o").trim_start_matches("0"), 8)
                .map_err(|e| CoreError::Validation {
                    message: format!("Failed to parse file mode: {}", e),
                    severity: ErrorSeverity::Error,
                })?;

        debug!("Setting file mode: {} -> 0o{:o}", path.display(), mode);

        let metadata = fs::metadata(path).await.map_err(|e| CoreError::Io {
            message: format!("Failed to get file metadata: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        let mut permissions = metadata.permissions();
        permissions.set_mode(mode);

        fs::set_permissions(path, permissions)
            .await
            .map_err(|e| CoreError::Io {
                message: format!("Failed to set file permissions: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        Ok(())
    }

    /// 设置文件所有者
    async fn set_file_owner(&self, path: &Path, owner_str: &str) -> Result<()> {
        if owner_str.is_empty() {
            return Ok(());
        }

        debug!("Setting file owner: {} -> {}", path.display(), owner_str);

        // 使用 chown 命令设置所有者
        let output = tokio::process::Command::new("chown")
            .arg(owner_str)
            .arg(path)
            .output()
            .await
            .map_err(|e| CoreError::Io {
                message: format!("Failed to execute chown command: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CoreError::Io {
                message: format!("Failed to set file owner: {}", stderr),
                severity: ErrorSeverity::Error,
            });
        }

        Ok(())
    }
}
