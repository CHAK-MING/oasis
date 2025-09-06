use crate::nats_client::NatsClient;
use anyhow::{Context, Result};
use async_nats::jetstream;
use futures::StreamExt;
use oasis_core::{constants::*, core_types::AgentId, file_types::FileApplyConfig};
use prost::Message;
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
            .context("Failed to get files stream")?;

        // 创建持久化消费者，只接收发给此Agent的文件任务
        let consumer_name = format!("agent-files-{}", self.agent_id);
        let filter_subject = format!("{}.{}", FILES_SUBJECT_PREFIX, self.agent_id);

        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                filter_subject: filter_subject.clone(),
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                max_deliver: 1, // 文件任务只尝试一次，失败就失败
                ack_wait: std::time::Duration::from_secs(300), // 5分钟超时
                ..Default::default()
            })
            .await
            .context("Failed to create file consumer")?;

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
        let file_apply_config =
            match oasis_core::proto::FileApplyConfigMsg::decode(msg.payload.as_ref()) {
                Ok(file_apply_config_msg) => {
                    // 从 proto 转换为内部类型
                    FileApplyConfig::try_from(file_apply_config_msg).map_err(|e| {
                        error!(
                            "Failed to convert FileApplyConfigMsg to FileApplyConfig: {}",
                            e
                        );
                        e
                    })?
                }
                Err(e) => {
                    error!("Failed to decode file apply config message: {}", e);
                    // 文件任务失败不重试，直接 ack
                    if let Err(ack_err) = msg.ack().await {
                        warn!("Failed to ack failed file message: {}", ack_err);
                    }
                    return Err(anyhow::anyhow!("Failed to decode file apply config: {}", e));
                }
            };

        debug!(
            "Processing file: {} -> {}",
            file_apply_config.name, file_apply_config.destination_path
        );

        // 执行文件部署
        let result = self.apply_file(&file_apply_config).await;

        match &result {
            Ok(_) => {
                info!(
                    "Successfully applied file: {} -> {}",
                    file_apply_config.name, file_apply_config.destination_path
                );
                if let Err(e) = msg.ack().await {
                    warn!("Failed to ack file message: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to apply file {}: {}", file_apply_config.name, e);
                // 文件任务失败不重试，直接 ack
                if let Err(ack_err) = msg.ack().await {
                    warn!("Failed to ack failed file message: {}", ack_err);
                }
            }
        }

        result
    }

    /// 应用文件到本地系统
    async fn apply_file(&self, config: &FileApplyConfig) -> Result<()> {
        // 验证配置
        config
            .validate()
            .map_err(|e| anyhow::anyhow!("Invalid config: {}", e))?;

        // 1. 从 Object Store 下载文件
        let file_data = self.download_from_object_store(&config.name).await?;

        // 2. 应用到本地文件系统
        self.deploy_file_to_local(&file_data, config).await?;

        Ok(())
    }

    /// 从 Object Store 下载文件
    async fn download_from_object_store(&self, object_name: &str) -> Result<Vec<u8>> {
        debug!("Downloading file from object store: {}", object_name);

        let object_store = self
            .nats_client
            .jetstream
            .get_object_store(JS_OBJ_ARTIFACTS)
            .await
            .context("Failed to get object store")?;

        let mut object = object_store
            .get(object_name)
            .await
            .context("Failed to get object from store")?;

        let mut data = Vec::new();

        // 使用 AsyncReadExt 读取所有数据
        use tokio::io::AsyncReadExt;
        object
            .read_to_end(&mut data)
            .await
            .context("Failed to read object data")?;

        info!("Downloaded file: {} ({} bytes)", object_name, data.len());

        Ok(data)
    }

    /// 部署文件到本地文件系统
    async fn deploy_file_to_local(&self, data: &[u8], config: &FileApplyConfig) -> Result<()> {
        let dest_path = Path::new(&config.destination_path);

        info!(
            "Deploying file to: {} (atomic: {}, size: {} bytes)",
            dest_path.display(),
            config.atomic,
            data.len()
        );

        if config.atomic {
            self.atomic_write(dest_path, data).await?;
        } else {
            self.direct_write(dest_path, data).await?;
        }

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

    /// 原子写入文件
    async fn atomic_write(&self, dest_path: &Path, data: &[u8]) -> Result<()> {
        // 在同一目录创建临时文件
        let temp_path = {
            let mut temp_name = dest_path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("Invalid destination path"))?
                .to_owned();
            temp_name.push(".tmp");

            dest_path.with_file_name(temp_name)
        };

        debug!(
            "Atomic write: {} -> {}",
            temp_path.display(),
            dest_path.display()
        );

        // 确保目标目录存在
        if let Some(parent_dir) = dest_path.parent() {
            fs::create_dir_all(parent_dir)
                .await
                .context("Failed to create parent directory")?;
        }

        // 写入临时文件
        fs::write(&temp_path, data)
            .await
            .context("Failed to write to temporary file")?;

        // 原子移动
        fs::rename(&temp_path, dest_path)
            .await
            .context("Failed to move temporary file to destination")?;

        Ok(())
    }

    /// 直接写入文件
    async fn direct_write(&self, dest_path: &Path, data: &[u8]) -> Result<()> {
        debug!("Direct write to: {}", dest_path.display());

        // 确保目标目录存在
        if let Some(parent_dir) = dest_path.parent() {
            fs::create_dir_all(parent_dir)
                .await
                .context("Failed to create parent directory")?;
        }

        fs::write(dest_path, data)
            .await
            .context("Failed to write file")?;

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
                .context("Failed to parse file mode")?;

        debug!("Setting file mode: {} -> 0o{:o}", path.display(), mode);

        let metadata = fs::metadata(path)
            .await
            .context("Failed to get file metadata")?;

        let mut permissions = metadata.permissions();
        permissions.set_mode(mode);

        fs::set_permissions(path, permissions)
            .await
            .context("Failed to set file permissions")?;

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
            .context("Failed to execute chown command")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("Failed to set file owner: {}", stderr));
        }

        Ok(())
    }
}
