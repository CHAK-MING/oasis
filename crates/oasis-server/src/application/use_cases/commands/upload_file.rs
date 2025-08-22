use oasis_core::error::CoreError;
use std::sync::Arc;

use crate::application::ports::repositories::{FileRepository, TaskRepository};
use crate::domain::models::file::{FileApplyConfig, FileUploadResult};
use crate::domain::models::task::Task;
use oasis_core::types::AgentId;
use oasis_core::task::TaskSpec;
use oasis_core::types::TaskId;

/// 文件上传用例
pub struct UploadFileUseCase {
    file_repo: Arc<dyn FileRepository>,
    task_repo: Arc<dyn TaskRepository>,
}

impl UploadFileUseCase {
    pub fn new(file_repo: Arc<dyn FileRepository>, task_repo: Arc<dyn TaskRepository>) -> Self {
        Self {
            file_repo,
            task_repo,
        }
    }

    /// 上传文件到对象存储
    pub async fn push_file(
        &self,
        object_name: &str,
        file_data: Vec<u8>,
    ) -> Result<FileUploadResult, CoreError> {
        // 1. 验证文件名
        if object_name.is_empty() {
            return Err(CoreError::InvalidTask {
                reason: "Object name cannot be empty".to_string(),
                
            });
        }

        // 2. 验证文件数据
        if file_data.is_empty() {
            return Err(CoreError::InvalidTask {
                reason: "File data cannot be empty".to_string(),
                
            });
        }

        // 3. 上传文件
        self.file_repo.upload(object_name, file_data).await
    }

    /// 应用文件到目标节点
    pub async fn apply_file(
        &self,
        config: FileApplyConfig,
        target_nodes: Vec<String>,
    ) -> Result<String, CoreError> {
        // 1. 验证配置
        config.validate()?;

        // 2. 验证目标节点
        if target_nodes.is_empty() {
            return Err(CoreError::Agent {
                agent_id: "no target nodes specified".to_string(),
                message: "No target nodes specified".to_string(),
            });
        }

        // 3. 验证文件存在
        let file_info = if let Some(info) = self.file_repo.get_info(&config.object_name).await? {
            // 4. 验证SHA256（如果提供）
            if let Some(expected_sha256) = &config.expected_sha256 {
                info.validate_sha256(expected_sha256)?;
            }
            info
        } else {
            return Err(CoreError::InvalidTask {
                reason: format!("File '{}' not found in object store", config.object_name),
                
            });
        };

        // 5. 构建统一的文件应用消息（JSON 单参数）
        let msg = serde_json::json!({
            "object_name": config.object_name,
            "destination": config.destination_path,
            "sha256": file_info.checksum,
            "size": file_info.size,
            "mode": config.mode.clone().unwrap_or_default(),
            "owner": config.owner.clone().unwrap_or_default(),
            "atomic": config.atomic,
        })
        .to_string();

        // 6. 构建任务并通过仓储发布（统一使用 oasis:file-apply）
        let task_spec = TaskSpec::for_agents(
            TaskId::new(uuid::Uuid::new_v4().to_string()),
            oasis_core::constants::CMD_FILE_APPLY.to_string(),
            target_nodes.clone().into_iter().map(AgentId::from).collect(),
        )
        .with_args(vec![msg])
        .with_timeout(300);
        let task = Task::from_spec(task_spec);
        let _ = self.task_repo.create(task.clone()).await; // best-effort metadata persistence
        let task_id = self.task_repo.publish(task).await?;

        tracing::info!(
            task_id = %task_id,
            object_name = %config.object_name,
            destination = %config.destination_path,
            target_count = target_nodes.len(),
            "File apply task created and published via TaskRepository"
        );

        // 7. 返回异步任务ID
        Ok(task_id)
    }
}
