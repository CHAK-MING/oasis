use std::sync::Arc;

use oasis_core::error::CoreError;

use crate::application::ports::repositories::{ResultConsumer, TaskRepository};

/// 任务结果流用例
pub struct StreamTaskResultsUseCase {
    task_repo: Arc<dyn TaskRepository>,
}

impl StreamTaskResultsUseCase {
    pub fn new(task_repo: Arc<dyn TaskRepository>) -> Self {
        Self { task_repo }
    }

    /// 创建结果消费者（用于流式推送）
    pub async fn create_consumer(
        &self,
        task_id: &str,
    ) -> Result<Box<dyn ResultConsumer>, CoreError> {
        self.task_repo.create_result_consumer(task_id).await
    }
}
