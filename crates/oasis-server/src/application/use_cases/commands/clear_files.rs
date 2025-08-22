use std::sync::Arc;

use oasis_core::error::CoreError;

use crate::application::ports::repositories::FileRepository;

/// 清理文件用例
pub struct ClearFilesUseCase {
    file_repo: Arc<dyn FileRepository>,
}

impl ClearFilesUseCase {
    pub fn new(file_repo: Arc<dyn FileRepository>) -> Self {
        Self { file_repo }
    }

    /// 清理所有文件
    pub async fn clear_all(&self) -> Result<u64, CoreError> {
        self.file_repo.clear_all().await
    }
}


