use super::status::TaskStatus;
use oasis_core::{error::CoreError, types::TaskSpec};

pub struct Task {
    pub spec: TaskSpec,
    pub status: TaskStatus,
    version: u64, // 用于乐观锁
}

impl Task {
    pub fn new(spec: TaskSpec) -> Self {
        Self {
            spec,
            status: TaskStatus::Pending,
            version: 1,
        }
    }

    /// 将任务状态转换为 Running
    pub fn start(&mut self) -> Result<(), CoreError> {
        self.transition_to(TaskStatus::Running)
    }

    /// 将任务状态转换为 Completed
    pub fn complete(&mut self, exit_code: i32) -> Result<(), CoreError> {
        self.transition_to(TaskStatus::Completed { exit_code })
    }

    /// 将任务状态转换为 Failed
    pub fn fail(&mut self, error: String) -> Result<(), CoreError> {
        self.transition_to(TaskStatus::Failed { error })
    }

    /// 将任务状态转换为 Timeout
    pub fn timeout(&mut self) -> Result<(), CoreError> {
        self.transition_to(TaskStatus::Timeout)
    }

    /// 核心状态转换逻辑，强制执行业务规则
    fn transition_to(&mut self, new_status: TaskStatus) -> Result<(), CoreError> {
        // 验证状态转换
        match (&self.status, &new_status) {
            (TaskStatus::Pending, TaskStatus::Running) => {}
            (TaskStatus::Running, TaskStatus::Completed { .. }) => {}
            (TaskStatus::Running, TaskStatus::Failed { .. }) => {}
            (TaskStatus::Running, TaskStatus::Timeout) => {}
            _ => {
                return Err(CoreError::InvalidTask {
                    reason: format!(
                        "Invalid state transition from {:?} to {:?}",
                        self.status, new_status
                    ),

                });
            }
        }

        // 应用新状态
        self.status = new_status;
        self.version += 1;
        Ok(())
    }

    /// 获取版本号（用于乐观锁）
    pub fn version(&self) -> u64 {
        self.version
    }

    /// 检查任务是否已完成（成功或失败）
    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            TaskStatus::Completed { .. } | TaskStatus::Failed { .. } | TaskStatus::Timeout
        )
    }
}
