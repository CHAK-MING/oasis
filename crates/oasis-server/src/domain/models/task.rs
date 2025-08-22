use oasis_core::{error::CoreError, types::TaskSpec};
use serde::{Deserialize, Serialize};

/// 任务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed { exit_code: i32 },
    Failed { error: String },
    Cancelled { reason: String },
    Timeout,
}

/// 任务结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: String,
    pub agent_id: String,
    pub status: TaskStatus,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub timestamp: i64,
}

impl TaskResult {
    /// 从状态中提取退出码
    pub fn exit_code(&self) -> i32 {
        match &self.status {
            TaskStatus::Completed { exit_code } => *exit_code,
            TaskStatus::Failed { .. } => -1,
            TaskStatus::Cancelled { .. } => -2,
            TaskStatus::Timeout => -3,
            _ => 0, // Pending/Running
        }
    }
}

/// 任务实体 - 业务逻辑封装
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub spec: TaskSpec,
    pub status: TaskStatus,
    pub created_at: i64,
    pub updated_at: i64,
    pub version: u64, // 乐观锁
}

impl Task {
    /// 从TaskSpec创建
    pub fn from_spec(spec: TaskSpec) -> Self {
        let now = chrono::Utc::now().timestamp();
        let id = spec.id.to_string();

        Self {
            id: id.clone(),
            spec,
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            version: 1,
        }
    }

    /// 业务规则：应用状态迁移
    pub fn apply_status(&mut self, new_status: TaskStatus) -> Result<(), CoreError> {
        // 验证状态转换
        self.validate_transition(new_status.clone())?;

        // 应用新状态
        self.status = new_status;
        self.updated_at = chrono::Utc::now().timestamp();
        self.version += 1;

        Ok(())
    }

    /// 业务规则：验证状态转换
    pub fn validate_transition(&self, new_status: TaskStatus) -> Result<(), CoreError> {
        match (self.status.clone(), new_status.clone()) {
            (TaskStatus::Pending, TaskStatus::Running) => Ok(()),
            (TaskStatus::Running, TaskStatus::Completed { .. }) => Ok(()),
            (TaskStatus::Running, TaskStatus::Failed { .. }) => Ok(()),
            (TaskStatus::Running, TaskStatus::Timeout) => Ok(()),
            (TaskStatus::Pending, TaskStatus::Cancelled { .. }) => Ok(()),
            (TaskStatus::Running, TaskStatus::Cancelled { .. }) => Ok(()),
            _ => Err(CoreError::InvalidTask {
                reason: format!(
                    "Invalid state transition from {:?} to {:?}",
                    self.status, new_status
                ),
            }),
        }
    }

    /// 转换为TaskSpec
    pub fn to_spec(&self) -> TaskSpec {
        self.spec.clone()
    }

    /// 获取目标代理列表
    pub fn target_agents(&self) -> Vec<String> {
        self.spec
            .target_agents()
            .map(|agents| agents.iter().map(|id| id.as_str().to_string()).collect())
            .unwrap_or_else(Vec::new)
    }
}
