use oasis_core::types::{AgentId, TaskId, TaskSpec};
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
    pub task_id: TaskId,
    pub agent_id: AgentId,
    pub status: TaskStatus,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub timestamp: i64,
}

impl TaskResult {
    // 如需可添加其他方法
}

/// 任务实体 - 业务逻辑封装
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
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
        let id = spec.id.clone();

        Self {
            id,
            spec,
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            version: 1,
        }
    }

    /// 转换为TaskSpec
    pub fn to_spec(&self) -> TaskSpec {
        self.spec.clone()
    }

    /// 获取目标代理列表
    pub fn target_agents(&self) -> Vec<AgentId> {
        self.spec
            .target_agents()
            .map(|agents| agents.to_vec())
            .unwrap_or_default()
    }
}
