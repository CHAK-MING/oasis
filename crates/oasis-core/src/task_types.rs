use crate::core_types::{AgentId, BatchId, SelectorExpression, TaskId};
use chrono;
use serde::{Deserialize, Serialize};

/// 任务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Created,
    Pending,
    Running,
    Success,
    Failed,
    Timeout,
    Cancelled,
}

impl TaskState {
    /// 检查状态是否为终端状态
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Success | TaskState::Failed | TaskState::Timeout | TaskState::Cancelled
        )
    }

    /// 检查状态是否为可取消状态
    pub fn is_cancellable(&self) -> bool {
        matches!(
            self,
            TaskState::Created | TaskState::Pending | TaskState::Running
        )
    }
}

impl Default for TaskState {
    fn default() -> Self {
        TaskState::Created
    }
}

/// 任务请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRequest {
    /// 命令
    pub command: String,
    /// 参数
    pub args: Vec<String>,
    /// 选择器
    pub selector: SelectorExpression,
    /// 超时时间（秒）
    pub timeout_seconds: u32,
}

impl Default for BatchRequest {
    fn default() -> Self {
        Self {
            command: String::new(),
            args: Vec::new(),
            selector: SelectorExpression::new(""),
            timeout_seconds: 300, // 5分钟默认超时
        }
    }
}

/// 任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// 任务ID
    pub task_id: TaskId,
    /// 批次ID
    pub batch_id: BatchId,
    /// AgentId
    pub agent_id: AgentId,
    /// 命令
    pub command: String,
    /// 参数
    pub args: Vec<String>,
    /// 超时时间（秒）
    pub timeout_seconds: u32,
    /// 状态
    pub state: TaskState,
    /// 创建时间
    pub created_at: i64,
    /// 更新时间
    pub updated_at: i64,
}

impl Task {
    pub fn new(command: String, args: Vec<String>, timeout_seconds: u32) -> Self {
        Self {
            task_id: TaskId::generate(),
            batch_id: BatchId::new(""),
            agent_id: AgentId::new(""),
            created_at: chrono::Utc::now().timestamp(),
            updated_at: chrono::Utc::now().timestamp(),
            command,
            args,
            timeout_seconds,
            state: TaskState::Created,
        }
    }

    pub fn with_batch_id(mut self, batch_id: BatchId) -> Self {
        self.batch_id = batch_id;
        self
    }
    pub fn with_agent_id(mut self, agent_id: AgentId) -> Self {
        self.agent_id = agent_id;
        self
    }

    /// 转换状态
    pub fn transition_to(&mut self, new_state: TaskState) -> Result<(), String> {
        // 检查状态转换是否有效
        if !self.can_transition_to(new_state) {
            return Err(format!(
                "Invalid state transition from {:?} to {:?}",
                self.state, new_state
            ));
        }

        self.state = new_state;
        self.updated_at = chrono::Utc::now().timestamp();
        Ok(())
    }

    /// 检查是否可以转换到指定状态
    fn can_transition_to(&self, new_state: TaskState) -> bool {
        match (self.state, new_state) {
            // 创建 -> 待处理
            (TaskState::Created, TaskState::Pending) => true,
            // 待处理 -> 运行
            (TaskState::Pending, TaskState::Running) => true,
            // 运行 -> 成功/失败/超时
            (TaskState::Running, TaskState::Success | TaskState::Failed | TaskState::Timeout) => {
                true
            }
            // 任何状态 -> 取消
            (_, TaskState::Cancelled) => true,
            // 其他转换不允许
            _ => false,
        }
    }

    pub fn get_task_id(&self) -> TaskId {
        self.task_id.clone()
    }
}

/// 任务执行结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecution {
    /// 任务ID
    pub task_id: TaskId,
    /// 代理ID
    pub agent_id: AgentId,
    /// 执行状态
    pub state: TaskState,
    /// 退出码
    pub exit_code: Option<i32>,
    /// 标准输出
    pub stdout: String,
    /// 标准错误
    pub stderr: String,
    /// 开始时间
    pub started_at: i64,
    /// 完成时间
    pub finished_at: Option<i64>,
    /// 执行时长（毫秒）
    pub duration_ms: Option<f64>,
}

impl TaskExecution {
    /// 创建运行中的执行记录
    pub fn running(task_id: TaskId, agent_id: AgentId) -> Self {
        Self {
            task_id,
            agent_id,
            state: TaskState::Running,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            started_at: chrono::Utc::now().timestamp(),
            finished_at: None,
            duration_ms: None,
        }
    }

    /// 创建成功的执行记录
    pub fn success(
        task_id: TaskId,
        agent_id: AgentId,
        exit_code: i32,
        stdout: String,
        stderr: String,
        duration_ms: f64,
    ) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            task_id,
            agent_id,
            state: TaskState::Success,
            exit_code: Some(exit_code),
            stdout,
            stderr,
            started_at: now,
            finished_at: Some(now),
            duration_ms: Some(duration_ms),
        }
    }

    /// 创建失败的执行记录
    pub fn failure(
        task_id: TaskId,
        agent_id: AgentId,
        error_message: String,
        duration_ms: f64,
    ) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            task_id,
            agent_id,
            state: TaskState::Failed,
            exit_code: Some(-1),
            stdout: String::new(),
            stderr: error_message,
            started_at: now,
            finished_at: Some(now),
            duration_ms: Some(duration_ms),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Batch {
    pub batch_id: BatchId,
    pub command: String,
    pub args: Vec<String>,
    pub timeout_seconds: u32,
    pub created_at: i64,
}

impl Batch {
    pub fn new(command: String, args: Vec<String>, timeout_seconds: u32) -> Self {
        Self {
            batch_id: BatchId::generate(),
            command,
            args,
            timeout_seconds,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn with_batch_id(mut self, batch_id: BatchId) -> Self {
        self.batch_id = batch_id;
        self
    }

    pub fn get_batch_id(&self) -> BatchId {
        self.batch_id.clone()
    }
}
