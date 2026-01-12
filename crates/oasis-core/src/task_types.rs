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

#[cfg(test)]
mod tests {
    use super::*;

    mod task_state_tests {
        use super::*;

        #[test]
        fn test_is_terminal_success() {
            assert!(TaskState::Success.is_terminal());
        }

        #[test]
        fn test_is_terminal_failed() {
            assert!(TaskState::Failed.is_terminal());
        }

        #[test]
        fn test_is_terminal_timeout() {
            assert!(TaskState::Timeout.is_terminal());
        }

        #[test]
        fn test_is_terminal_cancelled() {
            assert!(TaskState::Cancelled.is_terminal());
        }

        #[test]
        fn test_is_not_terminal_created() {
            assert!(!TaskState::Created.is_terminal());
        }

        #[test]
        fn test_is_not_terminal_pending() {
            assert!(!TaskState::Pending.is_terminal());
        }

        #[test]
        fn test_is_not_terminal_running() {
            assert!(!TaskState::Running.is_terminal());
        }

        #[test]
        fn test_is_cancellable_created() {
            assert!(TaskState::Created.is_cancellable());
        }

        #[test]
        fn test_is_cancellable_pending() {
            assert!(TaskState::Pending.is_cancellable());
        }

        #[test]
        fn test_is_cancellable_running() {
            assert!(TaskState::Running.is_cancellable());
        }

        #[test]
        fn test_is_not_cancellable_terminal_states() {
            assert!(!TaskState::Success.is_cancellable());
            assert!(!TaskState::Failed.is_cancellable());
            assert!(!TaskState::Timeout.is_cancellable());
            assert!(!TaskState::Cancelled.is_cancellable());
        }

        #[test]
        fn test_default() {
            assert_eq!(TaskState::default(), TaskState::Created);
        }
    }

    mod task_tests {
        use super::*;

        fn create_test_task() -> Task {
            Task::new("echo".to_string(), vec!["hello".to_string()], 30)
        }

        #[test]
        fn test_new_task_has_generated_id() {
            let task = create_test_task();
            assert!(!task.task_id.as_str().is_empty());
        }

        #[test]
        fn test_new_task_initial_state() {
            let task = create_test_task();
            assert_eq!(task.state, TaskState::Created);
        }

        #[test]
        fn test_new_task_command_and_args() {
            let task = Task::new(
                "systemctl".to_string(),
                vec!["status".to_string(), "nginx".to_string()],
                60,
            );
            assert_eq!(task.command, "systemctl");
            assert_eq!(task.args, vec!["status", "nginx"]);
            assert_eq!(task.timeout_seconds, 60);
        }

        #[test]
        fn test_with_batch_id() {
            let batch_id = BatchId::new("batch-123");
            let task = create_test_task().with_batch_id(batch_id.clone());
            assert_eq!(task.batch_id, batch_id);
        }

        #[test]
        fn test_with_agent_id() {
            let agent_id = AgentId::new("agent-456");
            let task = create_test_task().with_agent_id(agent_id.clone());
            assert_eq!(task.agent_id, agent_id);
        }

        #[test]
        fn test_valid_transition_created_to_pending() {
            let mut task = create_test_task();
            assert!(task.transition_to(TaskState::Pending).is_ok());
            assert_eq!(task.state, TaskState::Pending);
        }

        #[test]
        fn test_valid_transition_pending_to_running() {
            let mut task = create_test_task();
            task.transition_to(TaskState::Pending).unwrap();
            assert!(task.transition_to(TaskState::Running).is_ok());
            assert_eq!(task.state, TaskState::Running);
        }

        #[test]
        fn test_valid_transition_running_to_success() {
            let mut task = create_test_task();
            task.transition_to(TaskState::Pending).unwrap();
            task.transition_to(TaskState::Running).unwrap();
            assert!(task.transition_to(TaskState::Success).is_ok());
            assert_eq!(task.state, TaskState::Success);
        }

        #[test]
        fn test_valid_transition_running_to_failed() {
            let mut task = create_test_task();
            task.transition_to(TaskState::Pending).unwrap();
            task.transition_to(TaskState::Running).unwrap();
            assert!(task.transition_to(TaskState::Failed).is_ok());
        }

        #[test]
        fn test_valid_transition_running_to_timeout() {
            let mut task = create_test_task();
            task.transition_to(TaskState::Pending).unwrap();
            task.transition_to(TaskState::Running).unwrap();
            assert!(task.transition_to(TaskState::Timeout).is_ok());
        }

        #[test]
        fn test_any_state_can_be_cancelled() {
            for initial in [TaskState::Created, TaskState::Pending, TaskState::Running] {
                let mut task = create_test_task();
                if initial != TaskState::Created {
                    task.state = initial;
                }
                assert!(task.transition_to(TaskState::Cancelled).is_ok());
            }
        }

        #[test]
        fn test_invalid_transition_created_to_running() {
            let mut task = create_test_task();
            assert!(task.transition_to(TaskState::Running).is_err());
        }

        #[test]
        fn test_invalid_transition_created_to_success() {
            let mut task = create_test_task();
            assert!(task.transition_to(TaskState::Success).is_err());
        }

        #[test]
        fn test_invalid_transition_pending_to_success() {
            let mut task = create_test_task();
            task.transition_to(TaskState::Pending).unwrap();
            assert!(task.transition_to(TaskState::Success).is_err());
        }

        #[test]
        fn test_get_task_id() {
            let task = create_test_task();
            let id = task.get_task_id();
            assert_eq!(id, task.task_id);
        }

        #[test]
        fn test_updated_at_changes_on_transition() {
            let mut task = create_test_task();
            let original_updated = task.updated_at;
            std::thread::sleep(std::time::Duration::from_millis(10));
            task.transition_to(TaskState::Pending).unwrap();
            assert!(task.updated_at >= original_updated);
        }
    }

    mod task_execution_tests {
        use super::*;

        #[test]
        fn test_running_execution() {
            let exec = TaskExecution::running(TaskId::new("task-1"), AgentId::new("agent-1"));
            assert_eq!(exec.state, TaskState::Running);
            assert!(exec.exit_code.is_none());
            assert!(exec.finished_at.is_none());
        }

        #[test]
        fn test_success_execution() {
            let exec = TaskExecution::success(
                TaskId::new("task-1"),
                AgentId::new("agent-1"),
                0,
                "output".to_string(),
                "".to_string(),
                100.0,
            );
            assert_eq!(exec.state, TaskState::Success);
            assert_eq!(exec.exit_code, Some(0));
            assert_eq!(exec.stdout, "output");
            assert!(exec.stderr.is_empty());
            assert_eq!(exec.duration_ms, Some(100.0));
            assert!(exec.finished_at.is_some());
        }

        #[test]
        fn test_failure_execution() {
            let exec = TaskExecution::failure(
                TaskId::new("task-1"),
                AgentId::new("agent-1"),
                "command not found".to_string(),
                50.0,
            );
            assert_eq!(exec.state, TaskState::Failed);
            assert_eq!(exec.exit_code, Some(-1));
            assert!(exec.stdout.is_empty());
            assert_eq!(exec.stderr, "command not found");
        }
    }

    mod batch_request_tests {
        use super::*;

        #[test]
        fn test_default() {
            let req = BatchRequest::default();
            assert!(req.command.is_empty());
            assert!(req.args.is_empty());
            assert_eq!(req.timeout_seconds, 300);
        }
    }

    mod batch_tests {
        use super::*;

        #[test]
        fn test_new_batch() {
            let batch = Batch::new("ls".to_string(), vec!["-la".to_string()], 60);
            assert_eq!(batch.command, "ls");
            assert_eq!(batch.args, vec!["-la"]);
            assert_eq!(batch.timeout_seconds, 60);
            assert!(!batch.batch_id.as_str().is_empty());
        }

        #[test]
        fn test_with_batch_id() {
            let custom_id = BatchId::new("custom-batch");
            let batch = Batch::new("cmd".to_string(), vec![], 30).with_batch_id(custom_id.clone());
            assert_eq!(batch.batch_id, custom_id);
        }

        #[test]
        fn test_get_batch_id() {
            let batch = Batch::new("cmd".to_string(), vec![], 30);
            let id = batch.get_batch_id();
            assert_eq!(id, batch.batch_id);
        }
    }
}
