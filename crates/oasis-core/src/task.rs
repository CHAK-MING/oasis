use crate::type_defs::{AgentId, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 任务目标枚举 - 消除 target_agents 和 selector 的歧义
/// 明确指定任务应该运行的位置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskTarget {
    /// 指定具体的 Agent 列表
    Agents(Vec<AgentId>),
    /// 使用 CEL 选择器表达式动态选择 Agent
    Selector(String),
    /// 运行在所有在线 Agent 上
    AllAgents,
}

impl TaskTarget {
    /// 便捷构造函数：创建单个 Agent 目标
    pub fn single_agent(agent_id: impl Into<AgentId>) -> Self {
        TaskTarget::Agents(vec![agent_id.into()])
    }

    /// 便捷构造函数：创建 CEL 选择器目标
    pub fn selector(expression: impl Into<String>) -> Self {
        TaskTarget::Selector(expression.into())
    }

    /// 检查是否为空目标
    pub fn is_empty(&self) -> bool {
        match self {
            TaskTarget::Agents(agents) => agents.is_empty(),
            TaskTarget::Selector(expr) => expr.is_empty(),
            TaskTarget::AllAgents => false,
        }
    }
}

/// 任务规格 - 纯数据传输对象，不可变
/// 包含任务执行所需的所有信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: TaskId,
    pub command: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    /// 任务目标：明确指定任务应在哪些节点上执行
    pub target: TaskTarget,
    pub timeout_seconds: u32,
}

impl TaskSpec {
    /// 便捷构造函数：创建针对特定 Agent 的任务
    pub fn for_agents(
        id: impl Into<TaskId>,
        command: impl Into<String>,
        agents: Vec<AgentId>,
    ) -> Self {
        Self {
            id: id.into(),
            command: command.into(),
            args: Vec::new(),
            env: HashMap::new(),
            target: TaskTarget::Agents(agents),
            timeout_seconds: 300, // 5分钟默认超时
        }
    }

    /// 便捷构造函数：创建使用选择器的任务
    pub fn with_selector(
        id: impl Into<TaskId>,
        command: impl Into<String>,
        selector: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            command: command.into(),
            args: Vec::new(),
            env: HashMap::new(),
            target: TaskTarget::Selector(selector.into()),
            timeout_seconds: 300,
        }
    }

    /// 便捷构造函数：创建面向所有 Agent 的任务
    pub fn for_all_agents(id: impl Into<TaskId>, command: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            command: command.into(),
            args: Vec::new(),
            env: HashMap::new(),
            target: TaskTarget::AllAgents,
            timeout_seconds: 300,
        }
    }

    /// 可链式方法：设置参数
    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    /// 可链式方法：设置环境变量
    pub fn with_env(mut self, env: HashMap<String, String>) -> Self {
        self.env = env;
        self
    }

    /// 可链式方法：设置超时
    pub fn with_timeout(mut self, timeout_seconds: u32) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    /// 向后兼容：获取目标 Agent 列表（仅适用于 Agents 目标）
    pub fn target_agents(&self) -> Option<&[AgentId]> {
        match &self.target {
            TaskTarget::Agents(agents) => Some(agents),
            _ => None,
        }
    }

    /// 向后兼容：获取选择器表达式（仅适用于 Selector 目标）
    pub fn selector(&self) -> Option<&str> {
        match &self.target {
            TaskTarget::Selector(expr) => Some(expr),
            _ => None,
        }
    }
}

/// 任务执行结果 - 纯数据传输对象，不可变
/// 包含任务执行完成后的所有输出信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecution {
    pub task_id: TaskId,
    pub agent_id: AgentId,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub timestamp: i64,
    pub duration_ms: u64,
}
