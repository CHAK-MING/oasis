use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AgentStatus {
    Initializing,
    Running,
    Draining, // 正在处理剩余任务，不再接受新任务
    Shutdown,
}
