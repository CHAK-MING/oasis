use crate::infrastructure::services::{AgentService, FileService, RolloutService, TaskService};
use std::sync::Arc;

/// 应用程序上下文 - 管理所有依赖
#[derive(Clone)]
pub struct ApplicationContext {
    pub agent_service: Arc<AgentService>,
    pub task_service: Arc<TaskService>,
    pub file_service: Arc<FileService>,
    pub rollout_service: Arc<RolloutService>,
}
