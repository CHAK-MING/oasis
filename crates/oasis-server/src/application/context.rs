use anyhow::Result;
use std::sync::Arc;

use crate::application::ports::repositories::{
    AgentConfigRepository, FileRepository, NodeRepository, RolloutRepository, TaskRepository,
};
use crate::domain::services::SelectorEngine;

/// 应用程序上下文 - 管理所有依赖（仅持有抽象）
#[derive(Clone)]
pub struct ApplicationContext {
    pub node_repo: Arc<dyn NodeRepository>,
    pub task_repo: Arc<dyn TaskRepository>,
    pub rollout_repo: Arc<dyn RolloutRepository>,
    pub file_repo: Arc<dyn FileRepository>,
    pub agent_config_repo: Arc<dyn AgentConfigRepository>,
    pub selector_engine: Arc<dyn SelectorEngine>,
}

/// 应用程序上下文构建器（仅接受 trait 对象）
pub struct ApplicationContextBuilder {
    node_repo: Option<Arc<dyn NodeRepository>>,
    task_repo: Option<Arc<dyn TaskRepository>>,
    rollout_repo: Option<Arc<dyn RolloutRepository>>,
    file_repo: Option<Arc<dyn FileRepository>>,
    agent_config_repo: Option<Arc<dyn AgentConfigRepository>>,
    selector_engine: Option<Arc<dyn SelectorEngine>>,
}

impl ApplicationContextBuilder {
    pub fn new() -> Self {
        Self {
            node_repo: None,
            task_repo: None,
            rollout_repo: None,
            file_repo: None,
            agent_config_repo: None,
            selector_engine: None,
        }
    }

    pub fn with_node_repo(mut self, repo: Arc<dyn NodeRepository>) -> Self {
        self.node_repo = Some(repo);
        self
    }

    pub fn with_task_repo(mut self, repo: Arc<dyn TaskRepository>) -> Self {
        self.task_repo = Some(repo);
        self
    }

    pub fn with_rollout_repo(mut self, repo: Arc<dyn RolloutRepository>) -> Self {
        self.rollout_repo = Some(repo);
        self
    }

    pub fn with_file_repo(mut self, repo: Arc<dyn FileRepository>) -> Self {
        self.file_repo = Some(repo);
        self
    }

    pub fn with_agent_config_repo(mut self, repo: Arc<dyn AgentConfigRepository>) -> Self {
        self.agent_config_repo = Some(repo);
        self
    }

    pub fn with_selector_engine(mut self, engine: Arc<dyn SelectorEngine>) -> Self {
        self.selector_engine = Some(engine);
        self
    }

    pub fn build(self) -> Result<ApplicationContext> {
        Ok(ApplicationContext {
            node_repo: self
                .node_repo
                .ok_or_else(|| anyhow::anyhow!("NodeRepository is required"))?,
            task_repo: self
                .task_repo
                .ok_or_else(|| anyhow::anyhow!("TaskRepository is required"))?,
            rollout_repo: self
                .rollout_repo
                .ok_or_else(|| anyhow::anyhow!("RolloutRepository is required"))?,
            file_repo: self
                .file_repo
                .ok_or_else(|| anyhow::anyhow!("FileRepository is required"))?,
            agent_config_repo: self
                .agent_config_repo
                .ok_or_else(|| anyhow::anyhow!("AgentConfigRepository is required"))?,
            selector_engine: self
                .selector_engine
                .ok_or_else(|| anyhow::anyhow!("SelectorEngine is required"))?,
        })
    }
}

impl Default for ApplicationContextBuilder {
    fn default() -> Self {
        Self::new()
    }
}
