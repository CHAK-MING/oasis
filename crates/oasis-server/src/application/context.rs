use anyhow::Result;
use std::sync::Arc;

use crate::application::ports::repositories::{
    AgentRepository, FileRepository, RolloutRepository, TaskRepository,
};
use crate::application::selector::SelectorEngine;

/// 应用程序上下文 - 管理所有依赖
#[derive(Clone)]
pub struct ApplicationContext {
    pub agent_repo: Arc<dyn AgentRepository>,
    pub task_repo: Arc<dyn TaskRepository>,
    pub rollout_repo: Arc<dyn RolloutRepository>,
    pub file_repo: Arc<dyn FileRepository>,
    pub selector_engine: Arc<SelectorEngine>,
}

/// 应用程序上下文构建器
pub struct ApplicationContextBuilder {
    agent_repo: Option<Arc<dyn AgentRepository>>,
    task_repo: Option<Arc<dyn TaskRepository>>,
    rollout_repo: Option<Arc<dyn RolloutRepository>>,
    file_repo: Option<Arc<dyn FileRepository>>,
    selector_engine: Option<Arc<SelectorEngine>>,
}

impl ApplicationContextBuilder {
    pub fn new() -> Self {
        Self {
            agent_repo: None,
            task_repo: None,
            rollout_repo: None,
            file_repo: None,
            selector_engine: None,
        }
    }

    pub fn with_agent_repo(mut self, repo: Arc<dyn AgentRepository>) -> Self {
        self.agent_repo = Some(repo);
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

    pub fn with_selector_engine(mut self, engine: Arc<SelectorEngine>) -> Self {
        self.selector_engine = Some(engine);
        self
    }

    pub fn build(self) -> Result<ApplicationContext> {
        Ok(ApplicationContext {
            agent_repo: self
                .agent_repo
                .ok_or_else(|| anyhow::anyhow!("AgentRepository is required"))?,
            task_repo: self
                .task_repo
                .ok_or_else(|| anyhow::anyhow!("TaskRepository is required"))?,
            rollout_repo: self
                .rollout_repo
                .ok_or_else(|| anyhow::anyhow!("RolloutRepository is required"))?,
            file_repo: self
                .file_repo
                .ok_or_else(|| anyhow::anyhow!("FileRepository is required"))?,

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
