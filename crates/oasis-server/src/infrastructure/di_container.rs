use anyhow::Result;
use std::sync::Arc;

use crate::application::context::{ApplicationContext, ApplicationContextBuilder};
use crate::application::ports::repositories::{
    AgentRepository, FileRepository, RolloutRepository, TaskRepository,
};
use oasis_core::rate_limit::RateLimiterCollection;

/// 基础设施依赖注入容器 - 负责创建具体实现
pub struct InfrastructureDiContainer {
    jetstream: async_nats::jetstream::Context,
    limiters: std::sync::Arc<RateLimiterCollection>,
    /// 共享的选择器索引实例，确保 SelectorEngine 与 IndexUpdater 使用同一份索引
    shared_selector_index: std::sync::Arc<crate::application::selector::InvertedIndex>,
}

impl InfrastructureDiContainer {
    /// 创建依赖注入容器
    pub fn new(jetstream: async_nats::jetstream::Context) -> Self {
        Self {
            jetstream,
            limiters: std::sync::Arc::new(RateLimiterCollection::default()),
            shared_selector_index: std::sync::Arc::new(
                crate::application::selector::InvertedIndex::new(),
            ),
        }
    }

    /// 创建完整的应用程序上下文，包含所有具体实现
    pub fn create_application_context(&self) -> Result<ApplicationContext> {
        let agent_repo = self.create_agent_repository();
        let task_repo = self.create_task_repository();
        let rollout_repo = self.create_rollout_repository();
        let file_repo = self.create_file_repository();
        let selector_engine = self.create_selector_engine();

        ApplicationContextBuilder::new()
            .with_agent_repo(agent_repo)
            .with_task_repo(task_repo)
            .with_rollout_repo(rollout_repo)
            .with_file_repo(file_repo)
            .with_selector_engine(selector_engine)
            .build()
    }

    /// 创建 Agent 仓储实现
    fn create_agent_repository(&self) -> Arc<dyn AgentRepository> {
        Arc::new(
            crate::infrastructure::persistence::agent_repository::NatsAgentRepository::new(
                self.jetstream.clone(),
            ),
        ) as Arc<dyn AgentRepository>
    }

    /// 创建任务仓储实现
    fn create_task_repository(&self) -> Arc<dyn TaskRepository> {
        Arc::new(crate::infrastructure::persistence::NatsTaskRepository::new(
            self.jetstream.clone(),
            self.limiters.clone(),
        )) as Arc<dyn TaskRepository>
    }

    /// 创建发布仓储实现
    fn create_rollout_repository(&self) -> Arc<dyn RolloutRepository> {
        Arc::new(
            crate::infrastructure::persistence::NatsRolloutRepository::new(self.jetstream.clone()),
        ) as Arc<dyn RolloutRepository>
    }

    /// 创建文件仓储实现
    fn create_file_repository(&self) -> Arc<dyn FileRepository> {
        Arc::new(crate::infrastructure::persistence::NatsFileRepository::new(
            self.jetstream.clone(),
        )) as Arc<dyn FileRepository>
    }

    /// 创建选择器引擎
    fn create_selector_engine(&self) -> Arc<crate::application::selector::SelectorEngine> {
        Arc::new(crate::application::selector::SelectorEngine::new(
            self.shared_selector_index.clone(),
        ))
    }

    /// 获取共享的索引实例（用于 IndexUpdaterService）
    pub fn get_shared_index(&self) -> Arc<crate::application::selector::InvertedIndex> {
        self.shared_selector_index.clone()
    }
}
