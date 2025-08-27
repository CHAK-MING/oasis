use anyhow::Result;
use std::sync::Arc;

use crate::application::context::{ApplicationContext, ApplicationContextBuilder};
use crate::application::ports::repositories::{
    FileRepository, NodeRepository, RolloutRepository, TaskRepository,
};
use crate::domain::services::SelectorEngine;
use oasis_core::rate_limit::RateLimiterCollection;

/// 基础设施依赖注入容器 - 负责创建具体实现
pub struct InfrastructureDiContainer {
    jetstream: async_nats::jetstream::Context,
    limiters: std::sync::Arc<RateLimiterCollection>,
}

impl InfrastructureDiContainer {
    /// 创建依赖注入容器
    pub fn new(jetstream: async_nats::jetstream::Context) -> Self {
        Self {
            jetstream,
            limiters: std::sync::Arc::new(RateLimiterCollection::default()),
        }
    }

    /// 创建完整的应用程序上下文，包含所有具体实现
    pub fn create_application_context(&self) -> Result<ApplicationContext> {
        let node_repo = self.create_node_repository();
        let task_repo = self.create_task_repository();
        let rollout_repo = self.create_rollout_repository();
        let file_repo = self.create_file_repository();
        let selector_engine = self.create_selector_engine();

        ApplicationContextBuilder::new()
            .with_node_repo(node_repo)
            .with_task_repo(task_repo)
            .with_rollout_repo(rollout_repo)
            .with_file_repo(file_repo)
            .with_selector_engine(selector_engine)
            .build()
    }

    /// 创建节点仓储实现
    fn create_node_repository(&self) -> Arc<dyn NodeRepository> {
        Arc::new(crate::infrastructure::persistence::NatsNodeRepository::new(
            self.jetstream.clone(),
        )) as Arc<dyn NodeRepository>
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

    /// 创建选择器引擎实现
    fn create_selector_engine(&self) -> Arc<dyn SelectorEngine> {
        Arc::new(crate::domain::services::selector_engine::CelSelectorEngine::new())
            as Arc<dyn SelectorEngine>
    }
}
