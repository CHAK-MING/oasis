use std::sync::Arc;

use oasis_core::error::CoreError;
use oasis_core::types::TaskSpec;

use crate::application::ports::repositories::{NodeRepository, RolloutRepository};
use crate::domain::models::rollout::{Rollout, RolloutConfig};
use crate::domain::services::SelectorEngine;

/// 灰度发布用例
pub struct RolloutDeployUseCase {
    rollout_repo: Arc<dyn RolloutRepository>,
    node_repo: Arc<dyn NodeRepository>,
    selector_engine: Arc<dyn SelectorEngine>,
}

impl RolloutDeployUseCase {
    pub fn new(
        rollout_repo: Arc<dyn RolloutRepository>,
        node_repo: Arc<dyn NodeRepository>,
        selector_engine: Arc<dyn SelectorEngine>,
    ) -> Self {
        Self {
            rollout_repo,
            node_repo,
            selector_engine,
        }
    }

    pub async fn create_rollout(
        &self,
        name: &str,
        task: TaskSpec,
        target_selector: &str,
        config: RolloutConfig,
    ) -> Result<String, CoreError> {
        let online = self.node_repo.list_online().await?;
        let nodes_details = self.node_repo.get_nodes_batch(&online).await?;
        let nodes: Vec<_> = nodes_details
            .into_iter()
            .map(|n| n.to_attributes())
            .collect();
        let targets = self
            .selector_engine
            .resolve(target_selector, &nodes)
            .await?;
        let mut rollout = Rollout::new(
            uuid::Uuid::new_v4().to_string(),
            name.to_string(),
            task,
            target_selector.to_string(),
            config,
        );
        // 缓存首次解析的目标节点，避免后续重复解析
        if !targets.is_empty() {
            rollout.cached_target_nodes = Some(targets);
        }
        self.rollout_repo.create(rollout).await
    }

    pub async fn start_rollout(&self, id: &str) -> Result<(), CoreError> {
        let mut rollout = self.rollout_repo.get(id).await?;
        rollout.start()?;
        self.rollout_repo.update(rollout).await
    }

    pub async fn pause_rollout(&self, id: &str, reason: &str) -> Result<(), CoreError> {
        let mut rollout = self.rollout_repo.get(id).await?;
        rollout.pause(reason.to_string())?;
        self.rollout_repo.update(rollout).await
    }

    pub async fn resume_rollout(&self, id: &str) -> Result<(), CoreError> {
        let mut rollout = self.rollout_repo.get(id).await?;
        rollout.resume()?;
        self.rollout_repo.update(rollout).await
    }

    pub async fn abort_rollout(&self, id: &str, reason: &str) -> Result<(), CoreError> {
        let mut rollout = self.rollout_repo.get(id).await?;
        rollout.abort(reason.to_string())?;
        self.rollout_repo.update(rollout).await
    }

    pub async fn rollback_rollout(&self, id: &str, reason: &str) -> Result<(), CoreError> {
        let mut rollout = self.rollout_repo.get(id).await?;
        rollout.rollback(reason.to_string())?;
        self.rollout_repo.update(rollout).await
    }

    pub async fn get_rollout(&self, id: &str) -> Result<Option<Rollout>, CoreError> {
        Ok(Some(self.rollout_repo.get(id).await?))
    }

    pub async fn list_rollouts(&self) -> Result<Vec<Rollout>, CoreError> {
        self.rollout_repo.list().await
    }
}


