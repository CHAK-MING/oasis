use std::collections::HashMap;
use std::sync::Arc;

use oasis_core::error::CoreError;
use oasis_core::types::TaskSpec;

use crate::application::ports::repositories::RolloutRepository;
use crate::domain::models::rollout::{Rollout, RolloutConfig};
use crate::application::selector::SelectorEngine;

/// 灰度发布用例
pub struct RolloutDeployUseCase {
    rollout_repo: Arc<dyn RolloutRepository>,
    selector_engine: Arc<SelectorEngine>,
}

impl RolloutDeployUseCase {
    pub fn new(
        rollout_repo: Arc<dyn RolloutRepository>,
        selector_engine: Arc<SelectorEngine>,
    ) -> Self {
        Self {
            rollout_repo,
            selector_engine,
        }
    }

    /// 更高层的创建入口：封装验证与标签合并等业务规则
    pub async fn create_rollout_from_payload(
        &self,
        mut payload: CreateRolloutPayload,
    ) -> Result<String, CoreError> {
        if payload.name.trim().is_empty() {
            return Err(CoreError::Config {
                message: "Rollout name cannot be empty".to_string(),
            });
        }
        if payload.target_selector.trim().is_empty() {
            return Err(CoreError::Config {
                message: "Target selector cannot be empty".to_string(),
            });
        }

        if !payload.labels.is_empty() {
            payload.config.labels.extend(payload.labels);
        }

        self.create_rollout(
            &payload.name,
            payload.task,
            &payload.target_selector,
            payload.config,
        )
        .await
    }

    pub async fn create_rollout(
        &self,
        name: &str,
        task: TaskSpec,
        target_selector: &str,
        config: RolloutConfig,
    ) -> Result<String, CoreError> {
        let targets: Vec<String> = self
            .selector_engine
            .execute(target_selector)
            .map_err(|e| CoreError::InvalidTask {
                reason: format!("Failed to execute selector: {}", e),
            })?
            .agent_ids
            .iter()
            .map(|id| id.as_str().to_string())
            .collect();
        let mut rollout = Rollout::new(
            uuid::Uuid::new_v4().to_string(),
            name.to_string(),
            task,
            target_selector.to_string(),
            config,
        );
        // 缓存首次解析的目标节点，避免后续重复解析
        if !targets.is_empty() {
            rollout.cached_target_agents = Some(targets);
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
        match self.rollout_repo.get(id).await {
            Ok(rollout) => Ok(Some(rollout)),
            Err(CoreError::Agent {
                agent_id: _,
                message,
            }) if message.contains("Rollout not found") => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn list_rollouts(&self) -> Result<Vec<Rollout>, CoreError> {
        self.rollout_repo.list().await
    }
}

/// 用例输入负载：从接口层组装后传入业务层
pub struct CreateRolloutPayload {
    pub name: String,
    pub task: TaskSpec,
    pub target_selector: String,
    pub config: RolloutConfig,
    pub labels: HashMap<String, String>,
}
