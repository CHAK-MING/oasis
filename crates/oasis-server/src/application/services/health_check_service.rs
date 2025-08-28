use crate::application::ports::repositories::AgentRepository;
use crate::interface::health::HealthCheckService;
use oasis_core::error::CoreError;
use oasis_core::proto::AgentInfo;
use std::sync::Arc;

/// 健康检查服务实现
pub struct AgentHealthCheckService {
    agent_repo: Arc<dyn AgentRepository>,
}

impl AgentHealthCheckService {
    pub fn new(agent_repo: Arc<dyn AgentRepository>) -> Self {
        Self { agent_repo }
    }
}

#[async_trait::async_trait]
impl HealthCheckService for AgentHealthCheckService {
    async fn get_online_agents(&self) -> Result<Vec<AgentInfo>, CoreError> {
        let online_ids = self.agent_repo.list_online().await?;
        let agents = self.agent_repo.get_agents_batch(&online_ids).await?;

        let agent_infos: Vec<AgentInfo> = agents
            .into_iter()
            .map(|agent| AgentInfo {
                agent_id: Some(oasis_core::proto::AgentId {
                    value: agent.id.to_string(),
                }),
                is_online: agent.is_online(30),
                facts: Some(oasis_core::proto::AgentFacts::from(&agent.facts)),
                labels: agent.labels.clone(),
                groups: agent.groups.clone(),
            })
            .collect();

        Ok(agent_infos)
    }
}
