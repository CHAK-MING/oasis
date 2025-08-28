use tonic::{Request, Response, Status};

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct AgentHandlers;

impl AgentHandlers {
    pub async fn get_agent_labels(
        srv: &OasisServer,
        request: Request<oasis_core::proto::GetAgentLabelsRequest>,
    ) -> Result<Response<oasis_core::proto::GetAgentLabelsResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none() || req.agent_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let labels = srv
            .manage_agents_use_case()
            .get_agent_labels(agent_id)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::GetAgentLabelsResponse {
            labels,
        }))
    }

    pub async fn get_agent_facts(
        srv: &OasisServer,
        request: Request<oasis_core::proto::GetAgentFactsRequest>,
    ) -> Result<Response<oasis_core::proto::GetAgentFactsResponse>, Status> {
        let req = request.into_inner();
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let facts = srv
            .manage_agents_use_case()
            .get_agent_facts(agent_id)
            .await
            .map_err(map_core_error)?;
        let facts_proto = facts.map(|af| oasis_core::proto::AgentFacts::from(&af));
        Ok(Response::new(oasis_core::proto::GetAgentFactsResponse {
            facts: facts_proto,
        }))
    }

    pub async fn list_agents(
        srv: &OasisServer,
        request: Request<oasis_core::proto::ListAgentsRequest>,
    ) -> Result<Response<oasis_core::proto::ListAgentsResponse>, Status> {
        let req = request.into_inner();
        let target = req.target.as_ref().map(|s| s.as_str());

        let agents = srv
            .manage_agents_use_case()
            .list_agents(target)
            .await
            .map_err(map_core_error)?;

        tracing::info!(
            target = ?target,
            count = agents.len(),
            "list_agents resolved agents"
        );

        let ttl_sec = srv.online_ttl_sec();
        let agent_infos: Vec<oasis_core::proto::AgentInfo> = agents
            .into_iter()
            .map(|agent| {
                let facts_proto = Some(oasis_core::proto::AgentFacts::from(&agent.facts));
                oasis_core::proto::AgentInfo {
                    agent_id: Some(oasis_core::proto::AgentId {
                        value: agent.id.to_string(),
                    }),
                    is_online: agent.is_online(ttl_sec),
                    facts: facts_proto,
                    labels: agent.labels.clone(),
                    groups: agent.groups,
                }
            })
            .collect();

        Ok(Response::new(oasis_core::proto::ListAgentsResponse {
            agents: agent_infos,
        }))
    }
}
