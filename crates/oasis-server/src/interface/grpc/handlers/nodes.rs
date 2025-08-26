use tonic::{Request, Response, Status};

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct NodeHandlers;

impl NodeHandlers {
    pub async fn get_node_labels(
        srv: &OasisServer,
        request: Request<oasis_core::proto::GetNodeLabelsRequest>,
    ) -> Result<Response<oasis_core::proto::GetNodeLabelsResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none() || req.agent_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let labels = srv
            .manage_nodes_use_case()
            .get_node_labels(agent_id)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::GetNodeLabelsResponse {
            labels,
        }))
    }

    pub async fn get_node_facts(
        srv: &OasisServer,
        request: Request<oasis_core::proto::GetNodeFactsRequest>,
    ) -> Result<Response<oasis_core::proto::GetNodeFactsResponse>, Status> {
        let req = request.into_inner();
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let facts = srv
            .manage_nodes_use_case()
            .get_node_facts(agent_id)
            .await
            .map_err(map_core_error)?;
        let facts_proto = facts.map(|f| {
            let af = f.facts;
            oasis_core::proto::AgentFacts::from(&af)
        });
        Ok(Response::new(oasis_core::proto::GetNodeFactsResponse { facts: facts_proto }))
    }

    pub async fn list_nodes(
        srv: &OasisServer,
        request: Request<oasis_core::proto::ListNodesRequest>,
    ) -> Result<Response<oasis_core::proto::ListNodesResponse>, Status> {
        let req = request.into_inner();
        let selector = req.selector.as_ref().map(|s| s.as_str());
        let nodes = srv
            .manage_nodes_use_case()
            .list_nodes(selector)
            .await
            .map_err(map_core_error)?;

        let ttl_sec = srv.online_ttl_sec();
        let node_infos: Vec<oasis_core::proto::NodeInfo> = nodes
            .into_iter()
            .map(|node| {
                let facts_proto = Some(oasis_core::proto::AgentFacts::from(&node.facts.facts));
                oasis_core::proto::NodeInfo {
                agent_id: Some(oasis_core::proto::AgentId {
                    value: node.id.to_string(),
                }),
                is_online: node.is_online(ttl_sec),
                facts: facts_proto,
                labels: node.labels.labels.clone(),
            }
            })
            .collect();

        Ok(Response::new(oasis_core::proto::ListNodesResponse {
            nodes: node_infos,
        }))
    }
}
