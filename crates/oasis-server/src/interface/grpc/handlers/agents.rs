use oasis_core::proto::{
    ListAgentsRequest, ListAgentsResponse, RemoveAgentRequest, RemoveAgentResponse,
    SetInfoAgentRequest, SetInfoAgentResponse,
};
use tonic::{Request, Response, Status};

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct AgentHandlers;

impl AgentHandlers {
    pub async fn list_agents(
        srv: &OasisServer,
        request: Request<ListAgentsRequest>,
    ) -> std::result::Result<Response<ListAgentsResponse>, Status> {
        let req = request.into_inner();

        let expression = req
            .target
            .as_ref()
            .map(|t| t.expression.clone())
            .unwrap_or_else(|| "all".to_string());

        let result = srv
            .context()
            .agent_service
            .query(&expression)
            .await
            .map_err(map_core_error)?;

        let agent_ids = if req.is_online {
            result.to_online_agents()
        } else {
            result.to_all_agents()
        };

        // 从 AgentInfoMonitor 拿快照信息
        let infos = srv.context().agent_service.list_agent_infos(&agent_ids);

        // 转 proto
        let agents = infos
            .into_iter()
            .map(|info| oasis_core::proto::AgentInfoMsg::from(info))
            .collect();

        Ok(Response::new(ListAgentsResponse { agents }))
    }

    pub async fn remove_agent(
        srv: &OasisServer,
        request: Request<RemoveAgentRequest>,
    ) -> std::result::Result<Response<RemoveAgentResponse>, Status> {
        let req = request.into_inner();

        // 验证请求
        let agent_id = req
            .agent_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("agent_id is required"))?;

        if agent_id.value.is_empty() {
            return Err(Status::invalid_argument("agent_id cannot be empty"));
        }

        let agent_id = oasis_core::core_types::AgentId::from(agent_id.value.clone());

        // 调用选择器服务移除 Agent
        match srv.context().agent_service.remove_agent(&agent_id).await {
            Ok(removed) => {
                let message = if removed {
                    format!("Agent {} 已成功移除", agent_id)
                } else {
                    format!("Agent {} 不存在或已移除", agent_id)
                };

                Ok(Response::new(oasis_core::proto::RemoveAgentResponse {
                    success: removed,
                    message,
                }))
            }
            Err(e) => {
                let error_msg = format!("不能移除 Agent {}: {}", agent_id, e);
                tracing::error!("{}", error_msg);
                Err(Status::internal(error_msg))
            }
        }
    }

    pub async fn set_info_agent(
        srv: &OasisServer,
        request: Request<SetInfoAgentRequest>,
    ) -> std::result::Result<Response<SetInfoAgentResponse>, Status> {
        let req = request.into_inner();

        let agent_id = req
            .agent_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("agent_id is required"))?;

        // 调用 agent_service 的 set_info_agent 方法
        let agent_id = oasis_core::core_types::AgentId::from(agent_id.value.clone());
        match srv
            .context()
            .agent_service
            .set_info_agent(&agent_id, &req.info)
            .await
        {
            Ok(success) => Ok(Response::new(SetInfoAgentResponse {
                success,
                message: "success".to_string(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}
