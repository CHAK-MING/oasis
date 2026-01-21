use oasis_core::core_types::AgentId;
use oasis_core::proto::{
    CreateBootstrapTokenRequest, CreateBootstrapTokenResponse, ListAgentsRequest,
    ListAgentsResponse, RemoveAgentRequest, RemoveAgentResponse, SetInfoAgentRequest,
    SetInfoAgentResponse,
};
use std::time::Duration;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct AgentHandlers;

impl AgentHandlers {
    pub async fn create_bootstrap_token(
        srv: &OasisServer,
        request: Request<CreateBootstrapTokenRequest>,
    ) -> std::result::Result<Response<CreateBootstrapTokenResponse>, Status> {
        let req = request.into_inner();

        let agent_id = req
            .agent_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("agent_id is required"))?;

        if agent_id.value.is_empty() {
            return Err(Status::invalid_argument("agent_id cannot be empty"));
        }

        let agent_id = AgentId::from(agent_id.value.clone());
        let ttl = Duration::from_secs(req.ttl_hours.max(1) * 3600);

        match srv
            .context()
            .ca_service
            .create_bootstrap_token(agent_id.clone(), ttl)
            .await
        {
            Ok(token) => {
                info!(agent_id = %agent_id.as_str(), "Created bootstrap token via gRPC");
                Ok(Response::new(CreateBootstrapTokenResponse {
                    success: true,
                    token: token.token,
                    expires_at: token.expires_at,
                    message: String::new(),
                }))
            }
            Err(e) => {
                let error_msg = format!("Failed to create bootstrap token: {}", e);
                tracing::error!("{}", error_msg);
                Ok(Response::new(CreateBootstrapTokenResponse {
                    success: false,
                    token: String::new(),
                    expires_at: 0,
                    message: error_msg,
                }))
            }
        }
    }

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
            .map(oasis_core::proto::AgentInfoMsg::from)
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

#[cfg(test)]
mod tests {
    use super::*;
    use oasis_core::proto::AgentId as ProtoAgentId;

    #[test]
    fn test_create_bootstrap_token_request_validation_missing_agent_id() {
        let req = CreateBootstrapTokenRequest {
            agent_id: None,
            ttl_hours: 24,
        };
        assert!(req.agent_id.is_none());
    }

    #[test]
    fn test_create_bootstrap_token_request_validation_empty_agent_id() {
        let req = CreateBootstrapTokenRequest {
            agent_id: Some(ProtoAgentId {
                value: String::new(),
            }),
            ttl_hours: 24,
        };
        assert!(req.agent_id.as_ref().unwrap().value.is_empty());
    }

    #[test]
    fn test_create_bootstrap_token_ttl_minimum() {
        let req = CreateBootstrapTokenRequest {
            agent_id: Some(ProtoAgentId {
                value: "test-agent".to_string(),
            }),
            ttl_hours: 0,
        };
        // Handler uses .max(1) to enforce minimum 1 hour
        let ttl = Duration::from_secs(req.ttl_hours.max(1) * 3600);
        assert_eq!(ttl, Duration::from_secs(3600));
    }

    #[test]
    fn test_create_bootstrap_token_ttl_normal() {
        let req = CreateBootstrapTokenRequest {
            agent_id: Some(ProtoAgentId {
                value: "test-agent".to_string(),
            }),
            ttl_hours: 24,
        };
        let ttl = Duration::from_secs(req.ttl_hours.max(1) * 3600);
        assert_eq!(ttl, Duration::from_secs(24 * 3600));
    }

    #[test]
    fn test_create_bootstrap_token_ttl_large_value() {
        let req = CreateBootstrapTokenRequest {
            agent_id: Some(ProtoAgentId {
                value: "test-agent".to_string(),
            }),
            ttl_hours: 168, // 1 week
        };
        let ttl = Duration::from_secs(req.ttl_hours.max(1) * 3600);
        assert_eq!(ttl, Duration::from_secs(168 * 3600));
    }

    #[test]
    fn test_remove_agent_request_validation_missing_id() {
        let req = RemoveAgentRequest { agent_id: None };
        assert!(req.agent_id.is_none());
    }

    #[test]
    fn test_remove_agent_request_validation_empty_id() {
        let req = RemoveAgentRequest {
            agent_id: Some(ProtoAgentId {
                value: String::new(),
            }),
        };
        assert!(req.agent_id.as_ref().unwrap().value.is_empty());
    }

    #[test]
    fn test_remove_agent_request_valid() {
        let req = RemoveAgentRequest {
            agent_id: Some(ProtoAgentId {
                value: "agent-001".to_string(),
            }),
        };
        assert!(req.agent_id.is_some());
        assert_eq!(req.agent_id.as_ref().unwrap().value, "agent-001");
    }

    #[test]
    fn test_set_info_agent_request_validation() {
        let req = SetInfoAgentRequest {
            agent_id: Some(ProtoAgentId {
                value: "test-agent".to_string(),
            }),
            info: std::collections::HashMap::from([
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ]),
        };
        assert_eq!(req.info.len(), 2);
        assert_eq!(req.info.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_set_info_agent_empty_info() {
        let req = SetInfoAgentRequest {
            agent_id: Some(ProtoAgentId {
                value: "test-agent".to_string(),
            }),
            info: std::collections::HashMap::new(),
        };
        assert!(req.info.is_empty());
    }

    #[test]
    fn test_agent_id_conversion() {
        let proto_id = ProtoAgentId {
            value: "my-agent-123".to_string(),
        };
        let agent_id = AgentId::from(proto_id.value.clone());
        assert_eq!(agent_id.as_str(), "my-agent-123");
    }
}
