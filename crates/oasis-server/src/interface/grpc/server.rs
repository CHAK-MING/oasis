//! gRPC服务器 - 适配统一类型系统

use crate::application::context::ApplicationContext;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::instrument;

use oasis_core::proto::{
    AdvanceRolloutRequest, AdvanceRolloutResponse, CancelBatchRequest, CancelBatchResponse,
    CommitFileMsg, CreateBootstrapTokenRequest, CreateBootstrapTokenResponse, CreateRolloutRequest,
    CreateRolloutResponse, EmptyMsg, FileApplyRequestMsg, FileChunkMsg, FileChunkResponse,
    FileOperationResult, FileSpecMsg, FileUploadSession, GetBatchDetailsRequest,
    GetBatchDetailsResponse, GetFileHistoryRequest, GetFileHistoryResponse,
    GetRolloutStatusRequest, GetRolloutStatusResponse, GetTaskOutputRequest, GetTaskOutputResponse,
    ListAgentsRequest, ListAgentsResponse, ListBatchesRequest, ListBatchesResponse,
    ListRolloutsRequest, ListRolloutsResponse, RemoveAgentRequest, RemoveAgentResponse,
    RollbackFileRequest, RollbackRolloutRequest, RollbackRolloutResponse, SetInfoAgentRequest,
    SetInfoAgentResponse, SubmitBatchRequest, SubmitBatchResponse, oasis_service_server,
};

/// Oasis gRPC 服务器实现
pub struct OasisServer {
    context: Arc<ApplicationContext>,
}

impl OasisServer {
    pub fn new(context: Arc<ApplicationContext>) -> Self {
        Self { context }
    }
}

impl OasisServer {
    pub(crate) fn context(&self) -> &Arc<ApplicationContext> {
        &self.context
    }
}

#[tonic::async_trait]
impl oasis_service_server::OasisService for OasisServer {
    #[instrument(skip_all)]
    async fn submit_batch(
        &self,
        request: Request<SubmitBatchRequest>,
    ) -> std::result::Result<Response<SubmitBatchResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::submit_batch(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_batch_details(
        &self,
        request: Request<GetBatchDetailsRequest>,
    ) -> std::result::Result<Response<GetBatchDetailsResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::get_batch_details(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_task_output(
        &self,
        request: Request<GetTaskOutputRequest>,
    ) -> std::result::Result<Response<GetTaskOutputResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::get_task_output(self, request).await
    }

    #[instrument(skip_all)]
    async fn list_batches(
        &self,
        request: Request<ListBatchesRequest>,
    ) -> std::result::Result<Response<ListBatchesResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::list_batches(self, request).await
    }

    #[instrument(skip_all)]
    async fn cancel_batch(
        &self,
        request: Request<CancelBatchRequest>,
    ) -> std::result::Result<Response<CancelBatchResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::cancel_batch(self, request).await
    }

    #[instrument(skip_all)]
    async fn apply_file(
        &self,
        request: Request<FileApplyRequestMsg>,
    ) -> std::result::Result<Response<FileOperationResult>, Status> {
        crate::interface::grpc::handlers::FileHandlers::apply_file(self, request).await
    }

    #[instrument(skip_all)]
    async fn clear_files(
        &self,
        request: Request<EmptyMsg>,
    ) -> std::result::Result<Response<FileOperationResult>, Status> {
        crate::interface::grpc::handlers::FileHandlers::clear_files(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_file_history(
        &self,
        request: Request<GetFileHistoryRequest>,
    ) -> std::result::Result<Response<GetFileHistoryResponse>, Status> {
        crate::interface::grpc::handlers::FileHandlers::get_file_history(self, request).await
    }

    #[instrument(skip_all)]
    async fn rollback_file(
        &self,
        request: Request<RollbackFileRequest>,
    ) -> std::result::Result<Response<FileOperationResult>, Status> {
        crate::interface::grpc::handlers::FileHandlers::rollback_file(self, request).await
    }

    #[instrument(skip_all)]
    async fn begin_file_upload(
        &self,
        request: Request<FileSpecMsg>,
    ) -> std::result::Result<Response<FileUploadSession>, Status> {
        crate::interface::grpc::handlers::FileHandlers::begin_file_upload(self, request).await
    }

    #[instrument(skip_all)]
    async fn upload_file_chunk(
        &self,
        request: Request<FileChunkMsg>,
    ) -> std::result::Result<Response<FileChunkResponse>, Status> {
        crate::interface::grpc::handlers::FileHandlers::upload_file_chunk(self, request).await
    }

    #[instrument(skip_all)]
    async fn commit_file_upload(
        &self,
        request: Request<CommitFileMsg>,
    ) -> std::result::Result<Response<FileOperationResult>, Status> {
        crate::interface::grpc::handlers::FileHandlers::commit_file_upload(self, request).await
    }

    #[instrument(skip_all)]
    async fn list_agents(
        &self,
        request: Request<ListAgentsRequest>,
    ) -> std::result::Result<Response<ListAgentsResponse>, Status> {
        crate::interface::grpc::handlers::AgentHandlers::list_agents(self, request).await
    }

    #[instrument(skip_all)]
    async fn remove_agent(
        &self,
        request: Request<RemoveAgentRequest>,
    ) -> std::result::Result<Response<RemoveAgentResponse>, Status> {
        crate::interface::grpc::handlers::AgentHandlers::remove_agent(self, request).await
    }

    #[instrument(skip_all)]
    async fn set_info_agent(
        &self,
        request: Request<SetInfoAgentRequest>,
    ) -> std::result::Result<Response<SetInfoAgentResponse>, Status> {
        crate::interface::grpc::handlers::AgentHandlers::set_info_agent(self, request).await
    }

    #[instrument(skip_all)]
    async fn create_bootstrap_token(
        &self,
        request: Request<CreateBootstrapTokenRequest>,
    ) -> std::result::Result<Response<CreateBootstrapTokenResponse>, Status> {
        crate::interface::grpc::handlers::AgentHandlers::create_bootstrap_token(self, request).await
    }

    #[instrument(skip_all)]
    async fn create_rollout(
        &self,
        request: Request<CreateRolloutRequest>,
    ) -> std::result::Result<Response<CreateRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::create_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_rollout_status(
        &self,
        request: Request<GetRolloutStatusRequest>,
    ) -> std::result::Result<Response<GetRolloutStatusResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::get_rollout_status(self, request).await
    }

    #[instrument(skip_all)]
    async fn list_rollouts(
        &self,
        request: Request<ListRolloutsRequest>,
    ) -> std::result::Result<Response<ListRolloutsResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::list_rollouts(self, request).await
    }

    #[instrument(skip_all)]
    async fn advance_rollout(
        &self,
        request: Request<AdvanceRolloutRequest>,
    ) -> std::result::Result<Response<AdvanceRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::advance_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn rollback_rollout(
        &self,
        request: Request<RollbackRolloutRequest>,
    ) -> std::result::Result<Response<RollbackRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::rollback_rollout(self, request).await
    }
}

#[cfg(test)]
mod tests {
    use oasis_core::proto::*;

    #[test]
    fn test_submit_batch_request_structure() {
        let request = SubmitBatchRequest {
            batch_request: Some(BatchRequestMsg {
                command: "echo".to_string(),
                args: vec!["hello".to_string()],
                target: Some(SelectorExpression {
                    expression: "all".to_string(),
                }),
                timeout_seconds: 60,
            }),
        };
        assert!(request.batch_request.is_some());
        let br = request.batch_request.unwrap();
        assert_eq!(br.command, "echo");
        assert_eq!(br.timeout_seconds, 60);
    }

    #[test]
    fn test_create_bootstrap_token_request() {
        let request = CreateBootstrapTokenRequest {
            agent_id: Some(AgentId {
                value: "my-agent".to_string(),
            }),
            ttl_hours: 24,
        };
        assert_eq!(request.ttl_hours, 24);
        assert_eq!(request.agent_id.unwrap().value, "my-agent");
    }

    #[test]
    fn test_create_bootstrap_token_response_success() {
        let response = CreateBootstrapTokenResponse {
            success: true,
            token: "token-abc-123".to_string(),
            expires_at: 1234567890,
            message: String::new(),
        };
        assert!(response.success);
        assert!(!response.token.is_empty());
        assert!(response.expires_at > 0);
    }

    #[test]
    fn test_create_bootstrap_token_response_failure() {
        let response = CreateBootstrapTokenResponse {
            success: false,
            token: String::new(),
            expires_at: 0,
            message: "Failed to create token".to_string(),
        };
        assert!(!response.success);
        assert!(response.token.is_empty());
        assert!(!response.message.is_empty());
    }

    #[test]
    fn test_file_apply_request() {
        let request = FileApplyRequestMsg {
            config: Some(FileConfigMsg {
                source_path: "/etc/app.conf".to_string(),
                destination_path: "/opt/app/app.conf".to_string(),
                revision: 1,
                owner: "root".to_string(),
                mode: "0644".to_string(),
                target: Some(SelectorExpression {
                    expression: "all".to_string(),
                }),
            }),
        };
        assert!(request.config.is_some());
    }

    #[test]
    fn test_remove_agent_request() {
        let request = RemoveAgentRequest {
            agent_id: Some(AgentId {
                value: "agent-to-remove".to_string(),
            }),
        };
        assert!(request.agent_id.is_some());
    }

    #[test]
    fn test_remove_agent_response() {
        let response = RemoveAgentResponse {
            success: true,
            message: "Agent removed".to_string(),
        };
        assert!(response.success);
    }

    #[test]
    fn test_set_info_agent_request() {
        let mut info = std::collections::HashMap::new();
        info.insert("hostname".to_string(), "server1".to_string());
        info.insert("env".to_string(), "prod".to_string());

        let request = SetInfoAgentRequest {
            agent_id: Some(AgentId {
                value: "my-agent".to_string(),
            }),
            info,
        };
        assert!(request.agent_id.is_some());
        assert_eq!(request.info.len(), 2);
    }

    #[test]
    fn test_set_info_agent_response() {
        let response = SetInfoAgentResponse {
            success: true,
            message: "Info updated".to_string(),
        };
        assert!(response.success);
    }

    #[test]
    fn test_get_rollout_status_request() {
        let request = GetRolloutStatusRequest {
            rollout_id: Some(RolloutId {
                value: "rollout-abc".to_string(),
            }),
        };
        assert!(request.rollout_id.is_some());
    }

    #[test]
    fn test_advance_rollout_request() {
        let request = AdvanceRolloutRequest {
            rollout_id: Some(RolloutId {
                value: "rollout-123".to_string(),
            }),
        };
        assert!(request.rollout_id.is_some());
    }

    #[test]
    fn test_batch_id_validation() {
        let id = BatchId {
            value: "batch-123".to_string(),
        };
        assert!(id.validate().is_ok());

        let empty_id = BatchId {
            value: String::new(),
        };
        assert!(empty_id.validate().is_err());
    }

    #[test]
    fn test_agent_id_validation() {
        let id = AgentId {
            value: "agent-123".to_string(),
        };
        assert!(id.validate().is_ok());

        let empty_id = AgentId {
            value: String::new(),
        };
        assert!(empty_id.validate().is_err());
    }

    #[test]
    fn test_rollout_id_validation() {
        let id = RolloutId {
            value: "rollout-123".to_string(),
        };
        assert!(id.validate().is_ok());

        let empty_id = RolloutId {
            value: String::new(),
        };
        assert!(empty_id.validate().is_err());
    }
}
