//! gRPC服务器 - 适配统一类型系统

use crate::application::context::ApplicationContext;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::instrument;

use oasis_core::proto::{
    AdvanceRolloutRequest, AdvanceRolloutResponse, CancelBatchRequest, CancelBatchResponse,
    CommitFileMsg, CreateRolloutRequest, CreateRolloutResponse, EmptyMsg, FileApplyRequestMsg,
    FileChunkMsg, FileChunkResponse, FileOperationResult, FileSpecMsg, FileUploadSession,
    GetBatchDetailsRequest, GetBatchDetailsResponse, GetFileHistoryRequest, GetFileHistoryResponse,
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
