use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::instrument;

use oasis_core::proto::oasis_service_server;
use oasis_core::proto::{
    AbortRolloutRequest, AbortRolloutResponse, CheckAgentsRequest, CheckAgentsResponse,
    CreateRolloutRequest, CreateRolloutResponse, ExecuteTaskRequest, ExecuteTaskResponse,
    GetTaskResultRequest, GetTaskResultResponse, HealthCheckRequest, HealthCheckResponse,
    PauseRolloutRequest, PauseRolloutResponse, ResumeRolloutRequest, ResumeRolloutResponse,
    RollbackRolloutRequest, RollbackRolloutResponse, StartRolloutRequest, StartRolloutResponse,
};

use crate::application::context::ApplicationContext;
use crate::application::use_cases::commands::{
    ClearFilesUseCase, ExecuteTaskUseCase, RolloutDeployUseCase, UploadFileUseCase,
};
use crate::application::use_cases::queries::{ManageAgentConfigUseCase, ManageNodesUseCase};
// use crate::domain::models::rollout::RolloutConfig; // moved to converters module
use crate::interface::health::HealthService;

/// Oasis gRPC 服务器实现
pub struct OasisServer {
    context: Arc<ApplicationContext>,
    shutdown_token: tokio_util::sync::CancellationToken,
    online_ttl_sec: u64,
    stream_backoff: crate::config::StreamingBackoffSection,
    execute_task_use_case: Arc<ExecuteTaskUseCase>,
    manage_nodes_use_case: Arc<ManageNodesUseCase>,
    rollout_deploy_use_case: Arc<RolloutDeployUseCase>,
    upload_file_use_case: Arc<UploadFileUseCase>,
    manage_agent_config_use_case: Arc<ManageAgentConfigUseCase>,
    clear_files_use_case: Arc<ClearFilesUseCase>,
    health_service: Arc<HealthService>,
}

impl OasisServer {
    pub fn new(
        context: ApplicationContext,
        shutdown_token: CancellationToken,
        online_ttl_sec: u64,
        stream_backoff: crate::config::StreamingBackoffSection,
        health_service: Option<Arc<HealthService>>,
    ) -> Self {
        let context_clone = context.clone();
        let context_arc = Arc::new(context);
        let execute_uc = Arc::new(ExecuteTaskUseCase::new(
            context_clone.task_repo.clone(),
            context_clone.node_repo.clone(),
        ));
        Self {
            context: context_arc,
            shutdown_token,
            online_ttl_sec,
            stream_backoff,
            execute_task_use_case: execute_uc.clone(),
            manage_nodes_use_case: Arc::new(ManageNodesUseCase::new(
                context_clone.node_repo.clone(),
                context_clone.selector_engine.clone(),
            )),
            rollout_deploy_use_case: Arc::new(RolloutDeployUseCase::new(
                context_clone.rollout_repo.clone(),
                context_clone.node_repo.clone(),
                context_clone.selector_engine.clone(),
            )),
            upload_file_use_case: Arc::new(UploadFileUseCase::new(
                context_clone.file_repo.clone(),
                context_clone.task_repo.clone(),
            )),
            manage_agent_config_use_case: Arc::new(ManageAgentConfigUseCase::new(
                context_clone.node_repo.clone(),
                context_clone.agent_config_repo.clone(),
            )),
            clear_files_use_case: Arc::new(ClearFilesUseCase::new(context_clone.file_repo.clone())),
            health_service: health_service.expect("HealthService must be provided"),
        }
    }
}

// ===== Public accessors for handler modules =====
impl OasisServer {
    pub(crate) fn context(&self) -> &Arc<ApplicationContext> {
        &self.context
    }

    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    pub(crate) fn online_ttl_sec(&self) -> u64 {
        self.online_ttl_sec
    }

    pub(crate) fn stream_backoff(&self) -> &crate::config::StreamingBackoffSection {
        &self.stream_backoff
    }

    pub(crate) fn execute_task_use_case(&self) -> &Arc<ExecuteTaskUseCase> {
        &self.execute_task_use_case
    }

    pub(crate) fn manage_nodes_use_case(&self) -> &Arc<ManageNodesUseCase> {
        &self.manage_nodes_use_case
    }

    pub(crate) fn rollout_deploy_use_case(&self) -> &Arc<RolloutDeployUseCase> {
        &self.rollout_deploy_use_case
    }

    pub(crate) fn upload_file_use_case(&self) -> &Arc<UploadFileUseCase> {
        &self.upload_file_use_case
    }

    pub(crate) fn manage_agent_config_use_case(&self) -> &Arc<ManageAgentConfigUseCase> {
        &self.manage_agent_config_use_case
    }

    pub(crate) fn clear_files_use_case(&self) -> &Arc<ClearFilesUseCase> {
        &self.clear_files_use_case
    }

    pub(crate) fn health_service(&self) -> &Arc<HealthService> {
        &self.health_service
    }
}

// Converters are now provided by `crate::interface::grpc::converters` module

#[tonic::async_trait]
impl oasis_service_server::OasisService for OasisServer {
    type StreamTaskResultsStream = std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<oasis_core::proto::StreamTaskResultsResponse, Status>>
                + Send,
        >,
    >;
    #[instrument(skip_all)]
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        crate::interface::grpc::handlers::HealthHandlers::health_check(self, _request).await
    }

    #[instrument(skip_all)]
    async fn execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::execute_task(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_task_result(
        &self,
        request: Request<GetTaskResultRequest>,
    ) -> Result<Response<GetTaskResultResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::get_task_result(self, request).await
    }

    #[instrument(skip_all)]
    async fn check_agents(
        &self,
        request: Request<CheckAgentsRequest>,
    ) -> Result<Response<CheckAgentsResponse>, Status> {
        let req = request.into_inner();
        let agent_ids: Vec<String> = req.agent_ids.iter().map(|id| id.value.clone()).collect();
        let statuses = self
            .manage_nodes_use_case()
            .check_agents(agent_ids)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CheckAgentsResponse { statuses }))
    }

    // push_file removed (not in proto)

    // 应用文件到目标节点
    #[instrument(skip_all)]
    async fn apply_file(
        &self,
        request: Request<oasis_core::proto::ApplyFileRequest>,
    ) -> Result<Response<oasis_core::proto::ApplyFileResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::apply_file(self, request).await
    }

    #[instrument(skip_all)]
    async fn clear_files(
        &self,
        _request: Request<oasis_core::proto::ClearFilesRequest>,
    ) -> Result<Response<oasis_core::proto::ClearFilesResponse>, Status> {
        crate::interface::grpc::handlers::TaskHandlers::clear_files(self, _request).await
    }

    #[instrument(skip_all)]
    async fn create_rollout(
        &self,
        request: Request<CreateRolloutRequest>,
    ) -> Result<Response<CreateRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::create_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn start_rollout(
        &self,
        request: Request<StartRolloutRequest>,
    ) -> Result<Response<StartRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::start_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn pause_rollout(
        &self,
        request: Request<PauseRolloutRequest>,
    ) -> Result<Response<PauseRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::pause_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn resume_rollout(
        &self,
        request: Request<ResumeRolloutRequest>,
    ) -> Result<Response<ResumeRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::resume_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn abort_rollout(
        &self,
        request: Request<AbortRolloutRequest>,
    ) -> Result<Response<AbortRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::abort_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn rollback_rollout(
        &self,
        request: Request<RollbackRolloutRequest>,
    ) -> Result<Response<RollbackRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::rollback_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn stream_task_results(
        &self,
        request: Request<oasis_core::proto::StreamTaskResultsRequest>,
    ) -> Result<Response<Self::StreamTaskResultsStream>, Status> {
        crate::interface::grpc::handlers::StreamingHandlers::stream_task_results(self, request)
            .await
    }

    // set_node_labels removed (not in proto)

    // delete_node_labels removed (not in proto)

    #[instrument(skip_all)]
    async fn get_node_labels(
        &self,
        request: Request<oasis_core::proto::GetNodeLabelsRequest>,
    ) -> Result<Response<oasis_core::proto::GetNodeLabelsResponse>, Status> {
        crate::interface::grpc::handlers::NodeHandlers::get_node_labels(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_node_facts(
        &self,
        request: Request<oasis_core::proto::GetNodeFactsRequest>,
    ) -> Result<Response<oasis_core::proto::GetNodeFactsResponse>, Status> {
        crate::interface::grpc::handlers::NodeHandlers::get_node_facts(self, request).await
    }

    #[instrument(skip_all)]
    async fn list_nodes(
        &self,
        request: Request<oasis_core::proto::ListNodesRequest>,
    ) -> Result<Response<oasis_core::proto::ListNodesResponse>, Status> {
        crate::interface::grpc::handlers::NodeHandlers::list_nodes(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_rollout(
        &self,
        request: Request<oasis_core::proto::GetRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::GetRolloutResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::get_rollout(self, request).await
    }

    #[instrument(skip_all)]
    async fn list_rollouts(
        &self,
        request: Request<oasis_core::proto::ListRolloutsRequest>,
    ) -> Result<Response<oasis_core::proto::ListRolloutsResponse>, Status> {
        crate::interface::grpc::handlers::RolloutHandlers::list_rollouts(self, request).await
    }

    // === Agent config (stubs) ===
    #[instrument(skip_all)]
    async fn apply_agent_config(
        &self,
        request: Request<oasis_core::proto::ApplyAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ApplyAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::apply_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_agent_config(
        &self,
        request: Request<oasis_core::proto::GetAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::GetAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::get_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn set_agent_config(
        &self,
        request: Request<oasis_core::proto::SetAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::SetAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::set_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn del_agent_config(
        &self,
        request: Request<oasis_core::proto::DelAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::DelAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::del_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn list_agent_config(
        &self,
        request: Request<oasis_core::proto::ListAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ListAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::list_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn show_agent_config(
        &self,
        request: Request<oasis_core::proto::ShowAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ShowAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::show_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn backup_agent_config(
        &self,
        request: Request<oasis_core::proto::BackupAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::BackupAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::backup_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn restore_agent_config(
        &self,
        request: Request<oasis_core::proto::RestoreAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::RestoreAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::restore_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn validate_agent_config(
        &self,
        request: Request<oasis_core::proto::ValidateAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ValidateAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::validate_agent_config(self, request).await
    }

    #[instrument(skip_all)]
    async fn diff_agent_config(
        &self,
        request: Request<oasis_core::proto::DiffAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::DiffAgentConfigResponse>, Status> {
        crate::interface::grpc::handlers::ConfigHandlers::diff_agent_config(self, request).await
    }
}
