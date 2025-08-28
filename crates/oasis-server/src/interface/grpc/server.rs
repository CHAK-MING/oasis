use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::application::context::ApplicationContext;
use crate::application::use_cases::commands::{
    ClearFilesUseCase, ExecuteTaskUseCase, RolloutDeployUseCase, UploadFileUseCase,
};
use crate::application::use_cases::queries::ManageAgentsUseCase;
use crate::interface::health::HealthService;
use oasis_core::proto::{
    AbortRolloutRequest, AbortRolloutResponse, CheckAgentsRequest, CheckAgentsResponse,
    CreateRolloutRequest, CreateRolloutResponse, ExecuteTaskRequest, ExecuteTaskResponse,
    GetTaskResultRequest, GetTaskResultResponse, HealthCheckRequest, HealthCheckResponse,
    PauseRolloutRequest, PauseRolloutResponse, ResumeRolloutRequest, ResumeRolloutResponse,
    RollbackRolloutRequest, RollbackRolloutResponse, StartRolloutRequest, StartRolloutResponse,
    oasis_service_server,
};

#[derive(Clone)]
pub struct StreamingBackoffSection {
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub max_retries: u32,
}

/// Oasis gRPC 服务器实现
pub struct OasisServer {
    context: Arc<ApplicationContext>,
    shutdown_token: tokio_util::sync::CancellationToken,
    online_ttl_sec: u64,
    stream_backoff: StreamingBackoffSection,
    execute_task_use_case: Arc<ExecuteTaskUseCase>,
    manage_agents_use_case: Arc<ManageAgentsUseCase>,
    rollout_deploy_use_case: Arc<RolloutDeployUseCase>,
    upload_file_use_case: Arc<UploadFileUseCase>,
    clear_files_use_case: Arc<ClearFilesUseCase>,
    health_service: Arc<HealthService>,
}

impl OasisServer {
    pub fn new(
        context: ApplicationContext,
        shutdown_token: CancellationToken,
        online_ttl_sec: u64,
        stream_backoff: StreamingBackoffSection,
        health_service: Option<Arc<HealthService>>,
    ) -> Self {
        let context_clone = context.clone();
        let context_arc = Arc::new(context);
        let execute_uc = Arc::new(ExecuteTaskUseCase::new(
            context_clone.task_repo.clone(),
            context_clone.agent_repo.clone(),
        ));
        // 创建 ManageAgentsUseCase
        let manage_agents_use_case = ManageAgentsUseCase::new(
            context_clone.agent_repo.clone(),
            context_clone.selector_engine.clone(),
        );

        Self {
            context: context_arc,
            shutdown_token,
            online_ttl_sec,
            stream_backoff,
            execute_task_use_case: execute_uc.clone(),
            manage_agents_use_case: Arc::new(manage_agents_use_case),
            rollout_deploy_use_case: Arc::new(RolloutDeployUseCase::new(
                context_clone.rollout_repo.clone(),
                context_clone.selector_engine.clone(),
            )),
            upload_file_use_case: Arc::new(UploadFileUseCase::new(
                context_clone.file_repo.clone(),
                context_clone.task_repo.clone(),
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

    pub(crate) fn stream_backoff(&self) -> &StreamingBackoffSection {
        &self.stream_backoff
    }

    pub(crate) fn execute_task_use_case(&self) -> &Arc<ExecuteTaskUseCase> {
        &self.execute_task_use_case
    }

    pub(crate) fn manage_agents_use_case(&self) -> &Arc<ManageAgentsUseCase> {
        &self.manage_agents_use_case
    }

    pub(crate) fn rollout_deploy_use_case(&self) -> &Arc<RolloutDeployUseCase> {
        &self.rollout_deploy_use_case
    }

    pub(crate) fn upload_file_use_case(&self) -> &Arc<UploadFileUseCase> {
        &self.upload_file_use_case
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
            .manage_agents_use_case()
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
    async fn get_agent_labels(
        &self,
        request: Request<oasis_core::proto::GetAgentLabelsRequest>,
    ) -> Result<Response<oasis_core::proto::GetAgentLabelsResponse>, Status> {
        crate::interface::grpc::handlers::AgentHandlers::get_agent_labels(self, request).await
    }

    #[instrument(skip_all)]
    async fn get_agent_facts(
        &self,
        request: Request<oasis_core::proto::GetAgentFactsRequest>,
    ) -> Result<Response<oasis_core::proto::GetAgentFactsResponse>, Status> {
        crate::interface::grpc::handlers::AgentHandlers::get_agent_facts(self, request).await
    }

    #[instrument(skip_all)]
    async fn list_agents(
        &self,
        request: Request<oasis_core::proto::ListAgentsRequest>,
    ) -> Result<Response<oasis_core::proto::ListAgentsResponse>, Status> {
        crate::interface::grpc::handlers::AgentHandlers::list_agents(self, request).await
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
}
