use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::instrument;

use oasis_core::proto::oasis_service_server;
use oasis_core::proto::{
    AbortRolloutRequest, AbortRolloutResponse, CheckAgentsRequest, CheckAgentsResponse,
    CreateRolloutRequest, CreateRolloutResponse, ExecuteTaskRequest, ExecuteTaskResponse,
    GetNodeFactsResponse, GetTaskResultRequest, GetTaskResultResponse, HealthCheckRequest,
    HealthCheckResponse, PauseRolloutRequest, PauseRolloutResponse, ResolveSelectorRequest,
    ResolveSelectorResponse, ResumeRolloutRequest, ResumeRolloutResponse, RollbackRolloutRequest,
    RollbackRolloutResponse, StartRolloutRequest, StartRolloutResponse,
};
use oasis_core::types::TaskSpec;

use crate::application::context::ApplicationContext;
use crate::application::use_cases::commands::{
    ClearFilesUseCase, ExecuteTaskUseCase, RolloutDeployUseCase, UploadFileUseCase,
};
use crate::application::use_cases::queries::{
    ManageAgentConfigUseCase, ManageNodesUseCase, StreamTaskResultsUseCase,
};
use crate::domain::models::rollout::RolloutConfig;
use crate::interface::grpc::errors::map_core_error;
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

// ===== Converters between domain and proto messages =====
fn from_proto_task_spec(msg: &oasis_core::proto::TaskSpecMsg) -> TaskSpec {
    let id: oasis_core::types::TaskId = if msg.id.is_empty() {
        uuid::Uuid::new_v4().to_string().into()
    } else {
        msg.id.clone().into()
    };
    let mut spec = if !msg.selector.is_empty() {
        TaskSpec::with_selector(id, msg.command.clone(), msg.selector.clone())
    } else {
        let agents = msg
            .target_agents
            .iter()
            .cloned()
            .map(|s| s.into())
            .collect();
        TaskSpec::for_agents(id, msg.command.clone(), agents)
    };
    spec = spec
        .with_args(msg.args.clone())
        .with_env(msg.env.clone())
        .with_timeout(msg.timeout_seconds);
    spec
}

fn to_proto_task_spec(task: &TaskSpec) -> oasis_core::proto::TaskSpecMsg {
    let (agents, selector) = match task.target {
        oasis_core::task::TaskTarget::Agents(ref v) => (
            v.iter().map(|a| a.as_str().to_string()).collect(),
            String::new(),
        ),
        oasis_core::task::TaskTarget::Selector(ref s) => (Vec::new(), s.clone()),
        oasis_core::task::TaskTarget::AllAgents => (Vec::new(), String::from("true")),
    };
    oasis_core::proto::TaskSpecMsg {
        id: task.id.as_str().to_string(),
        command: task.command.clone(),
        args: task.args.clone(),
        env: task.env.clone(),
        target_agents: agents,
        selector,
        timeout_seconds: task.timeout_seconds,
    }
}

fn from_proto_batch_size(
    bs: &oasis_core::proto::BatchSizeMsg,
) -> crate::domain::models::rollout::BatchSize {
    match &bs.kind {
        Some(oasis_core::proto::batch_size_msg::Kind::Percentage(p)) => {
            crate::domain::models::rollout::BatchSize::Percentage(*p)
        }
        Some(oasis_core::proto::batch_size_msg::Kind::Count(c)) => {
            crate::domain::models::rollout::BatchSize::Count(*c as usize)
        }
        None => crate::domain::models::rollout::BatchSize::Percentage(10.0),
    }
}

fn to_proto_batch_size(
    bs: &crate::domain::models::rollout::BatchSize,
) -> oasis_core::proto::BatchSizeMsg {
    match bs {
        crate::domain::models::rollout::BatchSize::Percentage(p) => {
            oasis_core::proto::BatchSizeMsg {
                kind: Some(oasis_core::proto::batch_size_msg::Kind::Percentage(*p)),
            }
        }
        crate::domain::models::rollout::BatchSize::Count(c) => oasis_core::proto::BatchSizeMsg {
            kind: Some(oasis_core::proto::batch_size_msg::Kind::Count(*c as u32)),
        },
    }
}

fn from_proto_rollout_config(msg: &oasis_core::proto::RolloutConfigMsg) -> RolloutConfig {
    let strategy = match &msg.strategy {
        Some(oasis_core::proto::rollout_config_msg::Strategy::Canary(s)) => {
            crate::domain::models::rollout::RolloutStrategy::Canary {
                percentage: s.percentage,
                observation_duration_secs: s.observation_duration_secs as u64,
            }
        }
        Some(oasis_core::proto::rollout_config_msg::Strategy::Rolling(s)) => {
            crate::domain::models::rollout::RolloutStrategy::Rolling {
                batch_size: from_proto_batch_size(&s.batch_size.clone().unwrap_or(
                    oasis_core::proto::BatchSizeMsg {
                        kind: Some(oasis_core::proto::batch_size_msg::Kind::Percentage(10.0)),
                    },
                )),
                batch_delay_secs: s.batch_delay_secs as u64,
                max_failures: s.max_failures as usize,
            }
        }
        Some(oasis_core::proto::rollout_config_msg::Strategy::BlueGreen(s)) => {
            crate::domain::models::rollout::RolloutStrategy::BlueGreen {
                switch_percentage: s.switch_percentage,
                warmup_secs: s.warmup_secs as u64,
            }
        }
        None => crate::domain::models::rollout::RolloutStrategy::Rolling {
            batch_size: crate::domain::models::rollout::BatchSize::Percentage(10.0),
            batch_delay_secs: 30,
            max_failures: 3,
        },
    };

    RolloutConfig {
        strategy,
        timeout_seconds: msg.timeout_seconds,
        auto_advance: msg.auto_advance,
        health_check: if msg.health_check.is_empty() {
            None
        } else {
            Some(msg.health_check.clone())
        },
        labels: msg.labels.clone(),
    }
}

fn to_proto_rollout_config(cfg: &RolloutConfig) -> oasis_core::proto::RolloutConfigMsg {
    let strategy = match &cfg.strategy {
        crate::domain::models::rollout::RolloutStrategy::Canary {
            percentage,
            observation_duration_secs,
        } => Some(oasis_core::proto::rollout_config_msg::Strategy::Canary(
            oasis_core::proto::CanaryStrategyMsg {
                percentage: *percentage,
                observation_duration_secs: *observation_duration_secs,
            },
        )),
        crate::domain::models::rollout::RolloutStrategy::Rolling {
            batch_size,
            batch_delay_secs,
            max_failures,
        } => Some(oasis_core::proto::rollout_config_msg::Strategy::Rolling(
            oasis_core::proto::RollingStrategyMsg {
                batch_size: Some(to_proto_batch_size(batch_size)),
                batch_delay_secs: *batch_delay_secs,
                max_failures: *max_failures as u32,
            },
        )),
        crate::domain::models::rollout::RolloutStrategy::BlueGreen {
            switch_percentage,
            warmup_secs,
        } => Some(oasis_core::proto::rollout_config_msg::Strategy::BlueGreen(
            oasis_core::proto::BlueGreenStrategyMsg {
                switch_percentage: *switch_percentage,
                warmup_secs: *warmup_secs,
            },
        )),
    };

    oasis_core::proto::RolloutConfigMsg {
        strategy,
        timeout_seconds: cfg.timeout_seconds,
        auto_advance: cfg.auto_advance,
        health_check: cfg.health_check.clone().unwrap_or_default(),
        labels: cfg.labels.clone(),
    }
}

fn to_proto_progress(
    p: &crate::domain::models::rollout::RolloutProgress,
) -> oasis_core::proto::RolloutProgressMsg {
    oasis_core::proto::RolloutProgressMsg {
        total_nodes: p.total_nodes as u32,
        processed_nodes: p.processed_nodes as u32,
        successful_nodes: p.successful_nodes as u32,
        failed_nodes: p.failed_nodes as u32,
        completion_rate: p.completion_rate,
        current_batch: p.current_batch.unwrap_or(0) as u32,
        total_batches: p.total_batches as u32,
    }
}

fn to_proto_batch_result(
    b: &crate::domain::models::rollout::BatchResult,
) -> oasis_core::proto::BatchResultMsg {
    oasis_core::proto::BatchResultMsg {
        batch_index: b.batch_index as u32,
        node_count: b.node_count as u32,
        successful_count: b.successful_count as u32,
        failed_count: b.failed_count as u32,
        duration_secs: b.duration_secs,
        completed_at: b.completed_at,
    }
}

fn to_proto_state(
    r: &crate::domain::models::rollout::Rollout,
) -> (
    oasis_core::proto::RolloutStateEnum,
    oasis_core::proto::RolloutStateDataMsg,
) {
    use crate::domain::models::rollout::RolloutState as S;
    match &r.state {
        S::Created => (
            oasis_core::proto::RolloutStateEnum::RollCreated,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: 0,
                batch_completed_at: 0,
                reason: String::new(),
                error: String::new(),
            },
        ),
        S::RunningBatch { current_batch } => (
            oasis_core::proto::RolloutStateEnum::RollRunningBatch,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: *current_batch as u32,
                batch_completed_at: 0,
                reason: String::new(),
                error: String::new(),
            },
        ),
        S::WaitingForNextBatch { batch_completed_at } => (
            oasis_core::proto::RolloutStateEnum::RollWaitingNext,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: *batch_completed_at,
                reason: String::new(),
                error: String::new(),
            },
        ),
        S::Paused { reason } => (
            oasis_core::proto::RolloutStateEnum::RollPaused,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: reason.clone(),
                error: String::new(),
            },
        ),
        S::Succeeded => (
            oasis_core::proto::RolloutStateEnum::RollSucceeded,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: String::new(),
                error: String::new(),
            },
        ),
        S::Failed { error } => (
            oasis_core::proto::RolloutStateEnum::RollFailed,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: String::new(),
                error: error.clone(),
            },
        ),
        S::Aborted { reason } => (
            oasis_core::proto::RolloutStateEnum::RollAborted,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: reason.clone(),
                error: String::new(),
            },
        ),
        S::RollingBack { reason } => (
            oasis_core::proto::RolloutStateEnum::RollRollingBack,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: reason.clone(),
                error: String::new(),
            },
        ),
    }
}

fn to_proto_rollout(r: &crate::domain::models::rollout::Rollout) -> oasis_core::proto::RolloutMsg {
    let (state, state_data) = to_proto_state(r);
    oasis_core::proto::RolloutMsg {
        id: r.id.clone(),
        name: r.name.clone(),
        task: Some(to_proto_task_spec(&r.task)),
        target_selector: r.target_selector.clone(),
        config: Some(to_proto_rollout_config(&r.config)),
        state: state as i32,
        state_data: Some(state_data),
        progress: Some(to_proto_progress(&r.progress)),
        batch_results: r.batch_results.iter().map(to_proto_batch_result).collect(),
        processed_nodes: r.processed_nodes.iter().cloned().collect(),
        current_batch_tasks: r.current_batch_tasks.clone(),
        current_batch_started_at: r.current_batch_started_at.unwrap_or(0),
        cached_target_nodes: r.cached_target_nodes.clone().unwrap_or_default(),
        created_at: r.created_at,
        updated_at: r.updated_at,
        version: r.version,
    }
}

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
        // 使用新的健康检查服务
        let is_healthy = self.health_service.is_healthy().await;
        let nats_healthy = self.health_service.is_nats_healthy().await;
        let agents_healthy = self.health_service.is_agents_healthy().await;
        let online_count = self.health_service.get_online_agents_count().await;

        let message = if is_healthy {
            format!(
                "healthy - NATS: {}, Agents: {} ({} online)",
                if nats_healthy { "OK" } else { "FAIL" },
                if agents_healthy { "OK" } else { "FAIL" },
                online_count
            )
        } else {
            format!(
                "unhealthy - NATS: {}, Agents: {} ({} online)",
                if nats_healthy { "OK" } else { "FAIL" },
                if agents_healthy { "OK" } else { "FAIL" },
                online_count
            )
        };

        Ok(Response::new(HealthCheckResponse {
            ok: is_healthy,
            message,
        }))
    }

    #[instrument(skip_all)]
    async fn execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        let req = request.into_inner();

        // 输入验证
        if req.command.trim().is_empty() {
            return Err(Status::invalid_argument("Command cannot be empty"));
        }
        if req.targets.is_empty() {
            return Err(Status::invalid_argument("Targets cannot be empty"));
        }
        if req.timeout_seconds <= 0 {
            return Err(Status::invalid_argument("Timeout must be greater than 0"));
        }

        // 调用 Application 层的用例
        let task_id = self
            .execute_task_use_case
            .execute(
                req.targets,
                req.command,
                req.args,
                req.timeout_seconds as u32,
                Some(req.env),
            )
            .await
            .map_err(map_core_error)?;

        // 记录 task_id 到追踪 span 中
        tracing::Span::current().record("task_id", &task_id);

        Ok(Response::new(ExecuteTaskResponse { task_id }))
    }

    #[instrument(skip_all)]
    async fn get_task_result(
        &self,
        request: Request<GetTaskResultRequest>,
    ) -> Result<Response<GetTaskResultResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("task_id", &req.task_id);

        let task_id = req.task_id.clone();
        let agent_id = if req.agent_id.is_empty() {
            None
        } else {
            Some(req.agent_id.clone())
        };
        let wait_timeout_ms = req.wait_timeout_ms;

        let get_result_closure = || async {
            self.execute_task_use_case
                .get_task_result(&task_id, agent_id.as_deref())
                .await
                .map_err(map_core_error)
        };

        let to_response =
            |task_result: crate::domain::models::task::TaskResult| -> GetTaskResultResponse {
                GetTaskResultResponse {
                    found: true,
                    timestamp: task_result.timestamp,
                    duration_ms: task_result.duration_ms,
                    task_id: task_result.task_id.clone(),
                    agent_id: task_result.agent_id.clone(),
                    exit_code: match task_result.status {
                        crate::domain::models::task::TaskStatus::Completed { exit_code } => {
                            exit_code
                        }
                        _ => -1,
                    },
                    stdout: task_result.stdout,
                    stderr: task_result.stderr,
                }
            };

        let not_found_response = || -> GetTaskResultResponse {
            GetTaskResultResponse {
                found: false,
                task_id: task_id.clone(),
                agent_id: req.agent_id.clone(),
                ..Default::default()
            }
        };

        // 如果不等待，则执行单次查询
        if wait_timeout_ms <= 0 {
            if let Some(task_result) = get_result_closure().await? {
                return Ok(Response::new(to_response(task_result)));
            }
            return Ok(Response::new(not_found_response()));
        }

        // 如果需要等待，则在超时时间内轮询
        let mut poll_interval = tokio::time::interval(std::time::Duration::from_millis(500));
        let start_time = std::time::Instant::now();
        let timeout_duration = std::time::Duration::from_millis(wait_timeout_ms as u64);

        loop {
            if start_time.elapsed() >= timeout_duration {
                return Ok(Response::new(not_found_response()));
            }

            poll_interval.tick().await;

            if let Some(task_result) = get_result_closure().await? {
                return Ok(Response::new(to_response(task_result)));
            }
        }
    }

    #[instrument(skip_all)]
    async fn check_agents(
        &self,
        request: Request<CheckAgentsRequest>,
    ) -> Result<Response<CheckAgentsResponse>, Status> {
        let req = request.into_inner();

        // 调用 Application 层的用例检查代理状态
        let statuses = self
            .manage_nodes_use_case
            .check_agents(req.agent_ids)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(CheckAgentsResponse { statuses }))
    }

    #[instrument(skip_all)]
    // list_online_agents removed (not in proto)
    #[instrument(skip_all)]
    async fn resolve_selector(
        &self,
        request: Request<ResolveSelectorRequest>,
    ) -> Result<Response<ResolveSelectorResponse>, Status> {
        let req = request.into_inner();

        // 调用 Application 层的用例解析选择器
        let agent_ids = self
            .manage_nodes_use_case
            .resolve_selector(&req.selector_expression)
            .await
            .map_err(map_core_error)?;
        let total_nodes = agent_ids.len() as i32;
        let matched_nodes = agent_ids.len() as i32;

        Ok(Response::new(ResolveSelectorResponse {
            agent_ids,
            total_nodes,
            matched_nodes,
            error_message: "".to_string(),
        }))
    }

    // push_file removed (not in proto)

    // 应用文件到目标节点
    #[instrument(skip_all)]
    async fn apply_file(
        &self,
        request: Request<oasis_core::proto::ApplyFileRequest>,
    ) -> Result<Response<oasis_core::proto::ApplyFileResponse>, Status> {
        let req = request.into_inner();

        // 输入验证
        if req.object_name.trim().is_empty() {
            return Err(Status::invalid_argument("Object name cannot be empty"));
        }
        if req.destination_path.trim().is_empty() {
            return Err(Status::invalid_argument("Destination path cannot be empty"));
        }
        if req.target_selector.trim().is_empty() {
            return Err(Status::invalid_argument("Target selector cannot be empty"));
        }
        if req.file_data.is_empty() {
            return Err(Status::invalid_argument("File data cannot be empty"));
        }

        // 0. 先上传文件到对象存储
        self.upload_file_use_case
            .push_file(&req.object_name, req.file_data)
            .await
            .map_err(map_core_error)?;

        // 1. 解析选择器获取目标节点
        let response = self
            .manage_nodes_use_case
            .resolve_selector(&req.target_selector)
            .await
            .map_err(map_core_error)?;

        if response.is_empty() {
            return Ok(Response::new(oasis_core::proto::ApplyFileResponse {
                success: false,
                message: format!("No nodes matched selector: {}", req.target_selector),
                applied_nodes: Vec::new(),
                failed_nodes: Vec::new(),
            }));
        }

        // 2. 调用 Application 层的用例应用文件
        let mut config = crate::domain::models::file::FileApplyConfig::new(
            req.object_name,
            req.destination_path,
        );
        if !req.expected_sha256.is_empty() {
            config = config.with_sha256(req.expected_sha256);
        }
        if !req.owner.is_empty() {
            config = config.with_owner(req.owner);
        }
        if !req.mode.is_empty() {
            config = config.with_mode(req.mode);
        }
        if req.atomic {
            config = config.with_atomic(true);
        }

        let task_id = self
            .upload_file_use_case
            .apply_file(config, response.clone())
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(oasis_core::proto::ApplyFileResponse {
            success: true,
            message: format!("File apply task created: {}", task_id),
            applied_nodes: response,
            failed_nodes: Vec::new(),
        }))
    }

    #[instrument(skip_all)]
    async fn clear_files(
        &self,
        _request: Request<oasis_core::proto::ClearFilesRequest>,
    ) -> Result<Response<oasis_core::proto::ClearFilesResponse>, Status> {
        let deleted = self
            .clear_files_use_case
            .clear_all()
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::ClearFilesResponse {
            deleted_count: deleted,
        }))
    }

    #[instrument(skip_all)]
    async fn create_rollout(
        &self,
        request: Request<CreateRolloutRequest>,
    ) -> Result<Response<CreateRolloutResponse>, Status> {
        let req = request.into_inner();

        // 输入验证
        if req.name.trim().is_empty() {
            return Err(Status::invalid_argument("Rollout name cannot be empty"));
        }
        if req.target_selector.trim().is_empty() {
            return Err(Status::invalid_argument("Target selector cannot be empty"));
        }
        // 使用结构化 proto 构造 TaskSpec 与 RolloutConfig
        let task = match req.task.as_ref() {
            Some(t) => from_proto_task_spec(t),
            None => TaskSpec::for_agents(
                uuid::Uuid::new_v4().to_string(),
                "echo".to_string(),
                Vec::new(),
            )
            .with_args(vec!["hello".to_string()])
            .with_env(HashMap::new())
            .with_timeout(300),
        };

        let mut config = match req.config.as_ref() {
            Some(c) => from_proto_rollout_config(c),
            None => RolloutConfig {
                strategy: crate::domain::models::rollout::RolloutStrategy::Rolling {
                    batch_size: crate::domain::models::rollout::BatchSize::Percentage(10.0),
                    batch_delay_secs: 30,
                    max_failures: 3,
                },
                timeout_seconds: 300,
                auto_advance: true,
                health_check: None,
                labels: HashMap::new(),
            },
        };

        // 合并请求中的 labels 到配置中，避免丢弃
        if !req.labels.is_empty() {
            config.labels.extend(req.labels);
        }

        let rollout_id = self
            .rollout_deploy_use_case
            .create_rollout(&req.name, task, &req.target_selector, config)
            .await
            .map_err(map_core_error)?;

        // 记录 rollout_id 到追踪 span 中
        tracing::Span::current().record("rollout_id", &rollout_id);

        Ok(Response::new(CreateRolloutResponse { rollout_id }))
    }

    #[instrument(skip_all)]
    async fn start_rollout(
        &self,
        request: Request<StartRolloutRequest>,
    ) -> Result<Response<StartRolloutResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("rollout_id", &req.rollout_id);

        // 调用 Application 层的用例启动灰度发布
        self.rollout_deploy_use_case
            .start_rollout(&req.rollout_id)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(StartRolloutResponse {}))
    }

    #[instrument(skip_all)]
    async fn pause_rollout(
        &self,
        request: Request<PauseRolloutRequest>,
    ) -> Result<Response<PauseRolloutResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("rollout_id", &req.rollout_id);

        // 调用 Application 层的用例暂停灰度发布
        self.rollout_deploy_use_case
            .pause_rollout(&req.rollout_id, &req.reason)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(PauseRolloutResponse {}))
    }

    #[instrument(skip_all)]
    async fn resume_rollout(
        &self,
        request: Request<ResumeRolloutRequest>,
    ) -> Result<Response<ResumeRolloutResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("rollout_id", &req.rollout_id);

        // 调用 Application 层的用例恢复灰度发布
        self.rollout_deploy_use_case
            .resume_rollout(&req.rollout_id)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(ResumeRolloutResponse {}))
    }

    #[instrument(skip_all)]
    async fn abort_rollout(
        &self,
        request: Request<AbortRolloutRequest>,
    ) -> Result<Response<AbortRolloutResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("rollout_id", &req.rollout_id);

        // 调用 Application 层的用例中止灰度发布
        self.rollout_deploy_use_case
            .abort_rollout(&req.rollout_id, &req.reason)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(AbortRolloutResponse {}))
    }

    #[instrument(skip_all)]
    async fn rollback_rollout(
        &self,
        request: Request<RollbackRolloutRequest>,
    ) -> Result<Response<RollbackRolloutResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("rollout_id", &req.rollout_id);

        // 调用 Application 层的用例回滚灰度发布
        self.rollout_deploy_use_case
            .rollback_rollout(&req.rollout_id, &req.reason)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(RollbackRolloutResponse {}))
    }

    #[instrument(skip_all)]
    async fn stream_task_results(
        &self,
        request: Request<oasis_core::proto::StreamTaskResultsRequest>,
    ) -> Result<Response<Self::StreamTaskResultsStream>, Status> {
        let req = request.into_inner();

        // 1. 请求验证
        if req.task_id.trim().is_empty() {
            return Err(Status::invalid_argument("Task ID cannot be empty"));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let task_id = req.task_id.clone();
        let shutdown_token = self.shutdown_token.clone();
        let stream_use_case = Arc::new(StreamTaskResultsUseCase::new(
            self.context.task_repo.clone(),
        ));
        let backoff = self.stream_backoff.clone();

        // 3. 启动后台任务来流式获取结果
        tokio::spawn(async move {
            let mut retry_count = 0;
            let max_retries = backoff.max_retries;
            let initial_poll_interval =
                std::time::Duration::from_millis(backoff.initial_poll_interval_ms);
            let max_poll_interval = std::time::Duration::from_millis(backoff.max_poll_interval_ms);

            // 创建结果消费者（使用预先克隆的 use case，避免借用 self）
            let mut consumer = match stream_use_case.create_consumer(&task_id).await {
                Ok(consumer) => consumer,
                Err(e) => {
                    let _ = tx.send(Err(map_core_error(e))).await;
                    return;
                }
            };

            let mut poll_interval = initial_poll_interval;
            let mut consecutive_empty_results = 0u32;
            let mut last_result_timestamp = 0i64;

            loop {
                if tx.is_closed() {
                    tracing::debug!(task_id = %task_id, "Client disconnected, stopping stream");
                    break;
                }

                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        tracing::debug!(task_id = %task_id, "Stream task results received shutdown signal");
                        break;
                    }
                    result = consumer.next_result() => {
                        match result {
                            Ok(Some(task_result)) => {
                                // 重置轮询间隔和重试计数
                                poll_interval = initial_poll_interval;
                                consecutive_empty_results = 0;
                                retry_count = 0;

                                // 检查是否是新的结果
                                if task_result.timestamp > last_result_timestamp {
                                    last_result_timestamp = task_result.timestamp;

                                    let agent_id = task_result.agent_id.clone();
                                    let resp = oasis_core::proto::StreamTaskResultsResponse {
                                        task_id: task_result.task_id,
                                        agent_id: task_result.agent_id,
                                        stdout: task_result.stdout,
                                        stderr: task_result.stderr,
                                        exit_code: match task_result.status {
                                            crate::domain::models::task::TaskStatus::Completed { exit_code } => exit_code,
                                            crate::domain::models::task::TaskStatus::Failed { .. } => -1,
                                            crate::domain::models::task::TaskStatus::Cancelled { .. } => -2,
                                            crate::domain::models::task::TaskStatus::Timeout => -3,
                                            _ => 0, // 运行中或其他状态
                                        },
                                        timestamp: task_result.timestamp,
                                        duration_ms: task_result.duration_ms,
                                    };

                                    if tx.send(Ok(resp)).await.is_err() {
                                        tracing::debug!(task_id = %task_id, "Failed to send result to client");
                                        break;
                                    }

                                    let exit_code = match task_result.status {
                                        crate::domain::models::task::TaskStatus::Completed { exit_code } => exit_code,
                                        crate::domain::models::task::TaskStatus::Failed { .. } => -1,
                                        crate::domain::models::task::TaskStatus::Cancelled { .. } => -2,
                                        crate::domain::models::task::TaskStatus::Timeout => -3,
                                        _ => 0, // 运行中或其他状态
                                    };

                                    tracing::debug!(
                                        task_id = %task_id,
                                        agent_id = %agent_id,
                                        exit_code = exit_code,
                                        "Streamed task result"
                                    );
                                }
                            }
                            Ok(None) => {
                                // 自适应退避：当没有结果时增加轮询间隔
                                consecutive_empty_results += 1;
                                if consecutive_empty_results > backoff.empty_results_threshold {
                                    poll_interval = std::cmp::min(poll_interval * 2, max_poll_interval);
                                }

                                // 保守策略：继续轮询直到有结果或客户端/服务端终止

                                tokio::time::sleep(poll_interval).await;
                            }
                            Err(e) => {
                                retry_count += 1;
                                if retry_count >= max_retries {
                                    let _ = tx.send(Err(map_core_error(e))).await;
                                    break;
                                }

                                tracing::warn!(
                                    task_id = %task_id,
                                    retry_count = retry_count,
                                    error = %e,
                                    "Error fetching result, retrying"
                                );

                                // 指数退避重试
                                tokio::time::sleep(std::time::Duration::from_millis(100 * (2_u64.pow(retry_count)))).await;
                            }
                        }
                    }
                }
            }

            tracing::debug!(task_id = %task_id, "Stream task results completed");
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    // set_node_labels removed (not in proto)

    // delete_node_labels removed (not in proto)

    #[instrument(skip_all)]
    async fn get_node_labels(
        &self,
        request: Request<oasis_core::proto::GetNodeLabelsRequest>,
    ) -> Result<Response<oasis_core::proto::GetNodeLabelsResponse>, Status> {
        let req = request.into_inner();

        if req.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }

        // 调用 Application 层的用例获取节点标签
        let labels = self
            .manage_nodes_use_case
            .get_node_labels(&req.agent_id)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(oasis_core::proto::GetNodeLabelsResponse {
            labels,
        }))
    }

    #[instrument(skip_all)]
    async fn get_node_facts(
        &self,
        request: Request<oasis_core::proto::GetNodeFactsRequest>,
    ) -> Result<Response<oasis_core::proto::GetNodeFactsResponse>, Status> {
        let req = request.into_inner();

        // 调用 Application 层的用例获取节点事实
        let facts = self
            .manage_nodes_use_case
            .get_node_facts(&req.agent_id)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(GetNodeFactsResponse {
            facts_json: facts.map(|nf| nf.facts).unwrap_or_else(|| "{}".to_string()),
        }))
    }

    #[instrument(skip_all)]
    async fn list_nodes(
        &self,
        request: Request<oasis_core::proto::ListNodesRequest>,
    ) -> Result<Response<oasis_core::proto::ListNodesResponse>, Status> {
        let req = request.into_inner();

        // 调用 Application 层的用例列出节点
        let nodes = self
            .manage_nodes_use_case
            .list_nodes(if req.selector.trim().is_empty() {
                None
            } else {
                Some(&req.selector)
            })
            .await
            .map_err(map_core_error)?;

        // 转换为 NodeInfo 格式
        let ttl_sec = self.online_ttl_sec;
        let node_infos: Vec<oasis_core::proto::NodeInfo> = nodes
            .into_iter()
            .map(|node| oasis_core::proto::NodeInfo {
                agent_id: node.id.clone(),
                is_online: node.is_online(ttl_sec),
                facts_json: node.facts.facts.clone(),
                labels: node.labels.labels.clone(),
            })
            .collect();

        Ok(Response::new(oasis_core::proto::ListNodesResponse {
            nodes: node_infos,
        }))
    }

    #[instrument(skip_all)]
    async fn get_rollout(
        &self,
        request: Request<oasis_core::proto::GetRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::GetRolloutResponse>, Status> {
        let req = request.into_inner();

        // 调用 Application 层的用例获取 rollout
        let rollout = self
            .rollout_deploy_use_case
            .get_rollout(&req.rollout_id)
            .await
            .map_err(map_core_error)?;

        match rollout {
            Some(rollout) => Ok(Response::new(oasis_core::proto::GetRolloutResponse {
                rollout: Some(to_proto_rollout(&rollout)),
            })),
            None => Err(Status::not_found("Rollout not found")),
        }
    }

    #[instrument(skip_all)]
    async fn list_rollouts(
        &self,
        request: Request<oasis_core::proto::ListRolloutsRequest>,
    ) -> Result<Response<oasis_core::proto::ListRolloutsResponse>, Status> {
        let req = request.into_inner();

        let _status_filter: Option<String> = if req.status_filter.is_empty() {
            None
        } else {
            Some(req.status_filter)
        };

        let _limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };

        // 调用 Application 层的用例列出所有 rollouts
        let rollouts = self
            .rollout_deploy_use_case
            .list_rollouts()
            .await
            .map_err(map_core_error)?;

        let rollouts_proto: Vec<oasis_core::proto::RolloutMsg> =
            rollouts.iter().map(to_proto_rollout).collect();

        Ok(Response::new(oasis_core::proto::ListRolloutsResponse {
            rollouts: rollouts_proto,
        }))
    }

    // === Agent config (stubs) ===
    #[instrument(skip_all)]
    async fn apply_agent_config(
        &self,
        request: Request<oasis_core::proto::ApplyAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ApplyAgentConfigResponse>, Status> {
        let req = request.into_inner();

        // 调用 Application 层的用例处理 TOML 配置应用
        let applied = self
            .manage_agent_config_use_case
            .apply_toml_config(&req.selector, &req.config_data)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(oasis_core::proto::ApplyAgentConfigResponse {
            applied_agents: applied,
        }))
    }

    #[instrument(skip_all)]
    async fn get_agent_config(
        &self,
        request: Request<oasis_core::proto::GetAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::GetAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() || req.key.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID and key cannot be empty"));
        }

        // 调用 Application 层的用例获取配置
        let value = self
            .manage_agent_config_use_case
            .get(&req.agent_id, &req.key)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(oasis_core::proto::GetAgentConfigResponse {
            found: value.is_some(),
            value: value.unwrap_or_default(),
        }))
    }

    #[instrument(skip_all)]
    async fn set_agent_config(
        &self,
        request: Request<oasis_core::proto::SetAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::SetAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() || req.key.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID and key cannot be empty"));
        }

        self.manage_agent_config_use_case
            .set(&req.agent_id, &req.key, &req.value)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(oasis_core::proto::SetAgentConfigResponse {}))
    }

    #[instrument(skip_all)]
    async fn del_agent_config(
        &self,
        request: Request<oasis_core::proto::DelAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::DelAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() || req.key.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID and key cannot be empty"));
        }

        self.manage_agent_config_use_case
            .del(&req.agent_id, &req.key)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(oasis_core::proto::DelAgentConfigResponse {}))
    }

    #[instrument(skip_all)]
    async fn list_agent_config(
        &self,
        request: Request<oasis_core::proto::ListAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ListAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }

        let keys = self
            .manage_agent_config_use_case
            .list_keys(
                &req.agent_id,
                if req.prefix.is_empty() {
                    None
                } else {
                    Some(&req.prefix)
                },
            )
            .await
            .map_err(map_core_error)?;

        let total_count = keys.len() as u64;
        Ok(Response::new(oasis_core::proto::ListAgentConfigResponse {
            keys,
            total_count,
        }))
    }

    #[instrument(skip_all)]
    async fn show_agent_config(
        &self,
        request: Request<oasis_core::proto::ShowAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ShowAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }

        let summary = self
            .manage_agent_config_use_case
            .get_summary(&req.agent_id)
            .await
            .map_err(map_core_error)?;

        let total_keys = summary
            .get("total_keys")
            .unwrap_or(&"0".to_string())
            .parse()
            .unwrap_or(0);
        Ok(Response::new(oasis_core::proto::ShowAgentConfigResponse {
            summary,
            total_keys,
        }))
    }

    #[instrument(skip_all)]
    async fn backup_agent_config(
        &self,
        request: Request<oasis_core::proto::BackupAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::BackupAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }

        let (data, format) = self
            .manage_agent_config_use_case
            .backup_config(&req.agent_id, &req.output_format)
            .await
            .map_err(map_core_error)?;

        // 计算键数量
        let key_count = match format.as_str() {
            "toml" => {
                let toml_str = std::str::from_utf8(&data).unwrap_or("");
                let toml_value: toml::Value =
                    toml::from_str(toml_str).unwrap_or(toml::Value::Table(toml::Table::new()));
                let mut config = std::collections::HashMap::new();
                crate::application::use_cases::queries::manage_agent_config::ManageAgentConfigUseCase::flatten_toml_value("".to_string(), &toml_value, &mut config);
                config.len() as u64
            }
            "json" => {
                let json_str = std::str::from_utf8(&data).unwrap_or("{}");
                let config: std::collections::HashMap<String, String> =
                    serde_json::from_str(json_str).unwrap_or_default();
                config.len() as u64
            }
            "yaml" => {
                let yaml_str = std::str::from_utf8(&data).unwrap_or("");
                let config: std::collections::HashMap<String, String> =
                    serde_yaml::from_str(yaml_str).unwrap_or_default();
                config.len() as u64
            }
            _ => 0,
        };

        Ok(Response::new(
            oasis_core::proto::BackupAgentConfigResponse {
                config_data: data,
                format,
                key_count,
            },
        ))
    }

    #[instrument(skip_all)]
    async fn restore_agent_config(
        &self,
        request: Request<oasis_core::proto::RestoreAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::RestoreAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }

        let (restored, skipped) = self
            .manage_agent_config_use_case
            .restore_config(&req.agent_id, &req.config_data, &req.format, req.overwrite)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(
            oasis_core::proto::RestoreAgentConfigResponse {
                restored_keys: restored,
                skipped_keys: skipped,
            },
        ))
    }

    #[instrument(skip_all)]
    async fn validate_agent_config(
        &self,
        request: Request<oasis_core::proto::ValidateAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ValidateAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }

        let (valid, errors, warnings) = self
            .manage_agent_config_use_case
            .validate_config(&req.agent_id, &req.config_data, &req.format)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(
            oasis_core::proto::ValidateAgentConfigResponse {
                valid,
                errors,
                warnings,
            },
        ))
    }

    #[instrument(skip_all)]
    async fn diff_agent_config(
        &self,
        request: Request<oasis_core::proto::DiffAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::DiffAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }

        let changes = self
            .manage_agent_config_use_case
            .diff_config(&req.agent_id, &req.from_config, &req.to_config, &req.format)
            .await
            .map_err(map_core_error)?;

        let mut added_count = 0;
        let mut modified_count = 0;
        let mut deleted_count = 0;

        let proto_changes: Vec<oasis_core::proto::ConfigDiffItem> = changes
            .into_iter()
            .map(|change| {
                match change.operation.as_str() {
                    "added" => added_count += 1,
                    "modified" => modified_count += 1,
                    "deleted" => deleted_count += 1,
                    _ => {}
                }
                oasis_core::proto::ConfigDiffItem {
                    key: change.key,
                    operation: change.operation,
                    old_value: change.old_value.unwrap_or_default(),
                    new_value: change.new_value.unwrap_or_default(),
                }
            })
            .collect();

        Ok(Response::new(oasis_core::proto::DiffAgentConfigResponse {
            changes: proto_changes,
            added_count,
            modified_count,
            deleted_count,
        }))
    }
}

impl OasisServer {
    /// 启动健康检查监控
    pub async fn start_health_monitoring(
        &self,
        jetstream: async_nats::jetstream::Context,
        heartbeat_ttl_sec: u64,
    ) {
        self.health_service
            .start_monitoring(jetstream, self.context.node_repo.clone(), heartbeat_ttl_sec)
            .await;

        tracing::info!("Health monitoring started in OasisServer");
    }
}
