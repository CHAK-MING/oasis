use tonic::{Request, Response, Status};
use tracing::instrument;

use oasis_core::proto::{
    ApplyFileRequest, ApplyFileResponse, ClearFilesRequest, ClearFilesResponse, ExecuteTaskRequest,
    ExecuteTaskResponse, GetTaskResultRequest, GetTaskResultResponse,
};

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct TaskHandlers;

impl TaskHandlers {
    #[instrument(skip_all)]
    pub async fn execute_task(
        srv: &OasisServer,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        let req = request.into_inner();
        if req.command.trim().is_empty() {
            return Err(Status::invalid_argument("Command cannot be empty"));
        }
        if req.target.is_none() {
            return Err(Status::invalid_argument("Target cannot be empty"));
        }
        if req.timeout_seconds <= 0 {
            return Err(Status::invalid_argument("Timeout must be greater than 0"));
        }

        // 只支持选择器
        let targets = match req.target.as_ref().and_then(|t| t.target.as_ref()) {
            Some(oasis_core::proto::task_target_msg::Target::Selector(s)) => {
                if s.is_empty() {
                    return Err(Status::invalid_argument("Selector cannot be empty"));
                }
                srv.manage_agents_use_case()
                    .resolve_selector(s)
                    .await
                    .map_err(map_core_error)?
            }
            // AgentIds 已移除于 proto
            None => return Err(Status::invalid_argument("Target is required")),
        };

        let task_id = srv
            .execute_task_use_case()
            .execute(
                targets,
                req.command,
                req.args,
                req.timeout_seconds as u32,
                Some(req.env),
            )
            .await
            .map_err(map_core_error)?;

        tracing::Span::current().record("task_id", &task_id);
        Ok(Response::new(ExecuteTaskResponse {
            task_id: Some(oasis_core::proto::TaskId { value: task_id }),
        }))
    }

    #[instrument(skip_all)]
    pub async fn get_task_result(
        srv: &OasisServer,
        request: Request<GetTaskResultRequest>,
    ) -> Result<Response<GetTaskResultResponse>, Status> {
        let req = request.into_inner();
        if let Some(task_id) = &req.task_id {
            tracing::Span::current().record("task_id", &task_id.value);
        }

        let task_id = req.task_id.as_ref().unwrap().value.clone();
        let agent_id = req.agent_id.as_ref().map(|id| id.value.as_str());
        let wait_timeout_ms = req.wait_timeout_ms;

        let get_result_closure = || async {
            srv.execute_task_use_case()
                .get_task_result(&task_id, agent_id)
                .await
                .map_err(map_core_error)
        };

        let to_response =
            |task_result: crate::domain::models::task::TaskResult| -> GetTaskResultResponse {
                GetTaskResultResponse {
                    found: true,
                    timestamp: task_result.timestamp,
                    duration_ms: task_result.duration_ms,
                    task_id: Some(oasis_core::proto::TaskId {
                        value: task_result.task_id.to_string(),
                    }),
                    agent_id: Some(oasis_core::proto::AgentId {
                        value: task_result.agent_id.to_string(),
                    }),
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
                task_id: Some(oasis_core::proto::TaskId {
                    value: task_id.clone(),
                }),
                agent_id: req.agent_id.clone(),
                ..Default::default()
            }
        };

        if wait_timeout_ms <= 0 {
            if let Some(task_result) = get_result_closure().await? {
                return Ok(Response::new(to_response(task_result)));
            }
            return Ok(Response::new(not_found_response()));
        }

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
    pub async fn apply_file(
        srv: &OasisServer,
        request: Request<ApplyFileRequest>,
    ) -> Result<Response<ApplyFileResponse>, Status> {
        let req = request.into_inner();
        if req.object_name.trim().is_empty() {
            return Err(Status::invalid_argument("Object name cannot be empty"));
        }
        if req.destination_path.trim().is_empty() {
            return Err(Status::invalid_argument("Destination path cannot be empty"));
        }
        if req.target.is_none() {
            return Err(Status::invalid_argument("Target cannot be empty"));
        }
        if req.file_data.is_empty() {
            return Err(Status::invalid_argument("File data cannot be empty"));
        }

        srv.upload_file_use_case()
            .push_file(&req.object_name, req.file_data)
            .await
            .map_err(map_core_error)?;

        // 只支持选择器 - 文件分发需要动态目标解析
        let target_selector = match req.target.as_ref().and_then(|t| t.target.as_ref()) {
            Some(oasis_core::proto::task_target_msg::Target::Selector(s)) => {
                if s.is_empty() {
                    return Err(Status::invalid_argument("Selector cannot be empty"));
                }
                s.clone()
            }
            // AgentIds 已移除于 proto
            None => return Err(Status::invalid_argument("Target is required")),
        };

                  let response = srv
            .manage_agents_use_case()
            .resolve_selector(&target_selector)
            .await
            .map_err(map_core_error)?;

        if response.is_empty() {
            return Ok(Response::new(ApplyFileResponse {
                success: false,
                message: format!("No nodes matched selector: {}", target_selector),
                applied_agents: Vec::new(),
                failed_agents: Vec::new(),
            }));
        }

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

        let task_id = srv
            .upload_file_use_case()
            .apply_file(config, response.clone())
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(ApplyFileResponse {
            success: true,
            message: format!("File apply task created: {}", task_id),
            applied_agents: response
                .iter()
                .map(|id| oasis_core::proto::AgentId { value: id.clone() })
                .collect(),
            failed_agents: Vec::new(),
        }))
    }

    #[instrument(skip_all)]
    pub async fn clear_files(
        srv: &OasisServer,
        _request: Request<ClearFilesRequest>,
    ) -> Result<Response<ClearFilesResponse>, Status> {
        let deleted = srv
            .clear_files_use_case()
            .clear_all()
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(ClearFilesResponse {
            deleted_count: deleted,
        }))
    }
}
