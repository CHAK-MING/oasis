//! Task gRPC handlers - 完整实现

use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

use oasis_core::core_types::BatchId;
use oasis_core::core_types::TaskId;
use oasis_core::proto;
use oasis_core::task_types::{BatchRequest, TaskState};
use oasis_core::utils::truncate_output_preview;

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct TaskHandlers;

impl TaskHandlers {
    /// 提交批量任务
    #[instrument(skip_all)]
    pub async fn submit_batch(
        srv: &OasisServer,
        request: tonic::Request<proto::SubmitBatchRequest>,
    ) -> std::result::Result<tonic::Response<proto::SubmitBatchResponse>, tonic::Status> {
        let proto_request = request.into_inner();

        // 验证请求
        let batch_req = proto_request
            .batch_request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("batch_request is required"))?;

        if let Err(e) = batch_req.validate() {
            return Err(Status::invalid_argument(format!("Invalid request: {}", e)));
        }

        let batch_request = BatchRequest::from(&proto_request);

        let selector_expr = batch_request.selector.as_str();
        let result = srv
            .context()
            .agent_service
            .query(selector_expr)
            .await
            .map_err(map_core_error)?;

        let resolved_agent_ids = result.to_online_agents();

        info!(
            "Resolved selector '{}' to {} agents",
            selector_expr,
            resolved_agent_ids.len()
        );

        let agent_nums = resolved_agent_ids.len() as i64;

        // 如果没有解析到任何在线的 Agent，直接返回错误，避免创建空批次
        if resolved_agent_ids.is_empty() {
            return Err(Status::invalid_argument(
                "未找到任何在线的 Agent，批次任务未创建（请检查选择器）",
            ));
        }

        match srv
            .context()
            .task_service
            .submit_batch(batch_request, resolved_agent_ids)
            .await
        {
            Ok(batch_id) => {
                let response = proto::SubmitBatchResponse::success(batch_id, agent_nums);
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to submit batch: {}", e);
                Err(map_core_error(e))
            }
        }
    }

    /// 获取单个任务的完整输出（stdout/stderr）
    #[instrument(skip_all)]
    pub async fn get_task_output(
        srv: &OasisServer,
        request: Request<proto::GetTaskOutputRequest>,
    ) -> Result<Response<proto::GetTaskOutputResponse>, Status> {
        let proto_request = request.into_inner();

        let task_id = match proto_request.task_id {
            Some(id) => {
                if let Err(e) = id.validate() {
                    return Err(Status::invalid_argument(format!("Invalid task_id: {}", e)));
                }
                TaskId::from(id)
            }
            None => {
                return Err(Status::invalid_argument("task_id is required"));
            }
        };

        match srv.context().task_service.get_task_output(&task_id).await {
            Ok(execution) => {
                let response = proto::GetTaskOutputResponse {
                    execution: Some(proto::TaskExecutionMsg::from(execution)),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to get task output {}: {}", task_id, e);
                Err(map_core_error(e))
            }
        }
    }

    #[instrument(skip_all)]
    pub async fn get_batch_details(
        srv: &OasisServer,
        request: Request<proto::GetBatchDetailsRequest>,
    ) -> Result<Response<proto::GetBatchDetailsResponse>, Status> {
        let proto_request = request.into_inner();

        let batch_id = match proto_request.batch_id {
            Some(id) => BatchId::from(id),
            None => {
                return Err(Status::invalid_argument("batch_id is required"));
            }
        };

        let state_filter = if proto_request.states.is_empty() {
            None
        } else {
            Some(
                proto_request
                    .states
                    .into_iter()
                    .map(TaskState::from)
                    .collect(),
            )
        };

        match srv
            .context()
            .task_service
            .get_batch_details(&batch_id, state_filter)
            .await
        {
            Ok(tasks) => {
                let response = proto::GetBatchDetailsResponse {
                    tasks: tasks
                        .into_iter()
                        .map(|t| {
                            let mut msg = proto::TaskExecutionMsg::from(t);
                            msg.stdout = truncate_output_preview(&msg.stdout);
                            msg.stderr = truncate_output_preview(&msg.stderr);
                            msg
                        })
                        .collect(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to get batch details: {}", e);
                Err(map_core_error(e))
            }
        }
    }

    /// 列出任务
    #[instrument(skip_all)]
    pub async fn list_batches(
        srv: &OasisServer,
        request: Request<proto::ListBatchesRequest>,
    ) -> Result<Response<proto::ListBatchesResponse>, Status> {
        let proto_request = request.into_inner();

        // 验证请求
        if proto_request.limit == 0 {
            return Err(Status::invalid_argument("limit must be greater than 0"));
        }

        // 转换状态过滤器
        let state_filter = if proto_request.states.is_empty() {
            None
        } else {
            Some(
                proto_request
                    .states
                    .into_iter()
                    .map(TaskState::from)
                    .collect(),
            )
        };

        match srv
            .context()
            .task_service
            .list_batches(proto_request.limit, state_filter)
            .await
        {
            Ok((batches, total_count)) => {
                let proto_batches: Vec<proto::BatchMsg> =
                    batches.into_iter().map(proto::BatchMsg::from).collect();
                let has_more = proto_batches.len() < total_count as usize;

                let response = proto::ListBatchesResponse {
                    batches: proto_batches,
                    total_count,
                    has_more,
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to list batches: {}", e);
                Err(map_core_error(e))
            }
        }
    }
    /// 取消任务
    #[instrument(skip_all)]
    pub async fn cancel_batch(
        srv: &OasisServer,
        request: Request<proto::CancelBatchRequest>,
    ) -> Result<Response<proto::CancelBatchResponse>, Status> {
        let proto_request = request.into_inner();

        // 验证请求
        let batch_id = match proto_request.batch_id {
            Some(id) => {
                if let Err(e) = id.validate() {
                    return Err(Status::invalid_argument(format!("Invalid batch_id: {}", e)));
                }
                BatchId::from(id)
            }
            None => {
                return Err(Status::invalid_argument("batch_id is required"));
            }
        };

        match srv.context().task_service.cancel_batch(&batch_id).await {
            Ok(_) => {
                let response = proto::CancelBatchResponse::success();
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to cancel batch {}: {}", batch_id, e);
                let response = proto::CancelBatchResponse::failure();
                Ok(Response::new(response))
            }
        }
    }
}
