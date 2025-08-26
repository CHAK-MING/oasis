use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct StreamingHandlers;

impl StreamingHandlers {
    pub async fn stream_task_results(
        srv: &OasisServer,
        request: Request<oasis_core::proto::StreamTaskResultsRequest>,
    ) -> Result<
        Response<
            std::pin::Pin<
                Box<
                    dyn futures::Stream<
                            Item = Result<oasis_core::proto::StreamTaskResultsResponse, Status>,
                        > + Send,
                >,
            >,
        >,
        Status,
    > {
        let req = request.into_inner();
        if req.task_id.is_none() || req.task_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Task ID cannot be empty"));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let task_id = req.task_id.as_ref().unwrap().value.clone();
        let shutdown_token = srv.shutdown_token();
        let stream_use_case = Arc::new(
            crate::application::use_cases::queries::StreamTaskResultsUseCase::new(
                srv.context().task_repo.clone(),
            ),
        );
        let backoff = srv.stream_backoff().clone();

        tokio::spawn(async move {
            let mut retry_count = 0;
            let max_retries = backoff.max_retries;
            let initial_poll_interval =
                std::time::Duration::from_millis(backoff.initial_delay_ms);
            let max_poll_interval = std::time::Duration::from_millis(backoff.max_delay_ms);

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
                                poll_interval = initial_poll_interval;
                                consecutive_empty_results = 0;
                                retry_count = 0;

                                if task_result.timestamp > last_result_timestamp {
                                    last_result_timestamp = task_result.timestamp;

                                    let agent_id = task_result.agent_id.clone();
                                    let resp = oasis_core::proto::StreamTaskResultsResponse {
                                        task_id: Some(oasis_core::proto::TaskId { value: task_result.task_id.to_string() }),
                                        agent_id: Some(oasis_core::proto::AgentId { value: task_result.agent_id.to_string() }),
                                        stdout: task_result.stdout,
                                        stderr: task_result.stderr,
                                        exit_code: match task_result.status {
                                            crate::domain::models::task::TaskStatus::Completed { exit_code } => exit_code,
                                            crate::domain::models::task::TaskStatus::Failed { .. } => -1,
                                            crate::domain::models::task::TaskStatus::Cancelled { .. } => -2,
                                            crate::domain::models::task::TaskStatus::Timeout => -3,
                                            _ => 0,
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
                                        _ => 0,
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
                                consecutive_empty_results += 1;
                                if consecutive_empty_results > 5 {
                                    poll_interval = std::cmp::min(poll_interval * 2, max_poll_interval);
                                }
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
}
