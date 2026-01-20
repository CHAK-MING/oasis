//! TaskService - 专注基础CRUD操作

use crate::infrastructure::monitor::task_monitor::TaskMonitor;
use async_nats::jetstream::Context;
use oasis_core::constants;
use oasis_core::core_types::{AgentId, BatchId, TaskId};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use oasis_core::task_types::*;
use prost::Message;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// n任务服务
pub struct TaskService {
    /// JetStream 上下文
    jetstream: Arc<Context>,
    /// 任务监控器 - 维护任务与执行缓存
    task_monitor: Arc<TaskMonitor>,
}

impl TaskService {
    /// 创建新的任务服务
    pub async fn new(jetstream: Arc<Context>, task_monitor: Arc<TaskMonitor>) -> Result<Self> {
        info!("Initializing TaskService");

        Ok(Self {
            jetstream,
            task_monitor,
        })
    }

    /// 提交新批次任务 - 接收已解析的代理列表
    pub async fn submit_batch(
        &self,
        request: BatchRequest,
        resolved_agent_ids: Vec<AgentId>,
    ) -> Result<BatchId> {
        let batch_id = BatchId::generate();

        info!(
            "Submitting batch: {} with command: {} to {} agents",
            batch_id,
            request.command,
            resolved_agent_ids.len()
        );

        // 步骤1: 并发创建所有任务（但不缓存）
        let task_futures: Vec<_> = resolved_agent_ids
            .into_iter()
            .map(|agent_id| {
                let request = request.clone();
                let batch_id = batch_id.clone();
                async move {
                    let mut task = Task::new(
                        request.command.clone(),
                        request.args.clone(),
                        request.timeout_seconds,
                    );
                    task = task.with_batch_id(batch_id);
                    task = task.with_agent_id(agent_id.clone());

                    // 转换状态为 Pending
                    task.transition_to(TaskState::Pending)
                        .map_err(|e| CoreError::Internal {
                            message: format!("Failed to transition task state: {}", e),
                            severity: ErrorSeverity::Error,
                        })?;

                    Ok::<(Task, AgentId), CoreError>((task, agent_id))
                }
            })
            .collect();

        // 步骤2: 等待所有任务创建完成
        let tasks_and_agents: Vec<(Task, AgentId)> =
            futures_util::future::try_join_all(task_futures).await?;

        // 步骤3: 批量发布所有任务
        let mut ack_futures = Vec::with_capacity(tasks_and_agents.len());
        let mut task_ids = Vec::with_capacity(tasks_and_agents.len());

        for (task, agent_id) in &tasks_and_agents {
            let subject = constants::tasks_unicast_subject(agent_id);
            let proto_task = oasis_core::proto::TaskMsg::from(task);
            let payload = proto_task.encode_to_vec();

            // 发布消息，获取 PublishAckFuture（此时消息已发送到 NATS）
            let ack_future = self
                .jetstream
                .publish(subject, payload.into())
                .await
                .map_err(|e| CoreError::Nats {
                    message: format!("Failed to publish unicast task: {}", e),
                    severity: ErrorSeverity::Error,
                })?;

            ack_futures.push((task.task_id.clone(), ack_future));
            task_ids.push(task.task_id.clone());
        }

        // 步骤4: 统一等待所有 ACK 确认
        for (task_id, ack_future) in ack_futures {
            ack_future.await.map_err(|e| CoreError::Nats {
                message: format!("Failed to confirm task {} publish: {}", task_id, e),
                severity: ErrorSeverity::Error,
            })?;
        }

        debug!(
            "Batch {} published {} tasks to NATS",
            batch_id,
            task_ids.len()
        );

        // 步骤5: 一次性批量缓存（避免竞态条件）

        // 5.1 创建 Batch 对象并缓存
        let batch = Batch {
            batch_id: batch_id.clone(),
            command: request.command.clone(),
            args: request.args.clone(),
            timeout_seconds: request.timeout_seconds,
            created_at: chrono::Utc::now().timestamp(),
        };
        self.task_monitor.cache_insert_batch(batch);

        // 5.2 一次性插入 BatchId -> Vec<TaskId> 映射
        self.task_monitor
            .cache_insert_batch_tasks(batch_id.clone(), task_ids.clone());

        // 5.3 批量插入 TaskId -> BatchId 反向映射
        for task_id in &task_ids {
            self.task_monitor
                .cache_insert_task_batch(task_id.clone(), batch_id.clone());
        }

        // 5.4 批量缓存所有任务
        for (task, _) in tasks_and_agents {
            self.task_monitor.cache_insert_task(task);
        }

        info!(
            "Batch {} submitted successfully with {} tasks",
            batch_id,
            task_ids.len()
        );
        Ok(batch_id)
    }

    /// 获取批次的所有任务执行信息
    pub async fn get_batch_details(
        &self,
        batch_id: &BatchId,
        state_filter: Option<Vec<TaskState>>,
    ) -> Result<Vec<TaskExecution>> {
        debug!("Getting batch details for batch {}", batch_id);

        // 获取这个批次的所有任务ID
        let task_ids = self
            .task_monitor
            .get_batch_task_ids(batch_id)
            .ok_or_else(|| CoreError::batch_not_found(batch_id.clone()))?;

        let mut task_executions: Vec<TaskExecution> = Vec::new();

        for task_id in task_ids {
            // 获取任务执行结果
            if let Some(execution) = self.task_monitor.latest_execution_from_cache(&task_id) {
                // 应用状态过滤器
                if let Some(ref states) = state_filter {
                    if states.contains(&execution.state) {
                        task_executions.push(execution);
                    }
                } else {
                    task_executions.push(execution);
                }
            } else {
                // 如果没有执行结果，但有任务，创建一个基于任务状态的执行记录
                if let Some(task) = self.task_monitor.task_cache.get(&task_id) {
                    let fake_execution = TaskExecution {
                        task_id: task_id.clone(),
                        agent_id: task.agent_id.clone(),
                        state: task.state,
                        exit_code: None,
                        stdout: String::new(),
                        stderr: String::new(),
                        started_at: task.created_at,
                        finished_at: None,
                        duration_ms: None,
                    };

                    // 应用状态过滤器
                    if let Some(ref states) = state_filter {
                        if states.contains(&fake_execution.state) {
                            task_executions.push(fake_execution);
                        }
                    } else {
                        task_executions.push(fake_execution);
                    }
                }
            }
        }

        Ok(task_executions)
    }

    /// 获取单个任务的完整输出（stdout/stderr）
    pub async fn get_task_output(&self, task_id: &TaskId) -> Result<TaskExecution> {
        if let Some(execution) = self.task_monitor.latest_execution_from_cache(task_id) {
            return Ok(execution);
        }

        if let Some(task) = self.task_monitor.task_cache.get(task_id) {
            return Ok(TaskExecution {
                task_id: task_id.clone(),
                agent_id: task.agent_id.clone(),
                state: task.state,
                exit_code: None,
                stdout: String::new(),
                stderr: String::new(),
                started_at: task.created_at,
                finished_at: None,
                duration_ms: None,
            });
        }

        Err(CoreError::NotFound {
            entity_type: "task".to_string(),
            entity_id: task_id.to_string(),
            severity: oasis_core::error::ErrorSeverity::Error,
        })
    }

    /// 列出批次
    pub async fn list_batches(
        &self,
        limit: u32,
        state_filter: Option<Vec<TaskState>>,
    ) -> Result<(Vec<Batch>, u32)> {
        debug!("Listing batches with limit: {}", limit);
        let (batches, total_count) = self
            .task_monitor
            .list_batches_from_cache(limit, state_filter.as_deref());

        Ok((batches, total_count))
    }

    /// 取消批次中的所有任务
    pub async fn cancel_batch(&self, batch_id: &BatchId) -> Result<()> {
        info!("Cancelling batch: {}", batch_id);

        // 检查批次是否存在
        let _batch = self
            .task_monitor
            .batch_cache
            .get(batch_id)
            .ok_or_else(|| CoreError::batch_not_found(batch_id.clone()))?;

        // 获取这个批次的所有任务ID
        let task_ids = self
            .task_monitor
            .get_batch_task_ids(batch_id)
            .ok_or_else(|| CoreError::batch_not_found(batch_id.clone()))?;

        let mut cancelled_count = 0;

        // 批量取消所有任务
        for task_id in task_ids {
            if let Some(mut cached_task) = self.task_monitor.task_cache.get_mut(&task_id) {
                let task = Arc::make_mut(&mut cached_task);

                // 检查是否可以取消
                if task.state.is_cancellable() {
                    // 转换状态
                    if task.transition_to(TaskState::Cancelled).is_ok() {
                        // 发布取消消息
                        if let Err(e) = self.publish_cancel_message(&task_id).await {
                            warn!(
                                "Failed to publish cancel message for task {}: {}",
                                task_id, e
                            );
                        } else {
                            cancelled_count += 1;
                        }
                    }
                }
            }
        }

        info!(
            "Batch {} cancelled successfully, {} tasks cancelled",
            batch_id, cancelled_count
        );
        Ok(())
    }

    /// 发布取消消息
    async fn publish_cancel_message(&self, task_id: &TaskId) -> Result<()> {
        let subject = format!("tasks.cancel.{}", task_id);
        let cancel_msg = oasis_core::proto::TaskMsg {
            task_id: Some(oasis_core::proto::TaskId {
                value: task_id.to_string(),
            }),
            state: oasis_core::proto::TaskStateEnum::TaskCancelled as i32,
            ..Default::default()
        };

        let payload = cancel_msg.encode_to_vec();

        let ack = self
            .jetstream
            .publish(subject, payload.into())
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to publish cancel: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        ack.await.map_err(|e| CoreError::Nats {
            message: format!("Failed to confirm cancel publish: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        Ok(())
    }
}
