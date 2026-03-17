//! TaskService - 专注基础CRUD操作

use crate::infrastructure::monitor::task_monitor::TaskMonitor;
use async_nats::jetstream::Context;
use oasis_core::constants;
use oasis_core::core_types::{AgentId, BatchId, TaskId};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use oasis_core::task_types::*;
use prost::Message;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
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
    fn mark_task_cancelled(task: &mut Task) -> bool {
        if !task.state.is_cancellable() {
            return false;
        }

        task.transition_to(TaskState::Cancelling).is_ok()
    }

    fn task_publish_headers(task_id: &TaskId, payload: &[u8]) -> async_nats::HeaderMap {
        let mut headers = async_nats::HeaderMap::new();
        let mut hasher = Sha256::new();
        hasher.update(payload);
        let digest = hasher.finalize();

        let mut digest_hex = String::with_capacity(64);
        for b in digest.iter() {
            let _ = write!(&mut digest_hex, "{:02x}", b);
        }
        let digest_prefix = &digest_hex[..12];
        headers.insert(
            "Nats-Msg-Id",
            format!("task-{}-sha256-{}", task_id, digest_prefix),
        );
        headers
    }

    fn group_publish_headers(
        batch_id: &BatchId,
        group: &str,
        payload: &[u8],
    ) -> async_nats::HeaderMap {
        let mut headers = async_nats::HeaderMap::new();
        let mut hasher = Sha256::new();
        hasher.update(payload);
        let digest = hasher.finalize();

        let mut digest_hex = String::with_capacity(64);
        for b in digest.iter() {
            let _ = write!(&mut digest_hex, "{:02x}", b);
        }
        let digest_prefix = &digest_hex[..12];
        headers.insert(
            "Nats-Msg-Id",
            format!("task-group-{}-{}-sha256-{}", batch_id, group, digest_prefix),
        );
        headers
    }

    fn cancel_publish_headers(task_id: &TaskId) -> async_nats::HeaderMap {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", format!("task-cancel-{}", task_id));
        headers
    }

    fn extract_multicast_group(
        selector: &oasis_core::core_types::SelectorExpression,
    ) -> Option<String> {
        let mut expr = selector.as_str().trim();
        if expr.starts_with('(') && expr.ends_with(')') {
            expr = expr[1..expr.len() - 1].trim();
        }

        let (quote, suffix) = match expr.as_bytes().first().copied() {
            Some(b'"') => ('"', "\" in groups"),
            Some(b'\'') => ('\'', "' in groups"),
            _ => return None,
        };

        if !expr.ends_with(suffix) || expr.len() <= suffix.len() + 1 {
            return None;
        }

        let group = &expr[1..expr.len() - suffix.len()];
        if group.is_empty() || group.contains(quote) {
            return None;
        }

        Some(group.to_string())
    }

    fn build_group_task_message(
        batch_id: &BatchId,
        group: &str,
        request: &BatchRequest,
        tasks_and_agents: &[(Task, AgentId)],
    ) -> oasis_core::proto::GroupTaskMsg {
        let agent_task_ids = tasks_and_agents
            .iter()
            .map(|(task, agent_id)| {
                (
                    agent_id.as_str().to_string(),
                    task.task_id.as_str().to_string(),
                )
            })
            .collect();

        oasis_core::proto::GroupTaskMsg {
            batch_id: Some(oasis_core::proto::BatchId {
                value: batch_id.as_str().to_string(),
            }),
            group: group.to_string(),
            command: request.command.clone(),
            args: request.args.clone(),
            timeout_seconds: request.timeout_seconds,
            agent_task_ids,
        }
    }

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

        // 并发创建所有任务（但不缓存）
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

                    task.transition_to(TaskState::Pending)
                        .map_err(|e| CoreError::Internal {
                            message: format!("Failed to transition task state: {}", e),
                            severity: ErrorSeverity::Error,
                        })?;

                    Ok::<(Task, AgentId), CoreError>((task, agent_id))
                }
            })
            .collect();

        let tasks_and_agents: Vec<(Task, AgentId)> =
            futures_util::future::try_join_all(task_futures).await?;

        // 批量发布所有任务
        let mut task_ids = Vec::with_capacity(tasks_and_agents.len());
        let multicast_group =
            Self::extract_multicast_group(&request.selector).filter(|_| tasks_and_agents.len() > 1);

        if let Some(group) = multicast_group.as_deref() {
            let subject = constants::tasks_group_subject(group);
            let group_task =
                Self::build_group_task_message(&batch_id, group, &request, &tasks_and_agents);
            let payload = group_task.encode_to_vec();
            let headers = Self::group_publish_headers(&batch_id, group, &payload);

            let ack = self
                .jetstream
                .publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(|e| CoreError::Nats {
                    message: format!("Failed to publish group task: {}", e),
                    severity: ErrorSeverity::Error,
                })?;

            ack.await.map_err(|e| CoreError::Nats {
                message: format!(
                    "Failed to confirm group task publish for batch {} and group {}: {}",
                    batch_id, group, e
                ),
                severity: ErrorSeverity::Error,
            })?;

            task_ids.extend(
                tasks_and_agents
                    .iter()
                    .map(|(task, _)| task.task_id.clone()),
            );
            info!(
                "Batch {} published {} tasks via group multicast subject {}",
                batch_id,
                task_ids.len(),
                group
            );
        } else {
            let mut ack_futures = Vec::with_capacity(tasks_and_agents.len());
            for (task, agent_id) in &tasks_and_agents {
                let subject = constants::tasks_unicast_subject(agent_id);
                let proto_task = oasis_core::proto::TaskMsg::from(task);
                let payload = proto_task.encode_to_vec();

                let headers = Self::task_publish_headers(&task.task_id, &payload);

                let ack_future = self
                    .jetstream
                    .publish_with_headers(subject, headers, payload.into())
                    .await
                    .map_err(|e| CoreError::Nats {
                        message: format!("Failed to publish unicast task: {}", e),
                        severity: ErrorSeverity::Error,
                    })?;

                ack_futures.push((task.task_id.clone(), ack_future));
                task_ids.push(task.task_id.clone());
            }

            for (task_id, ack_future) in ack_futures {
                ack_future.await.map_err(|e| CoreError::Nats {
                    message: format!("Failed to confirm task {} publish: {}", task_id, e),
                    severity: ErrorSeverity::Error,
                })?;
            }
        }

        debug!(
            "Batch {} published {} tasks to NATS",
            batch_id,
            task_ids.len()
        );

        // 一次性批量缓存（避免竞态条件）
        let batch = Batch {
            batch_id: batch_id.clone(),
            command: request.command.clone(),
            args: request.args.clone(),
            timeout_seconds: request.timeout_seconds,
            created_at: chrono::Utc::now().timestamp(),
        };
        self.task_monitor.cache_insert_batch(batch);

        self.task_monitor
            .cache_insert_batch_tasks(batch_id.clone(), task_ids.clone());

        for task_id in &task_ids {
            self.task_monitor
                .cache_insert_task_batch(task_id.clone(), batch_id.clone());
        }

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

        Err(CoreError::task_not_found(task_id))
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
            let can_cancel = self
                .task_monitor
                .task_cache
                .get(&task_id)
                .map(|task| task.state.is_cancellable())
                .unwrap_or(false);

            if !can_cancel {
                continue;
            }

            if let Err(e) = self.publish_cancel_message(&task_id).await {
                warn!(
                    "Failed to publish cancel message for task {}: {}",
                    task_id, e
                );
                continue;
            }

            if let Some(mut cached_task) = self.task_monitor.task_cache.get_mut(&task_id) {
                let task = Arc::make_mut(&mut cached_task);
                if Self::mark_task_cancelled(task) {
                    cancelled_count += 1;
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
        let task =
            self.task_monitor
                .task_cache
                .get(task_id)
                .ok_or_else(|| CoreError::task_not_found(task_id))?;

        let agent_id = &task.agent_id;
        let subject = format!("tasks.cancel.agent.{}.{}", agent_id, task_id);
        let cancel_msg = oasis_core::proto::TaskMsg {
            task_id: Some(oasis_core::proto::TaskId {
                value: task_id.to_string(),
            }),
            state: oasis_core::proto::TaskStateEnum::TaskCancelled as i32,
            ..Default::default()
        };

        let payload = cancel_msg.encode_to_vec();

        let headers = Self::cancel_publish_headers(task_id);

        let ack = self
            .jetstream
            .publish_with_headers(subject, headers, payload.into())
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

#[cfg(test)]
mod tests {
    use super::*;
    use oasis_core::core_types::{AgentId, BatchId};
    use std::collections::HashMap;

    #[test]
    fn test_mark_task_cancel_requested_only_changes_cancellable_tasks() {
        let batch_id = BatchId::generate();
        let agent_id = AgentId::new("agent-1");

        let mut cancellable = Task::new("echo".to_string(), Vec::new(), 30)
            .with_batch_id(batch_id.clone())
            .with_agent_id(agent_id.clone());
        cancellable.transition_to(TaskState::Pending).unwrap();

        assert!(TaskService::mark_task_cancelled(&mut cancellable));
        assert_eq!(cancellable.state, TaskState::Cancelling);

        let mut terminal = Task::new("echo".to_string(), Vec::new(), 30)
            .with_batch_id(batch_id)
            .with_agent_id(agent_id);
        terminal.transition_to(TaskState::Pending).unwrap();
        terminal.transition_to(TaskState::Running).unwrap();
        terminal.transition_to(TaskState::Success).unwrap();

        assert!(!TaskService::mark_task_cancelled(&mut terminal));
        assert_eq!(terminal.state, TaskState::Success);
    }

    #[test]
    fn test_extract_multicast_group_matches_exact_group_selector() {
        let selector = oasis_core::core_types::SelectorExpression::new("\"web\" in groups");
        assert_eq!(
            TaskService::extract_multicast_group(&selector),
            Some("web".to_string())
        );
    }

    #[test]
    fn test_extract_multicast_group_rejects_complex_selector() {
        let selector = oasis_core::core_types::SelectorExpression::new(
            "\"web\" in groups and labels[\"env\"] == \"prod\"",
        );
        assert_eq!(TaskService::extract_multicast_group(&selector), None);
    }

    #[test]
    fn test_build_group_task_message_preserves_per_agent_task_ids() {
        let batch_id = BatchId::generate();
        let request = BatchRequest {
            command: "uptime".to_string(),
            args: vec!["--pretty".to_string()],
            selector: oasis_core::core_types::SelectorExpression::new("\"web\" in groups"),
            timeout_seconds: 30,
        };

        let first_agent = AgentId::new("agent-1");
        let second_agent = AgentId::new("agent-2");

        let mut first_task = Task::new(
            request.command.clone(),
            request.args.clone(),
            request.timeout_seconds,
        )
        .with_batch_id(batch_id.clone())
        .with_agent_id(first_agent.clone());
        first_task.transition_to(TaskState::Pending).unwrap();

        let mut second_task = Task::new(
            request.command.clone(),
            request.args.clone(),
            request.timeout_seconds,
        )
        .with_batch_id(batch_id.clone())
        .with_agent_id(second_agent.clone());
        second_task.transition_to(TaskState::Pending).unwrap();

        let group_msg = TaskService::build_group_task_message(
            &batch_id,
            "web",
            &request,
            &[
                (first_task.clone(), first_agent.clone()),
                (second_task.clone(), second_agent.clone()),
            ],
        );

        let expected = HashMap::from([
            (
                first_agent.as_str().to_string(),
                first_task.task_id.as_str().to_string(),
            ),
            (
                second_agent.as_str().to_string(),
                second_task.task_id.as_str().to_string(),
            ),
        ]);

        assert_eq!(group_msg.group, "web");
        assert_eq!(group_msg.command, request.command);
        assert_eq!(group_msg.args, request.args);
        assert_eq!(group_msg.timeout_seconds, request.timeout_seconds);
        assert_eq!(group_msg.agent_task_ids, expected);
        assert_eq!(
            group_msg.batch_id.as_ref().map(|id| id.value.as_str()),
            Some(batch_id.as_str())
        );
    }
}
