use crate::nats_client::{ManagedNatsClient, NatsClient};
use async_nats::jetstream;
use base64::Engine;
use dashmap::{DashMap, DashSet};
use futures::{StreamExt, stream::SelectAll};
use oasis_core::{
    constants::*,
    core_types::{AgentId, BatchId, TaskId},
    error::Result,
    rate_limit::{RateLimiterCollection, rate_limited_operation},
    shutdown::{ExecutionError, execute_process_with_cancellation},
    task_types::{Task, TaskExecution, TaskState},
};
use prost::Message;
use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const COMPLETED_TASK_CACHE_LIMIT: usize = 1024;

#[derive(Clone)]
pub struct TaskManager {
    agent_id: AgentId,
    groups: Vec<String>,
    nats_client: ManagedNatsClient,
    shutdown_token: CancellationToken,
    running_tasks: Arc<DashMap<TaskId, CancellationToken>>,
    cancelled_tasks: Arc<DashSet<TaskId>>,
    completed_tasks: Arc<DashMap<TaskId, TaskExecution>>,
    completed_task_order: Arc<std::sync::Mutex<VecDeque<TaskId>>>,
    rate_limits: Arc<RateLimiterCollection>,
}

impl TaskManager {
    fn should_ack_task_message(result_publish: &Result<()>) -> bool {
        result_publish.is_ok()
    }

    fn register_cancel_request(
        task_id: &TaskId,
        running_tasks: &Arc<DashMap<TaskId, CancellationToken>>,
        cancelled_tasks: &Arc<DashSet<TaskId>>,
    ) {
        cancelled_tasks.insert(task_id.clone());

        if let Some((_task_id, cancel_token)) = running_tasks.remove(task_id) {
            cancel_token.cancel();
        }
    }

    fn take_pending_cancellation(task_id: &TaskId, cancelled_tasks: &Arc<DashSet<TaskId>>) -> bool {
        cancelled_tasks.remove(task_id).is_some()
    }

    fn completed_execution(
        task_id: &TaskId,
        completed_tasks: &Arc<DashMap<TaskId, TaskExecution>>,
    ) -> Option<TaskExecution> {
        completed_tasks
            .get(task_id)
            .map(|entry| entry.value().clone())
    }

    fn cache_completed_execution(
        execution: &TaskExecution,
        completed_tasks: &Arc<DashMap<TaskId, TaskExecution>>,
        completed_task_order: &Arc<std::sync::Mutex<VecDeque<TaskId>>>,
    ) {
        if !execution.state.is_terminal() {
            return;
        }

        completed_tasks.insert(execution.task_id.clone(), execution.clone());

        let mut order = completed_task_order
            .lock()
            .expect("completed task cache order mutex poisoned");
        order.push_back(execution.task_id.clone());
        while order.len() > COMPLETED_TASK_CACHE_LIMIT {
            if let Some(evicted_task_id) = order.pop_front() {
                if evicted_task_id != execution.task_id {
                    completed_tasks.remove(&evicted_task_id);
                }
            }
        }
    }

    pub fn new(
        agent_id: AgentId,
        nats_client: ManagedNatsClient,
        shutdown_token: CancellationToken,
        groups: Vec<String>,
        rate_limits: Arc<RateLimiterCollection>,
    ) -> Self {
        Self {
            agent_id,
            groups,
            nats_client,
            shutdown_token,
            running_tasks: Arc::new(DashMap::new()),
            cancelled_tasks: Arc::new(DashSet::new()),
            completed_tasks: Arc::new(DashMap::new()),
            completed_task_order: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            rate_limits,
        }
    }

    pub(crate) fn parse_groups_from_info(info: &HashMap<String, String>) -> Vec<String> {
        info.get("__groups")
            .map(|groups| {
                groups
                    .split(',')
                    .map(|group| group.trim().to_string())
                    .filter(|group| !group.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn task_from_group_message(
        agent_id: &AgentId,
        group_msg: &oasis_core::proto::GroupTaskMsg,
    ) -> Option<Task> {
        let batch_id = group_msg
            .batch_id
            .as_ref()
            .map(|batch_id| BatchId::new(batch_id.value.clone()))?;
        let task_id = group_msg.agent_task_ids.get(agent_id.as_str())?;
        let now = chrono::Utc::now().timestamp();

        Some(Task {
            task_id: TaskId::new(task_id.clone()),
            batch_id,
            agent_id: agent_id.clone(),
            command: group_msg.command.clone(),
            args: group_msg.args.clone(),
            timeout_seconds: group_msg.timeout_seconds,
            state: TaskState::Pending,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting task manager");
        let mut generation_rx = self.nats_client.subscribe_generation();

        loop {
            let client = self.nats_client.current().await;
            let unicast_consumer = self.create_unicast_task_consumer(&client).await?;
            let cancel_consumer = self.create_cancel_consumer(&client).await?;
            let group_consumers = self.create_group_task_consumers(&client).await?;
            let has_group_consumers = !group_consumers.is_empty();

            let mut unicast_messages = unicast_consumer.messages().await?;
            let mut cancel_messages = cancel_consumer.messages().await?;
            let mut group_messages = SelectAll::new();
            for consumer in group_consumers {
                group_messages.push(consumer.messages().await?);
            }

            info!("Task manager started with task and cancel consumers");

            loop {
                tokio::select! {
                    Some(msg_result) = unicast_messages.next() => {
                        match msg_result {
                            Ok(msg) => {
                                debug!("Received unicast task message");
                                if let Err(e) = self.process_task_message(msg, "unicast").await {
                                    error!("Failed to process unicast task message: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Error receiving unicast task message: {}", e);
                            }
                        }
                    }
                    Some(msg_result) = cancel_messages.next() => {
                        match msg_result {
                            Ok(msg) => {
                                debug!("Received cancel message");
                                if let Err(e) = self.process_cancel_message(msg).await {
                                    error!("Failed to process cancel message: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Error receiving cancel message: {}", e);
                            }
                        }
                    }
                    Some(msg_result) = group_messages.next(), if has_group_consumers => {
                        match msg_result {
                            Ok(msg) => {
                                debug!("Received group task message");
                                if let Err(e) = self.process_group_task_message(msg).await {
                                    error!("Failed to process group task message: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Error receiving group task message: {}", e);
                            }
                        }
                    }
                    changed = generation_rx.changed() => {
                        match changed {
                            Ok(()) => {
                                info!("NATS generation changed, rebuilding task consumers");
                                break;
                            }
                            Err(_) => {
                                info!("Task manager generation channel closed");
                                return Ok(());
                            }
                        }
                    }
                    _ = self.shutdown_token.cancelled() => {
                        info!("Task manager shutting down");
                        return Ok(());
                    }
                }
            }
        }
    }

    /// 创建单播任务消费者（独占消费，仅此Agent接收）
    async fn create_unicast_task_consumer(
        &self,
        nats_client: &NatsClient,
    ) -> Result<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
        let stream = nats_client.jetstream.get_stream(JS_STREAM_TASKS).await?;

        // 为此Agent创建专用的消费者，接收单播任务
        let consumer_name = unicast_consumer_name(&self.agent_id);
        let subject = tasks_unicast_subject(&self.agent_id);

        let consumer = stream
            .create_consumer(
                oasis_core::nats::ConsumerConfigBuilder::new(
                    consumer_name.clone(),
                    subject.clone(),
                )
                .build(),
            )
            .await?;

        info!(
            "Created unicast task consumer: {} for subject: {}",
            consumer_name, subject
        );

        Ok(consumer)
    }

    async fn create_group_task_consumers(
        &self,
        nats_client: &NatsClient,
    ) -> Result<Vec<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>>> {
        if self.groups.is_empty() {
            return Ok(Vec::new());
        }

        let stream = nats_client
            .jetstream
            .get_stream(JS_STREAM_GROUP_TASKS)
            .await?;

        let mut consumers = Vec::with_capacity(self.groups.len());
        for group in &self.groups {
            let consumer_name = group_consumer_name(&self.agent_id, group);
            let subject = tasks_group_subject(group);
            let consumer = stream
                .create_consumer(
                    oasis_core::nats::ConsumerConfigBuilder::new(
                        consumer_name.clone(),
                        subject.clone(),
                    )
                    .build(),
                )
                .await?;

            info!(
                "Created group task consumer: {} for subject: {}",
                consumer_name, subject
            );
            consumers.push(consumer);
        }

        Ok(consumers)
    }

    async fn create_cancel_consumer(
        &self,
        nats_client: &NatsClient,
    ) -> Result<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
        let stream = nats_client.jetstream.get_stream(JS_STREAM_TASKS).await?;

        let consumer_name = format!("oasis-cancel-v1-{}", self.agent_id);
        let subject = format!("tasks.cancel.agent.{}.>", self.agent_id);

        let consumer = stream
            .create_consumer(
                oasis_core::nats::ConsumerConfigBuilder::new(
                    consumer_name.clone(),
                    subject.clone(),
                )
                .build(),
            )
            .await?;

        info!(
            "Created cancel consumer: {} for subject: {}",
            consumer_name, subject
        );

        Ok(consumer)
    }

    async fn process_cancel_message(&self, msg: jetstream::Message) -> Result<()> {
        let task_id = match Self::parse_task_id_from_cancel_subject(&msg.subject) {
            Ok(id) => id,
            Err(e) => {
                warn!("Failed to parse task_id from cancel subject: {}", e);
                msg.ack()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;
                return Ok(());
            }
        };

        info!("Processing cancel request for task: {}", task_id);

        let was_running = self.running_tasks.contains_key(&task_id);
        Self::register_cancel_request(&task_id, &self.running_tasks, &self.cancelled_tasks);

        if was_running {
            info!("Cancelled running task: {}", task_id);
        } else {
            debug!("Recorded cancel request for pending task {}", task_id);
        }

        msg.ack()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to ack cancel message: {}", e))?;

        Ok(())
    }

    fn parse_task_id_from_cancel_subject(subject: &str) -> Result<TaskId> {
        let parts: Vec<&str> = subject.split('.').collect();
        if parts.len() >= 5 && parts[0] == "tasks" && parts[1] == "cancel" && parts[2] == "agent" {
            Ok(TaskId::new(parts[4]))
        } else {
            Err(anyhow::anyhow!("Invalid cancel subject format: {}", subject).into())
        }
    }

    /// 处理任务消息
    async fn process_task_message(&self, msg: jetstream::Message, source: &str) -> Result<()> {
        let task = match oasis_core::proto::TaskMsg::decode(msg.payload.as_ref()) {
            Ok(task_msg) => Task::from(task_msg),
            Err(e) => {
                error!("Failed to decode task message from {}: {}", source, e);
                msg.ack()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;
                return Ok(());
            }
        };

        self.process_task(msg, task, source).await
    }

    async fn process_group_task_message(&self, msg: jetstream::Message) -> Result<()> {
        let group_msg = match oasis_core::proto::GroupTaskMsg::decode(msg.payload.as_ref()) {
            Ok(group_msg) => group_msg,
            Err(e) => {
                error!("Failed to decode group task message: {}", e);
                msg.ack()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;
                return Ok(());
            }
        };

        let Some(task) = Self::task_from_group_message(&self.agent_id, &group_msg) else {
            warn!(
                "Ignoring group task {} because agent {} is not in the delivery map",
                group_msg.group, self.agent_id
            );
            msg.ack()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;
            return Ok(());
        };

        self.process_task(msg, task, "group").await
    }

    async fn process_task(&self, msg: jetstream::Message, task: Task, source: &str) -> Result<()> {
        info!("Processing {} task: {}", source, task.task_id);

        if let Some(cached_execution) =
            Self::completed_execution(&task.task_id, &self.completed_tasks)
        {
            info!(
                "Skipping duplicate task {} because a terminal result is already cached",
                task.task_id
            );
            let publish_result = self.publish_task_result(&cached_execution).await;
            if let Err(e) = &publish_result {
                error!("Failed to republish cached task result: {}", e);
            }

            if !Self::should_ack_task_message(&publish_result) {
                return Err(anyhow::anyhow!(
                    "Failed to republish cached task result for {}",
                    task.task_id
                )
                .into());
            }

            msg.ack()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;
            return Ok(());
        }

        if self.running_tasks.contains_key(&task.task_id) {
            info!(
                "Ignoring duplicate in-flight delivery for task {}; original execution is still running",
                task.task_id
            );
            msg.ack_with(async_nats::jetstream::AckKind::Progress)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to mark duplicate task as in-progress: {}", e)
                })?;
            return Ok(());
        }

        if Self::take_pending_cancellation(&task.task_id, &self.cancelled_tasks) {
            info!(
                "Skipping task {} because a cancel request was already recorded",
                task.task_id
            );
            let execution = Self::cancelled_execution(task.task_id.clone(), self.agent_id.clone());
            let publish_result = self.publish_task_result(&execution).await;
            if let Err(e) = &publish_result {
                error!("Failed to publish cancelled task result: {}", e);
            }

            if !Self::should_ack_task_message(&publish_result) {
                return Err(anyhow::anyhow!(
                    "Failed to publish cancelled task result for {}",
                    task.task_id
                )
                .into());
            }

            msg.ack()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;
            return Ok(());
        }

        let task_cancel_token = CancellationToken::new();
        self.running_tasks
            .insert(task.task_id.clone(), task_cancel_token.clone());

        // 发送一个任务正在执行的状态
        let running_execution = TaskExecution::running(task.task_id.clone(), self.agent_id.clone());

        if let Err(e) = self.publish_task_result(&running_execution).await {
            error!("Failed to publish task result: {}", e);
        }

        // 执行任务
        let execution = self.execute_task(&task, task_cancel_token.clone()).await;
        Self::cache_completed_execution(
            &execution,
            &self.completed_tasks,
            &self.completed_task_order,
        );

        self.running_tasks.remove(&task.task_id);
        self.cancelled_tasks.remove(&task.task_id);

        // 发布执行结果
        let publish_result = self.publish_task_result(&execution).await;
        if let Err(e) = &publish_result {
            error!("Failed to publish task result: {}", e);
        }

        if !Self::should_ack_task_message(&publish_result) {
            return Err(anyhow::anyhow!(
                "Failed to publish final task result for {}",
                task.task_id
            )
            .into());
        }

        // 仅在终态结果成功发布后确认消息，避免任务已执行但结果丢失。
        msg.ack()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;

        Ok(())
    }

    fn cancelled_execution(task_id: TaskId, agent_id: AgentId) -> TaskExecution {
        let now = chrono::Utc::now().timestamp();
        TaskExecution {
            task_id,
            agent_id,
            state: TaskState::Cancelled,
            exit_code: None,
            stdout: String::new(),
            stderr: "Task cancelled before execution started".to_string(),
            started_at: now,
            finished_at: Some(now),
            duration_ms: Some(0.0),
        }
    }

    async fn execute_task(
        &self,
        task: &Task,
        task_cancel_token: CancellationToken,
    ) -> TaskExecution {
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let start_instant = std::time::Instant::now();

        match task.command.as_str() {
            CMD_LABELS_UPDATE => {
                self.execute_labels_update_task(task, start_time, start_instant)
                    .await
            }
            _ => {
                self.execute_shell_command(task, start_time, start_instant, task_cancel_token)
                    .await
            }
        }
    }

    async fn execute_labels_update_task(
        &self,
        task: &Task,
        start_time: i64,
        start_instant: std::time::Instant,
    ) -> TaskExecution {
        debug!("Executing labels update task: {}", task.task_id);

        // 解析 labels 更新参数
        let mut new_labels = std::collections::HashMap::new();
        for arg in &task.args {
            if let Some((key, value)) = arg.split_once('=') {
                new_labels.insert(key.to_string(), value.to_string());
            }
        }

        // 更新本地标签到 KV
        match self.update_agent_labels(new_labels).await {
            Ok(_) => {
                let finish_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                TaskExecution::completed(
                    task.task_id.clone(),
                    self.agent_id.clone(),
                    TaskState::Success,
                    Some(0),
                    "Labels updated successfully".to_string(),
                    String::new(),
                    start_time,
                    finish_time,
                    start_instant.elapsed().as_millis() as f64,
                )
            }
            Err(e) => {
                let finish_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                TaskExecution::completed(
                    task.task_id.clone(),
                    self.agent_id.clone(),
                    TaskState::Failed,
                    Some(1),
                    String::new(),
                    format!("Failed to update labels: {}", e),
                    start_time,
                    finish_time,
                    start_instant.elapsed().as_millis() as f64,
                )
            }
        }
    }

    async fn update_agent_labels(
        &self,
        new_labels: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        use oasis_core::constants::{JS_KV_AGENT_LABELS, kv_key_labels};

        let kv = self
            .nats_client
            .current()
            .await
            .jetstream
            .get_key_value(JS_KV_AGENT_LABELS)
            .await?;
        let key = kv_key_labels(self.agent_id.as_str());

        let data = serde_json::to_vec(&new_labels).map_err(|e| {
            oasis_core::error::CoreError::Serialization {
                message: e.to_string(),
                severity: oasis_core::error::ErrorSeverity::Error,
            }
        })?;

        kv.put(&key, data.into()).await?;
        info!("Updated agent labels: {:?}", new_labels);

        Ok(())
    }

    async fn execute_shell_command(
        &self,
        task: &Task,
        start_time: i64,
        start_instant: std::time::Instant,
        task_cancel_token: CancellationToken,
    ) -> TaskExecution {
        info!(
            "Executing shell command: {} {} (timeout: {}s)",
            task.command,
            task.args.join(" "),
            task.timeout_seconds
        );

        // 构建完整命令
        let full_command = if task.args.is_empty() {
            task.command.clone()
        } else {
            format!("{} {}", task.command, task.args.join(" "))
        };

        // 创建子进程
        let child = match tokio::process::Command::new("/bin/sh")
            .args(["-c", &full_command])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(std::env::vars())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                let finish_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                return TaskExecution::completed(
                    task.task_id.clone(),
                    self.agent_id.clone(),
                    TaskState::Failed,
                    Some(-1),
                    String::new(),
                    format!("Command execution failed: {}", e),
                    start_time,
                    finish_time,
                    start_instant.elapsed().as_millis() as f64,
                );
            }
        };

        let timeout_duration = std::time::Duration::from_secs(task.timeout_seconds as u64);

        let combined_token = CancellationToken::new();

        tokio::spawn({
            let combined = combined_token.clone();
            let shutdown = self.shutdown_token.clone();
            let task_cancel = task_cancel_token.clone();
            async move {
                tokio::select! {
                    _ = shutdown.cancelled() => combined.cancel(),
                    _ = task_cancel.cancelled() => combined.cancel(),
                }
            }
        });

        let result = execute_process_with_cancellation(
            child,
            combined_token,
            timeout_duration,
            &full_command,
        )
        .await;

        let finish_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        match result {
            Ok(output) => {
                let exit_code = output.status.code().unwrap_or(-1);
                let state = if exit_code == 0 {
                    TaskState::Success
                } else {
                    TaskState::Failed
                };

                TaskExecution::completed(
                    task.task_id.clone(),
                    self.agent_id.clone(),
                    state,
                    Some(exit_code),
                    Self::encode_output(&output.stdout),
                    Self::encode_output(&output.stderr),
                    start_time,
                    finish_time,
                    start_instant.elapsed().as_millis() as f64,
                )
            }
            Err(ExecutionError::Cancelled) => TaskExecution::completed(
                task.task_id.clone(),
                self.agent_id.clone(),
                TaskState::Cancelled,
                Some(-1),
                String::new(),
                "Task cancelled".to_string(),
                start_time,
                finish_time,
                start_instant.elapsed().as_millis() as f64,
            ),
            Err(ExecutionError::Timeout(_)) => TaskExecution::completed(
                task.task_id.clone(),
                self.agent_id.clone(),
                TaskState::Timeout,
                Some(-1),
                String::new(),
                format!("Command timed out after {} seconds", task.timeout_seconds),
                start_time,
                finish_time,
                start_instant.elapsed().as_millis() as f64,
            ),
            Err(ExecutionError::Failed(e)) => TaskExecution::completed(
                task.task_id.clone(),
                self.agent_id.clone(),
                TaskState::Failed,
                Some(-1),
                String::new(),
                format!("Command execution failed: {}", e),
                start_time,
                finish_time,
                start_instant.elapsed().as_millis() as f64,
            ),
        }
    }

    fn encode_output(bytes: &[u8]) -> String {
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        format!("base64:{}", b64)
    }

    async fn publish_task_result(&self, execution: &TaskExecution) -> Result<()> {
        let subject = format!(
            "{}.{}.{}",
            RESULTS_SUBJECT_PREFIX, execution.task_id, execution.agent_id
        );
        let proto: oasis_core::proto::TaskExecutionMsg = execution.into();
        let data = proto.encode_to_vec();

        // 设置去重头部：包含阶段与时间，避免运行中消息与终态消息互相去重
        let mut headers = async_nats::HeaderMap::new();
        let phase = match execution.state {
            TaskState::Created => "created",
            TaskState::Pending => "pending",
            TaskState::Running => "running",
            TaskState::Cancelling => "cancelling",
            TaskState::Success => "success",
            TaskState::Failed => "failed",
            TaskState::Timeout => "timeout",
            TaskState::Cancelled => "cancelled",
        };
        let time_part = execution.finished_at.unwrap_or(execution.started_at);
        let dedupe_key = format!(
            "{}@{}@{}@{}",
            execution.task_id, execution.agent_id, phase, time_part
        );
        headers.insert("Nats-Msg-Id", dedupe_key);

        rate_limited_operation(
            &self.rate_limits.task_publish,
            || async {
                let ack = self
                    .nats_client
                    .current()
                    .await
                    .jetstream
                    .publish_with_headers(subject.clone(), headers, data.into())
                    .await?;

                ack.await?;
                Ok(())
            },
            Some(self.shutdown_token.clone()),
            "agent_task_publish_result",
        )
        .await?;
        info!("Published task result: {}", execution.task_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    mod encode_output_tests {
        use super::*;
        use base64::Engine;

        #[test]
        fn test_encode_empty_output() {
            let result = TaskManager::encode_output(&[]);
            assert_eq!(result, "base64:");
        }

        #[test]
        fn test_encode_simple_text() {
            let result = TaskManager::encode_output(b"hello world");
            assert!(result.starts_with("base64:"));

            let encoded_part = result
                .strip_prefix("base64:")
                .ok_or_else(|| anyhow::anyhow!("Invalid encoded format"))
                .unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded_part)
                .unwrap();
            assert_eq!(decoded, b"hello world");
        }

        #[test]
        fn test_encode_binary_data() {
            let binary_data: Vec<u8> = (0u8..=255).collect();
            let result = TaskManager::encode_output(&binary_data);
            assert!(result.starts_with("base64:"));

            let encoded_part = result
                .strip_prefix("base64:")
                .ok_or_else(|| anyhow::anyhow!("Invalid encoded format"))
                .unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded_part)
                .unwrap();
            assert_eq!(decoded, binary_data);
        }

        #[test]
        fn test_encode_utf8_text() {
            let utf8_text = "你好世界 🌍";
            let result = TaskManager::encode_output(utf8_text.as_bytes());

            let encoded_part = result
                .strip_prefix("base64:")
                .ok_or_else(|| anyhow::anyhow!("Invalid encoded format"))
                .unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded_part)
                .unwrap();
            assert_eq!(String::from_utf8(decoded).unwrap(), utf8_text);
        }

        #[test]
        fn test_encode_newlines() {
            let text_with_newlines = "line1\nline2\nline3";
            let result = TaskManager::encode_output(text_with_newlines.as_bytes());

            let encoded_part = result
                .strip_prefix("base64:")
                .ok_or_else(|| anyhow::anyhow!("Invalid encoded format"))
                .unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded_part)
                .unwrap();
            assert_eq!(String::from_utf8(decoded).unwrap(), text_with_newlines);
        }
    }

    mod task_execution_phase_tests {
        use super::*;

        #[test]
        fn test_running_state_phase_name() {
            let phase = match TaskState::Running {
                TaskState::Created => "created",
                TaskState::Pending => "pending",
                TaskState::Running => "running",
                TaskState::Cancelling => "cancelling",
                TaskState::Success => "success",
                TaskState::Failed => "failed",
                TaskState::Timeout => "timeout",
                TaskState::Cancelled => "cancelled",
            };
            assert_eq!(phase, "running");
        }

        #[test]
        fn test_all_states_have_phase_names() {
            let states = [
                TaskState::Created,
                TaskState::Pending,
                TaskState::Running,
                TaskState::Cancelling,
                TaskState::Success,
                TaskState::Failed,
                TaskState::Timeout,
                TaskState::Cancelled,
            ];

            for state in states {
                let phase = match state {
                    TaskState::Created => "created",
                    TaskState::Pending => "pending",
                    TaskState::Running => "running",
                    TaskState::Cancelling => "cancelling",
                    TaskState::Success => "success",
                    TaskState::Failed => "failed",
                    TaskState::Timeout => "timeout",
                    TaskState::Cancelled => "cancelled",
                };
                assert!(!phase.is_empty());
            }
        }

        #[test]
        fn test_only_ack_after_final_result_publish_succeeds() {
            let ok_result: Result<()> = Ok(());
            let err_result: Result<()> = Err(anyhow::anyhow!("publish failed").into());

            assert!(TaskManager::should_ack_task_message(&ok_result));
            assert!(!TaskManager::should_ack_task_message(&err_result));
        }
    }

    mod command_building_tests {
        #[test]
        fn test_full_command_with_args() {
            let command = "ls";
            let args = ["-la".to_string(), "/tmp".to_string()];

            let full_command = if args.is_empty() {
                command.to_string()
            } else {
                format!("{} {}", command, args.join(" "))
            };

            assert_eq!(full_command, "ls -la /tmp");
        }

        #[test]
        fn test_full_command_without_args() {
            let command = "pwd";
            let args: Vec<String> = vec![];

            let full_command = if args.is_empty() {
                command.to_string()
            } else {
                format!("{} {}", command, args.join(" "))
            };

            assert_eq!(full_command, "pwd");
        }

        #[test]
        fn test_full_command_single_arg() {
            let command = "echo";
            let args = ["hello".to_string()];

            let full_command = if args.is_empty() {
                command.to_string()
            } else {
                format!("{} {}", command, args.join(" "))
            };

            assert_eq!(full_command, "echo hello");
        }
    }

    mod labels_parsing_tests {
        use std::collections::HashMap;

        #[test]
        fn test_parse_single_label() {
            let args = vec!["env=production".to_string()];
            let mut labels = HashMap::new();

            for arg in &args {
                if let Some((key, value)) = arg.split_once('=') {
                    labels.insert(key.to_string(), value.to_string());
                }
            }

            assert_eq!(labels.get("env"), Some(&"production".to_string()));
        }

        #[test]
        fn test_parse_multiple_labels() {
            let args = vec![
                "env=production".to_string(),
                "region=us-east-1".to_string(),
                "tier=web".to_string(),
            ];
            let mut labels = HashMap::new();

            for arg in &args {
                if let Some((key, value)) = arg.split_once('=') {
                    labels.insert(key.to_string(), value.to_string());
                }
            }

            assert_eq!(labels.len(), 3);
            assert_eq!(labels.get("env"), Some(&"production".to_string()));
            assert_eq!(labels.get("region"), Some(&"us-east-1".to_string()));
            assert_eq!(labels.get("tier"), Some(&"web".to_string()));
        }

        #[test]
        fn test_parse_label_with_equals_in_value() {
            let args = vec!["config=key=value".to_string()];
            let mut labels = HashMap::new();

            for arg in &args {
                if let Some((key, value)) = arg.split_once('=') {
                    labels.insert(key.to_string(), value.to_string());
                }
            }

            assert_eq!(labels.get("config"), Some(&"key=value".to_string()));
        }

        #[test]
        fn test_parse_invalid_label_format() {
            let args = vec!["no-equals-sign".to_string()];
            let mut labels = HashMap::new();

            for arg in &args {
                if let Some((key, value)) = arg.split_once('=') {
                    labels.insert(key.to_string(), value.to_string());
                }
            }

            assert!(labels.is_empty());
        }

        #[test]
        fn test_parse_empty_value() {
            let args = vec!["key=".to_string()];
            let mut labels = HashMap::new();

            for arg in &args {
                if let Some((key, value)) = arg.split_once('=') {
                    labels.insert(key.to_string(), value.to_string());
                }
            }

            assert_eq!(labels.get("key"), Some(&"".to_string()));
        }
    }

    mod cancel_subject_parsing_tests {
        use super::*;

        #[test]
        fn test_parse_valid_cancel_subject() {
            let subject = "tasks.cancel.agent.agent-123.task-456";
            let result = TaskManager::parse_task_id_from_cancel_subject(subject);
            assert!(result.is_ok());
            assert_eq!(result.unwrap().as_str(), "task-456");
        }

        #[test]
        fn test_parse_cancel_subject_with_uuid() {
            let subject = "tasks.cancel.agent.my-agent.550e8400-e29b-41d4-a716-446655440000";
            let result = TaskManager::parse_task_id_from_cancel_subject(subject);
            assert!(result.is_ok());
            assert_eq!(
                result.unwrap().as_str(),
                "550e8400-e29b-41d4-a716-446655440000"
            );
        }

        #[test]
        fn test_parse_invalid_cancel_subject_missing_parts() {
            let subject = "tasks.cancel.agent";
            let result = TaskManager::parse_task_id_from_cancel_subject(subject);
            assert!(result.is_err());
        }

        #[test]
        fn test_parse_invalid_cancel_subject_wrong_prefix() {
            let subject = "tasks.exec.agent.agent-123.task-456";
            let result = TaskManager::parse_task_id_from_cancel_subject(subject);
            assert!(result.is_err());
        }

        #[test]
        fn test_parse_empty_cancel_subject() {
            let subject = "";
            let result = TaskManager::parse_task_id_from_cancel_subject(subject);
            assert!(result.is_err());
        }
    }

    mod running_tasks_map_tests {
        use super::*;

        #[test]
        fn test_insert_and_remove_running_task() {
            let running_tasks: Arc<DashMap<TaskId, CancellationToken>> = Arc::new(DashMap::new());
            let task_id = TaskId::new("task-1");
            let cancel_token = CancellationToken::new();

            running_tasks.insert(task_id.clone(), cancel_token.clone());
            assert_eq!(running_tasks.len(), 1);
            assert!(running_tasks.contains_key(&task_id));

            let removed = running_tasks.remove(&task_id);
            assert!(removed.is_some());
            assert_eq!(running_tasks.len(), 0);
        }

        #[test]
        fn test_cancel_running_task() {
            let running_tasks: Arc<DashMap<TaskId, CancellationToken>> = Arc::new(DashMap::new());
            let task_id = TaskId::new("task-2");
            let cancel_token = CancellationToken::new();

            running_tasks.insert(task_id.clone(), cancel_token.clone());

            if let Some((_id, token)) = running_tasks.remove(&task_id) {
                token.cancel();
                assert!(token.is_cancelled());
            }
        }

        #[test]
        fn test_cancel_nonexistent_task() {
            let running_tasks: Arc<DashMap<TaskId, CancellationToken>> = Arc::new(DashMap::new());
            let task_id = TaskId::new("nonexistent");

            let removed = running_tasks.remove(&task_id);
            assert!(removed.is_none());
        }

        #[test]
        fn test_multiple_running_tasks() {
            let running_tasks = Arc::new(DashMap::new());

            for i in 0..5 {
                let task_id = TaskId::new(&format!("task-{}", i));
                let cancel_token = CancellationToken::new();
                running_tasks.insert(task_id, cancel_token);
            }

            assert_eq!(running_tasks.len(), 5);

            let task_to_cancel = TaskId::new("task-2");
            if let Some((_id, token)) = running_tasks.remove(&task_to_cancel) {
                token.cancel();
            }

            assert_eq!(running_tasks.len(), 4);
        }
    }

    mod cancellation_tracking_tests {
        use super::*;
        use dashmap::DashSet;

        #[test]
        fn test_register_cancel_request_marks_task_and_cancels_running_token() {
            let task_id = TaskId::new("task-cancel");
            let running_tasks: Arc<DashMap<TaskId, CancellationToken>> = Arc::new(DashMap::new());
            let cancelled_tasks: Arc<DashSet<TaskId>> = Arc::new(DashSet::new());
            let token = CancellationToken::new();
            running_tasks.insert(task_id.clone(), token.clone());

            TaskManager::register_cancel_request(&task_id, &running_tasks, &cancelled_tasks);

            assert!(token.is_cancelled());
            assert!(cancelled_tasks.contains(&task_id));
            assert!(!running_tasks.contains_key(&task_id));
        }

        #[test]
        fn test_take_pending_cancellation_consumes_marker_once() {
            let task_id = TaskId::new("task-cancelled-before-start");
            let cancelled_tasks: Arc<DashSet<TaskId>> = Arc::new(DashSet::new());
            cancelled_tasks.insert(task_id.clone());

            assert!(TaskManager::take_pending_cancellation(
                &task_id,
                &cancelled_tasks
            ));
            assert!(!TaskManager::take_pending_cancellation(
                &task_id,
                &cancelled_tasks
            ));
        }
    }

    mod task_idempotency_tests {
        use super::*;

        #[test]
        fn test_cache_completed_execution_stores_terminal_result() {
            let completed_tasks = Arc::new(DashMap::new());
            let completed_task_order = Arc::new(std::sync::Mutex::new(VecDeque::new()));
            let execution = TaskExecution::success(
                TaskId::new("task-1"),
                AgentId::new("agent-1"),
                0,
                "ok".to_string(),
                String::new(),
                1.0,
            );

            TaskManager::cache_completed_execution(
                &execution,
                &completed_tasks,
                &completed_task_order,
            );

            let cached = TaskManager::completed_execution(&TaskId::new("task-1"), &completed_tasks)
                .expect("terminal execution should be cached");
            assert_eq!(cached.state, TaskState::Success);
            assert_eq!(cached.stdout, "ok");
        }

        #[test]
        fn test_cache_completed_execution_ignores_non_terminal_result() {
            let completed_tasks = Arc::new(DashMap::new());
            let completed_task_order = Arc::new(std::sync::Mutex::new(VecDeque::new()));
            let execution =
                TaskExecution::running(TaskId::new("task-running"), AgentId::new("agent-1"));

            TaskManager::cache_completed_execution(
                &execution,
                &completed_tasks,
                &completed_task_order,
            );

            assert!(
                TaskManager::completed_execution(&TaskId::new("task-running"), &completed_tasks)
                    .is_none()
            );
        }

        #[test]
        fn test_cache_completed_execution_evicts_oldest_entries() {
            let completed_tasks = Arc::new(DashMap::new());
            let completed_task_order = Arc::new(std::sync::Mutex::new(VecDeque::new()));

            for i in 0..(COMPLETED_TASK_CACHE_LIMIT + 1) {
                let execution = TaskExecution::success(
                    TaskId::new(&format!("task-{i}")),
                    AgentId::new("agent-1"),
                    0,
                    "ok".to_string(),
                    String::new(),
                    1.0,
                );
                TaskManager::cache_completed_execution(
                    &execution,
                    &completed_tasks,
                    &completed_task_order,
                );
            }

            assert!(
                TaskManager::completed_execution(&TaskId::new("task-0"), &completed_tasks)
                    .is_none()
            );
            assert!(
                TaskManager::completed_execution(
                    &TaskId::new(&format!("task-{}", COMPLETED_TASK_CACHE_LIMIT)),
                    &completed_tasks
                )
                .is_some()
            );
        }
    }

    mod group_multicast_tests {
        use super::*;

        #[test]
        fn test_parse_groups_from_info_splits_group_list() {
            let groups = TaskManager::parse_groups_from_info(&HashMap::from([(
                "__groups".to_string(),
                "web, prod ,canary".to_string(),
            )]));

            assert_eq!(groups, vec!["web", "prod", "canary"]);
        }

        #[test]
        fn test_task_from_group_message_returns_task_for_current_agent() {
            let agent_id = AgentId::new("agent-1");
            let batch_id = oasis_core::core_types::BatchId::generate();
            let task_id = TaskId::generate();
            let group_msg = oasis_core::proto::GroupTaskMsg {
                batch_id: Some(oasis_core::proto::BatchId {
                    value: batch_id.as_str().to_string(),
                }),
                group: "web".to_string(),
                command: "uptime".to_string(),
                args: vec!["--pretty".to_string()],
                timeout_seconds: 30,
                agent_task_ids: HashMap::from([(
                    agent_id.as_str().to_string(),
                    task_id.as_str().to_string(),
                )]),
            };

            let task = TaskManager::task_from_group_message(&agent_id, &group_msg)
                .expect("agent task should exist");

            assert_eq!(task.agent_id, agent_id);
            assert_eq!(task.batch_id.as_str(), batch_id.as_str());
            assert_eq!(task.task_id.as_str(), task_id.as_str());
            assert_eq!(task.command, "uptime");
            assert_eq!(task.args, vec!["--pretty"]);
            assert_eq!(task.timeout_seconds, 30);
            assert_eq!(task.state, TaskState::Pending);
        }

        #[test]
        fn test_task_from_group_message_ignores_other_agents() {
            let current_agent = AgentId::new("agent-1");
            let group_msg = oasis_core::proto::GroupTaskMsg {
                batch_id: Some(oasis_core::proto::BatchId {
                    value: oasis_core::core_types::BatchId::generate()
                        .as_str()
                        .to_string(),
                }),
                group: "web".to_string(),
                command: "uptime".to_string(),
                args: Vec::new(),
                timeout_seconds: 30,
                agent_task_ids: HashMap::from([(
                    "agent-2".to_string(),
                    TaskId::generate().to_string(),
                )]),
            };

            assert!(TaskManager::task_from_group_message(&current_agent, &group_msg).is_none());
        }
    }
}
