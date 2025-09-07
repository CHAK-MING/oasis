use crate::nats_client::NatsClient;
use anyhow::Result;
use async_nats::jetstream;
use futures::StreamExt;
use oasis_core::{
    constants::*,
    core_types::AgentId,
    task_types::{Task, TaskExecution, TaskState},
};
use prost::Message;
use std::process::Stdio;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct TaskManager {
    agent_id: AgentId,
    nats_client: NatsClient,
    shutdown_token: CancellationToken,
}

impl TaskManager {
    pub fn new(
        agent_id: AgentId,
        nats_client: NatsClient,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            agent_id,
            nats_client,
            shutdown_token,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting task manager");

        let unicast_consumer = self.create_unicast_task_consumer().await?;

        let mut unicast_messages = unicast_consumer.messages().await?;

        info!("Task manager started with dual consumers");

        loop {
            tokio::select! {
                // 处理单播任务（独占消费）
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
                // 接收关闭信号
                _ = self.shutdown_token.cancelled() => {
                    info!("Task manager shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// 创建单播任务消费者（独占消费，仅此Agent接收）
    async fn create_unicast_task_consumer(
        &self,
    ) -> Result<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
        let stream = self
            .nats_client
            .jetstream
            .get_stream(JS_STREAM_TASKS)
            .await?;

        // 为此Agent创建专用的消费者，接收单播任务
        let consumer_name = unicast_consumer_name(&self.agent_id);
        let subject = tasks_unicast_subject(&self.agent_id);

        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                filter_subject: subject.clone(),
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                max_deliver: 3,
                ack_wait: std::time::Duration::from_secs(120),
                ..Default::default()
            })
            .await?;

        info!(
            "Created unicast task consumer: {} for subject: {}",
            consumer_name, subject
        );

        Ok(consumer)
    }

    /// 处理任务消息
    async fn process_task_message(&self, msg: jetstream::Message, source: &str) -> Result<()> {
        // 解析任务(这里需要换成 proto)
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

        info!("Processing {} task: {}", source, task.task_id);

        // 发送一个任务正在执行的状态
        let running_execution = TaskExecution::running(task.task_id.clone(), self.agent_id.clone());

        if let Err(e) = self.publish_task_result(&running_execution).await {
            error!("Failed to publish task result: {}", e);
        }

        // 执行任务
        let execution = self.execute_task(&task).await;

        // 发布执行结果
        if let Err(e) = self.publish_task_result(&execution).await {
            error!("Failed to publish task result: {}", e);
        }

        // 确认消息
        msg.ack()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;

        Ok(())
    }

    async fn execute_task(&self, task: &Task) -> TaskExecution {
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
                self.execute_shell_command(task, start_time, start_instant)
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
                TaskExecution {
                    task_id: task.task_id.clone(),
                    agent_id: self.agent_id.clone(),
                    state: TaskState::Success,
                    exit_code: Some(0),
                    stdout: "Labels updated successfully".to_string(),
                    stderr: String::new(),
                    started_at: start_time,
                    finished_at: Some(finish_time),
                    duration_ms: Some(start_instant.elapsed().as_millis() as f64),
                }
            }
            Err(e) => {
                let finish_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                TaskExecution {
                    task_id: task.task_id.clone(),
                    agent_id: self.agent_id.clone(),
                    state: TaskState::Failed,
                    exit_code: Some(1),
                    stdout: String::new(),
                    stderr: format!("Failed to update labels: {}", e),
                    started_at: start_time,
                    finished_at: Some(finish_time),
                    duration_ms: Some(start_instant.elapsed().as_millis() as f64),
                }
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
            .jetstream
            .get_key_value(JS_KV_AGENT_LABELS)
            .await?;
        let key = kv_key_labels(self.agent_id.as_str());

        // TODO：这里也需要换成 proto
        let data = serde_json::to_vec(&new_labels)?;

        kv.put(&key, data.into()).await?;
        info!("Updated agent labels: {:?}", new_labels);

        Ok(())
    }

    async fn execute_shell_command(
        &self,
        task: &Task,
        start_time: i64,
        start_instant: std::time::Instant,
    ) -> TaskExecution {
        info!(
            "Executing shell command: {} {}",
            task.command,
            task.args.join(" ")
        );

        // 构建完整命令
        let full_command = if task.args.is_empty() {
            task.command.clone()
        } else {
            format!("{} {}", task.command, task.args.join(" "))
        };

        // 统一使用 shell 执行
        let output = match tokio::process::Command::new("/bin/sh")
            .args(&["-c", &full_command])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(std::env::vars())
            .output()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                let finish_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                return TaskExecution {
                    task_id: task.task_id.clone(),
                    agent_id: self.agent_id.clone(),
                    state: TaskState::Failed,
                    exit_code: Some(-1),
                    stdout: String::new(),
                    stderr: format!("Command execution failed: {}", e),
                    started_at: start_time,
                    finished_at: Some(finish_time),
                    duration_ms: Some(start_instant.elapsed().as_millis() as f64),
                };
            }
        };

        let finish_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let exit_code = output.status.code().unwrap_or(-1);
        let state = if exit_code == 0 {
            TaskState::Success
        } else {
            TaskState::Failed
        };

        TaskExecution {
            task_id: task.task_id.clone(),
            agent_id: self.agent_id.clone(),
            state,
            exit_code: Some(exit_code),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            started_at: start_time,
            finished_at: Some(finish_time),
            duration_ms: Some(start_instant.elapsed().as_millis() as f64),
        }
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

        let ack = self
            .nats_client
            .jetstream
            .publish_with_headers(subject.clone(), headers, data.into())
            .await?;

        ack.await?;
        info!("Published task result: {}", execution.task_id);

        Ok(())
    }
}
