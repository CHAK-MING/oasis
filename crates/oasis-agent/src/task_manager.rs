use crate::nats_client::NatsClient;
use async_nats::jetstream;
use base64::Engine;
use futures::StreamExt;
use oasis_core::{
    constants::*,
    core_types::AgentId,
    error::Result,
    shutdown::{ExecutionError, execute_process_with_cancellation},
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

        let timeout_duration = std::time::Duration::from_secs(task.timeout_seconds as u64);
        let result = execute_process_with_cancellation(
            child,
            self.shutdown_token.clone(),
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

                TaskExecution {
                    task_id: task.task_id.clone(),
                    agent_id: self.agent_id.clone(),
                    state,
                    exit_code: Some(exit_code),
                    stdout: Self::encode_output(&output.stdout),
                    stderr: Self::encode_output(&output.stderr),
                    started_at: start_time,
                    finished_at: Some(finish_time),
                    duration_ms: Some(start_instant.elapsed().as_millis() as f64),
                }
            }
            Err(ExecutionError::Cancelled) => TaskExecution {
                task_id: task.task_id.clone(),
                agent_id: self.agent_id.clone(),
                state: TaskState::Cancelled,
                exit_code: Some(-1),
                stdout: String::new(),
                stderr: "Command cancelled by shutdown signal".to_string(),
                started_at: start_time,
                finished_at: Some(finish_time),
                duration_ms: Some(start_instant.elapsed().as_millis() as f64),
            },
            Err(ExecutionError::Timeout(_)) => TaskExecution {
                task_id: task.task_id.clone(),
                agent_id: self.agent_id.clone(),
                state: TaskState::Timeout,
                exit_code: Some(-1),
                stdout: String::new(),
                stderr: format!("Command timed out after {} seconds", task.timeout_seconds),
                started_at: start_time,
                finished_at: Some(finish_time),
                duration_ms: Some(start_instant.elapsed().as_millis() as f64),
            },
            Err(ExecutionError::Failed(e)) => TaskExecution {
                task_id: task.task_id.clone(),
                agent_id: self.agent_id.clone(),
                state: TaskState::Failed,
                exit_code: Some(-1),
                stdout: String::new(),
                stderr: format!("Command execution failed: {}", e),
                started_at: start_time,
                finished_at: Some(finish_time),
                duration_ms: Some(start_instant.elapsed().as_millis() as f64),
            },
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

#[cfg(test)]
mod tests {
    use super::*;

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
                    TaskState::Success => "success",
                    TaskState::Failed => "failed",
                    TaskState::Timeout => "timeout",
                    TaskState::Cancelled => "cancelled",
                };
                assert!(!phase.is_empty());
            }
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
}
