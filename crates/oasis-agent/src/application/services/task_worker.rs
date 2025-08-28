use crate::domain::{agent::Agent, task::Task};
use crate::error::{CoreError, Result};
use crate::infrastructure::{
    nats::{attributes_repository::NatsAttributesRepository, publisher::NatsPublisher},
    system::{
        executor::CommandExecutor,
        file_apply_handler::{FileApplyHandler, FileApplyRequest},
    },
};
use base64::Engine as _;
use oasis_core::{
    backoff::{execute_with_backoff, network_publish_backoff},
    constants,
    dlq::{DeadLetterEntry, handle_dead_letter, publish_dlq},
    rate_limit::RateLimiterCollection,
    task::{TaskExecution, TaskSpec},
    types::TaskId,
};
use prost::Message;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

pub struct TaskWorker {
    agent: Arc<RwLock<Agent>>,
    executor: Arc<CommandExecutor>,
    file_handler: Arc<FileApplyHandler>,
    publisher: Arc<NatsPublisher>,
    labels_repo: Arc<NatsAttributesRepository>,
    agent_id: String,
    dlq_js: Arc<async_nats::jetstream::Context>,
    dlq_kv: Arc<RwLock<Option<async_nats::jetstream::kv::Store>>>,
    limiters: Arc<RateLimiterCollection>,
}

impl TaskWorker {
    pub fn new(
        agent: Arc<RwLock<Agent>>,
        executor: Arc<CommandExecutor>,
        file_handler: Arc<FileApplyHandler>,
        publisher: Arc<NatsPublisher>,
        labels_repo: Arc<NatsAttributesRepository>,
        agent_id: String,
        dlq_js: Arc<async_nats::jetstream::Context>,
        limiters: Arc<RateLimiterCollection>,
    ) -> Self {
        Self {
            agent,
            executor,
            file_handler,
            publisher,
            labels_repo,
            agent_id,
            dlq_js,
            dlq_kv: Arc::new(RwLock::new(None)),
            limiters,
        }
    }

    /// 安全发布任务结果（包含限流+重试逻辑）
    async fn publish_task_result_safely(&self, result: &TaskExecution) -> Result<()> {
        // 检查结果是否已发送过
        {
            let agent = self.agent.read().await;
            if agent.is_result_sent(&result.task_id) {
                tracing::debug!(task_id = %result.task_id, "Result already sent, skipping");
                return Ok(());
            }
        }

        let limiters = self.limiters.clone();
        let publish_result = oasis_core::rate_limit::rate_limited_operation(
            &limiters.task_publish,
            || async {
                execute_with_backoff(
                    || async {
                        self.publisher
                            .publish_task_result(result)
                            .await
                            .map_err(|e| e.to_string())
                    },
                    network_publish_backoff(),
                )
                .await
                .map_err(|e| crate::error::CoreError::Internal {
                    message: e.to_string(),
                })
            },
            None,
            "results.publish",
        )
        .await;

        // 如果发布成功，标记结果为已发送
        if publish_result.is_ok() {
            let mut agent = self.agent.write().await;
            agent.mark_result_sent(result.task_id.clone());
        }

        publish_result
    }

    /// 安全确认消息（包含错误日志）
    async fn ack_message_safely(&self, msg: &async_nats::jetstream::Message) {
        if let Err(e) = msg.ack().await {
            error!("Failed to ack message: {}", e);
        }
    }

    /// 发送DLQ并确认消息
    async fn send_dlq_and_ack(
        &self,
        task_spec: &TaskSpec,
        error_msg: &str,
        msg: &async_nats::jetstream::Message,
    ) -> Result<()> {
        let _ = self.send_dlq(task_spec, error_msg).await;
        self.ack_message_safely(msg).await;
        Ok(())
    }

    /// 反序列化任务规格
    async fn deserialize_task_spec(
        &self,
        msg: &async_nats::jetstream::Message,
    ) -> Result<oasis_core::types::TaskSpec> {
        match oasis_core::proto::TaskSpecMsg::decode(msg.payload.as_ref()) {
            Ok(proto) => Ok(oasis_core::types::TaskSpec::from(&proto)),
            Err(e) => {
                // 无法反序列化任务，发送 DLQ 以便后续审计
                match self
                    .send_dlq_minimal("deserialize error", e.to_string(), None)
                    .await
                {
                    Ok(()) => {
                        // DLQ 成功后 ack，防止重复投递死循环
                        self.ack_message_safely(msg).await;
                        Err(crate::error::CoreError::Internal {
                            message: "Task deserialization failed".to_string(),
                        })
                    }
                    Err(dlq_err) => {
                        // DLQ 失败：严重错误，NAK 以尽快重投递
                        error!("Failed to send DLQ for deserialize error: {}", dlq_err);
                        if let Err(ne) = msg
                            .ack_with(async_nats::jetstream::AckKind::Nak(None))
                            .await
                        {
                            error!("Failed to NAK message after DLQ failure: {}", ne);
                        }
                        Err(crate::error::CoreError::Internal {
                            message: "Both task deserialization and DLQ failed".to_string(),
                        })
                    }
                }
            }
        }
    }

    /// 处理标签更新命令
    async fn handle_labels_update(
        &self,
        task_spec: &oasis_core::types::TaskSpec,
        msg: &async_nats::jetstream::Message,
    ) -> Result<()> {
        let mut new_labels = std::collections::HashMap::new();
        if let Some(first) = task_spec.args.get(0) {
            if first.trim_start().starts_with('{') {
                new_labels = serde_json::from_str(first)
                    .map_err(|e| anyhow::anyhow!("Invalid labels JSON: {}", e))?;
            } else {
                for arg in &task_spec.args {
                    if let Some((k, v)) = arg.split_once('=') {
                        let key = k.trim();
                        let val = v.trim();
                        if !key.is_empty() && !val.is_empty() {
                            new_labels.insert(key.to_string(), val.to_string());
                        }
                    }
                }
            }
        }

        {
            // 直接发布 labels（Agent 模型不再持有 info）
            self.labels_repo
                .publish_attributes(
                    &oasis_core::types::AgentId::from(self.agent_id.clone()),
                    &new_labels,
                )
                .await?;
        }

        let result = TaskExecution {
            task_id: task_spec.id.clone(),
            agent_id: oasis_core::types::AgentId::from(self.agent_id.clone()),
            stdout: serde_json::to_string(&new_labels).unwrap_or_default(),
            stderr: String::new(),
            exit_code: Some(0),
            timestamp: chrono::Utc::now().timestamp(),
            duration_ms: 0,
        };
        self.publish_task_result_safely(&result).await?;
        self.ack_message_safely(msg).await;
        Ok(())
    }

    /// 处理文件应用命令
    async fn handle_file_apply(
        &self,
        task_spec: &oasis_core::types::TaskSpec,
        msg: &async_nats::jetstream::Message,
    ) -> Result<()> {
        // 在长 I/O 期间发送 Progress ACK（轻量）
        let msg_for_progress = msg.clone();
        let progress_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                ticker.tick().await;
                let _ = msg_for_progress
                    .ack_with(async_nats::jetstream::AckKind::Progress)
                    .await;
            }
        });
        // 解析为 Protobuf ApplyFileRequest（base64 编码的 protobuf）
        let payload_b64 = task_spec.args.get(0).cloned().unwrap_or_default();
        let payload_bytes = match base64::engine::general_purpose::STANDARD.decode(payload_b64) {
            Ok(b) => b,
            Err(e) => {
                self.send_dlq_and_ack(task_spec, &format!("file-apply invalid base64: {}", e), msg)
                    .await?;
                return Ok(());
            }
        };
        let apply_req = match oasis_core::proto::ApplyFileRequest::decode(payload_bytes.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                self.send_dlq_and_ack(
                    task_spec,
                    &format!("file-apply invalid protobuf: {}", e),
                    msg,
                )
                .await?;
                return Ok(());
            }
        };
        if apply_req.object_name.is_empty() || apply_req.destination_path.is_empty() {
            self.send_dlq_and_ack(
                task_spec,
                "file-apply missing object_name or destination",
                msg,
            )
            .await?;
            return Ok(());
        }

        // 从对象存储拉取文件
        let os = match self
            .dlq_js
            .get_object_store(oasis_core::JS_OBJ_ARTIFACTS)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                self.send_dlq_and_ack(task_spec, &format!("object-store bind failed: {}", e), msg)
                    .await?;
                return Ok(());
            }
        };
        let mut obj = match os.get(apply_req.object_name.as_str()).await {
            Ok(o) => o,
            Err(e) => {
                self.send_dlq_and_ack(task_spec, &format!("object not found: {}", e), msg)
                    .await?;
                return Ok(());
            }
        };
        use tokio::io::AsyncReadExt as _;
        let mut data = Vec::new();
        if let Err(e) = obj.read_to_end(&mut data).await {
            self.send_dlq_and_ack(task_spec, &format!("object read failed: {}", e), msg)
                .await?;
            return Ok(());
        }

        // TODO：获取允许的根目录列表（硬编码）
        let allowed_roots: Vec<std::path::PathBuf> = vec![
            std::path::PathBuf::from("/tmp"),
            std::path::PathBuf::from("/var/tmp"),
        ];

        // 解析权限/所有者设置
        let perms = u32::from_str_radix(apply_req.mode.trim_start_matches('0'), 8).unwrap_or(0o644);
        let (owner_user, owner_group) = apply_req
            .owner
            .split_once(':')
            .map(|(u, g)| (Some(u.to_string()), Some(g.to_string())))
            .unwrap_or((None, None));

        // 构造请求并执行原子落地
        let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
        let req = FileApplyRequest {
            path: apply_req.destination_path,
            content_b64: b64,
            permissions: perms,
            owner: owner_user,
            group: owner_group,
        };
        if let Err(e) = self
            .file_handler
            .apply_with_roots(&req, &allowed_roots)
            .await
        {
            self.send_dlq_and_ack(task_spec, &format!("file-apply failed: {}", e), msg)
                .await?;
            return Ok(());
        }

        let result = TaskExecution {
            task_id: task_spec.id.clone(),
            agent_id: oasis_core::types::AgentId::from(self.agent_id.clone()),
            stdout: String::from("File applied"),
            stderr: String::new(),
            exit_code: Some(0),
            timestamp: chrono::Utc::now().timestamp(),
            duration_ms: 0,
        };
        self.publish_task_result_safely(&result).await?;
        progress_handle.abort();
        self.ack_message_safely(msg).await;
        Ok(())
    }

    /// 处理Shell命令执行
    async fn handle_shell_command(
        &self,
        task_spec: &oasis_core::types::TaskSpec,
        msg: &async_nats::jetstream::Message,
    ) -> Result<()> {
        let mut task = Task::new(task_spec.clone());
        task.start()?;
        let start_time = std::time::Instant::now();

        // 在长任务期间周期性发送 Progress ACK，防止处理中被重投递
        let msg_for_progress = msg.clone();
        let progress_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                ticker.tick().await;
                let _ = msg_for_progress
                    .ack_with(async_nats::jetstream::AckKind::Progress)
                    .await;
            }
        });

        let (exit_code, stdout, stderr) = {
            match self
                .executor
                .execute(&task.spec.command, &task.spec.args, &task.spec.env)
                .await
            {
                Ok(t) => t,
                Err(e) => {
                    // 执行失败：回报失败结果并 ack，避免无限重投递
                    task.fail(format!("execute failed: {}", e))?;
                    let duration_ms = start_time.elapsed().as_millis() as u64;
                    let result = TaskExecution {
                        task_id: task.spec.id.clone(),
                        agent_id: oasis_core::types::AgentId::from(self.agent_id.clone()),
                        stdout: String::new(),
                        stderr: format!("execute failed: {}", e),
                        exit_code: Some(1),
                        timestamp: chrono::Utc::now().timestamp(),
                        duration_ms,
                    };
                    let _ = self.publish_task_result_safely(&result).await;
                    self.ack_message_safely(msg).await;
                    return Ok(());
                }
            }
        };

        let duration_ms = start_time.elapsed().as_millis() as u64;
        // 只有在任务还没有失败的情况下才更新状态
        if exit_code == 0 {
            task.complete(exit_code)?;
        } else {
            // 只有在任务状态不是 Failed 时才调用 fail 方法
            if !matches!(task.status, crate::domain::task::TaskStatus::Failed { .. }) {
                task.fail(format!("Command failed with exit code: {}", exit_code))?;
            }
        }

        let result = TaskExecution {
            task_id: task.spec.id.clone(),
            agent_id: oasis_core::types::AgentId::from(self.agent_id.clone()),
            stdout,
            stderr,
            exit_code: Some(exit_code),
            timestamp: chrono::Utc::now().timestamp(),
            duration_ms,
        };
        self.publish_task_result_safely(&result).await?;
        // 停止进度 ACK 发送
        progress_handle.abort();
        self.ack_message_safely(msg).await;
        Ok(())
    }

    pub async fn process_message(&self, msg: async_nats::jetstream::Message) -> Result<()> {
        let task_spec = match self.deserialize_task_spec(&msg).await {
            Ok(spec) => spec,
            Err(_) => return Ok(()), // 错误已在deserialize_task_spec中处理
        };

        // 检查任务是否已处理过
        {
            let agent = self.agent.read().await;
            if agent.is_task_processed(&task_spec.id) {
                tracing::info!(task_id = %task_spec.id, "Task already processed, skipping");
                self.ack_message_safely(&msg).await;
                return Ok(());
            }
        }

        // 标记任务为已处理（在开始处理前）
        {
            let mut agent = self.agent.write().await;
            agent.mark_task_processed(task_spec.id.clone());
        }

        // 保存任务开始状态到 KV（可选，用于恢复）
        let _ = self.save_task_state(&task_spec.id, "started").await;

        match task_spec.command.as_str() {
            constants::CMD_LABELS_UPDATE => self.handle_labels_update(&task_spec, &msg).await,
            constants::CMD_FILE_APPLY => self.handle_file_apply(&task_spec, &msg).await,
            _ => self.handle_shell_command(&task_spec, &msg).await,
        }
    }

    /// 保存任务状态到 KV 存储（用于恢复）
    async fn save_task_state(&self, task_id: &TaskId, state: &str) -> Result<()> {
        let key = oasis_core::constants::kv_key_task_state(&self.agent_id, task_id.as_str());
        let state_data = serde_json::json!({
            "agent_id": self.agent_id,
            "task_id": task_id.as_str(),
            "state": state,
            "timestamp": chrono::Utc::now().timestamp()
        });

        if let Ok(kv) = self
            .dlq_js
            .get_key_value(oasis_core::JS_KV_TASK_STATE)
            .await
        {
            let _ = kv
                .put(
                    &key,
                    serde_json::to_string(&state_data)
                        .unwrap_or_default()
                        .into(),
                )
                .await;
        }

        Ok(())
    }

    async fn send_dlq(&self, task_spec: &TaskSpec, error_msg: &str) -> Result<()> {
        let entry = DeadLetterEntry::new(
            task_spec.clone(),
            error_msg.to_string(),
            oasis_core::types::AgentId::from(self.agent_id.clone()),
            0,
        );
        // 确保 KV 绑定（惰性获取）
        {
            let mut guard = self.dlq_kv.write().await;
            if guard.is_none() {
                match self.dlq_js.get_key_value(oasis_core::JS_KV_DLQ).await {
                    Ok(kv) => {
                        *guard = Some(kv);
                    }
                    Err(e) => {
                        // 如果 KV 不存在或绑定失败，回退到仅发布 DLQ
                        tracing::warn!("DLQ KV not available: {}. Fallback to publish only.", e);
                    }
                }
            }
        }

        // 限流后优先写入 KV + 发布通知；如果 KV 不可用则仅发布
        if let Some(kv) = self.dlq_kv.read().await.clone() {
            oasis_core::rate_limit::rate_limited_operation(
                &self.limiters.nats,
                || async {
                    handle_dead_letter(&kv, &self.dlq_js, &entry)
                        .await
                        .map_err(|e| CoreError::Nats {
                            message: e.to_string(),
                        })
                },
                None,
                "dlq.handle",
            )
            .await
        } else {
            oasis_core::rate_limit::rate_limited_operation(
                &self.limiters.nats,
                || async {
                    publish_dlq(&self.dlq_js, &entry)
                        .await
                        .map_err(|e| CoreError::Nats {
                            message: e.to_string(),
                        })
                },
                None,
                "dlq.publish",
            )
            .await
        }
    }

    async fn send_dlq_minimal(
        &self,
        prefix: &str,
        details: String,
        task: Option<TaskSpec>,
    ) -> Result<()> {
        let task_spec = task.unwrap_or_else(|| {
            oasis_core::task::TaskSpec::for_agents(
                oasis_core::types::TaskId::from("unknown".to_string()),
                prefix.to_string(),
                Vec::new(),
            )
            .with_timeout(0)
        });
        self.send_dlq(&task_spec, &details).await
    }
}
