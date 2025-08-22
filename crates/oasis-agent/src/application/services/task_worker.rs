use crate::config::AgentConfig;
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
    dlq::{DeadLetterEntry, publish_dlq},
    rate_limit::RateLimiterCollection,
    task::{TaskExecution, TaskSpec},
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

pub struct TaskWorker {
    agent: Arc<RwLock<Agent>>,
    config: Arc<RwLock<AgentConfig>>,
    executor: Arc<CommandExecutor>,
    file_handler: Arc<FileApplyHandler>,
    publisher: Arc<NatsPublisher>,
    labels_repo: Arc<NatsAttributesRepository>,
    agent_id: String,
    dlq_js: Arc<async_nats::jetstream::Context>,
    limiters: Arc<RateLimiterCollection>,
}

impl TaskWorker {
    pub fn new(
        agent: Arc<RwLock<Agent>>,
        config: Arc<RwLock<AgentConfig>>,
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
            config,
            executor,
            file_handler,
            publisher,
            labels_repo,
            agent_id,
            dlq_js,
            limiters,
        }
    }

    pub async fn process_message(&self, msg: async_nats::jetstream::Message) -> Result<()> {
        // 反序列化 TaskSpec（旧版结构），走原先路径（含内置 labels 处理逻辑）
        let task_spec: oasis_core::types::TaskSpec = match serde_json::from_slice(&msg.payload) {
            Ok(s) => s,
            Err(e) => {
                // 无法反序列化任务，发送 DLQ 以便后续审计
                match self
                    .send_dlq_minimal("deserialize error", e.to_string(), None)
                    .await
                {
                    Ok(()) => {
                        // DLQ 成功后 ack，防止重复投递死循环
                        if let Err(ae) = msg.ack().await {
                            error!("Failed to ack after DLQ on deserialize: {}", ae);
                        }
                        return Ok(());
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
                        return Ok(());
                    }
                }
            }
        };

        // 内置命令：更新节点标签
        if task_spec.command == constants::CMD_LABELS_UPDATE {
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
                let mut agent_w = self.agent.write().await;
                // 合并到 attributes.labels 中
                for (k, v) in new_labels.iter() {
                    agent_w.attributes.labels.insert(k.clone(), v.clone());
                }
                // 将完整的 attributes 发布到 KV
                let attrs = agent_w.attributes.clone();
                drop(agent_w);
                self.labels_repo
                    .publish_attributes(
                        &oasis_core::types::AgentId::from(self.agent_id.clone()),
                        &attrs,
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
            // 限流 + 退避策略发布结果
            let limiters = self.limiters.clone();
            oasis_core::rate_limit::rate_limited_operation(
                &limiters.task_publish,
                || async {
                    execute_with_backoff(
                        || async {
                            self.publisher
                                .publish_task_result(&result)
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
            .await?;
            if let Err(e) = msg.ack().await {
                error!("Failed to ack message: {}", e);
            }
            return Ok(());
        }

        // 内置命令：文件下发（统一 JSON 参数：从对象存储拉取并原子落地）
        if task_spec.command == constants::CMD_FILE_APPLY {
            #[derive(Deserialize)]
            struct ApplyJson {
                object_name: String,
                destination: String,
                #[allow(dead_code)]
                sha256: Option<String>,
                #[allow(dead_code)]
                size: Option<u64>,
                mode: Option<String>,
                owner: Option<String>, // user:group
                                       // atomic: bool,          // TODO: 当前未使用，考虑移除或实现原子操作
            }

            let payload = task_spec.args.get(0).cloned().unwrap_or_default();
            let cfg: ApplyJson = match serde_json::from_str(&payload) {
                Ok(v) => v,
                Err(e) => {
                    let _ = self
                        .send_dlq(&task_spec, &format!("file-apply invalid json: {}", e))
                        .await;
                    if let Err(ae) = msg.ack().await {
                        error!("Failed to ack message: {}", ae);
                    }
                    return Ok(());
                }
            };

            if cfg.object_name.is_empty() || cfg.destination.is_empty() {
                let _ = self
                    .send_dlq(&task_spec, "file-apply missing object_name or destination")
                    .await;
                if let Err(ae) = msg.ack().await {
                    error!("Failed to ack message: {}", ae);
                }
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
                    let _ = self
                        .send_dlq(&task_spec, &format!("object-store bind failed: {}", e))
                        .await;
                    if let Err(ae) = msg.ack().await {
                        error!("Failed to ack message: {}", ae);
                    }
                    return Ok(());
                }
            };
            let mut obj = match os.get(cfg.object_name.as_str()).await {
                Ok(o) => o,
                Err(e) => {
                    let _ = self
                        .send_dlq(&task_spec, &format!("object not found: {}", e))
                        .await;
                    if let Err(ae) = msg.ack().await {
                        error!("Failed to ack message: {}", ae);
                    }
                    return Ok(());
                }
            };
            use tokio::io::AsyncReadExt as _;
            let mut data = Vec::new();
            if let Err(e) = obj.read_to_end(&mut data).await {
                let _ = self
                    .send_dlq(&task_spec, &format!("object read failed: {}", e))
                    .await;
                if let Err(ae) = msg.ack().await {
                    error!("Failed to ack message: {}", ae);
                }
                return Ok(());
            }

            // 获取允许的根目录列表
            let allowed_roots: Vec<std::path::PathBuf> = {
                let g = self.config.read().await;
                g.security
                    .file_allowed_roots
                    .iter()
                    .map(|s| std::path::PathBuf::from(s))
                    .collect()
            };

            // 解析权限/所有者设置
            let perms = cfg
                .mode
                .as_ref()
                .and_then(|m| u32::from_str_radix(m.trim_start_matches('0'), 8).ok())
                .unwrap_or(0o644);
            let (owner_user, owner_group) = cfg
                .owner
                .as_ref()
                .and_then(|s| s.split_once(':'))
                .map(|(u, g)| (Some(u.to_string()), Some(g.to_string())))
                .unwrap_or((None, None));

            // 构造请求并执行原子落地
            let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
            let req = FileApplyRequest {
                path: cfg.destination,
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
                let _ = self
                    .send_dlq(&task_spec, &format!("file-apply failed: {}", e))
                    .await;
                if let Err(ae) = msg.ack().await {
                    error!("Failed to ack message: {}", ae);
                }
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
            let limiters = self.limiters.clone();
            oasis_core::rate_limit::rate_limited_operation(
                &limiters.task_publish,
                || async {
                    execute_with_backoff(
                        || async {
                            self.publisher
                                .publish_task_result(&result)
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
            .await?;
            if let Err(e) = msg.ack().await {
                error!("Failed to ack message: {}", e);
            }
            return Ok(());
        }

        // 旧版 shell 命令执行路径
        let mut task = Task::new(task_spec);
        task.start()?;
        let start_time = std::time::Instant::now();
        let (exit_code, stdout, stderr) = {
            let cfg = self.config.read().await;
            if !cfg.is_command_allowed(&task.spec.command) {
                // 权限被拒：回报失败结果并 ack，避免无限重投递
                task.fail(format!(
                    "permission denied: command '{}' is not allowed",
                    task.spec.command
                ))?;
                let duration_ms = start_time.elapsed().as_millis() as u64;
                let result = TaskExecution {
                    task_id: task.spec.id.clone(),
                    agent_id: oasis_core::types::AgentId::from(self.agent_id.clone()),
                    stdout: String::new(),
                    stderr: format!(
                        "permission denied: command '{}' is not allowed",
                        task.spec.command
                    ),
                    exit_code: Some(1),
                    timestamp: chrono::Utc::now().timestamp(),
                    duration_ms,
                };
                let limiters = self.limiters.clone();
                let _ = oasis_core::rate_limit::rate_limited_operation(
                    &limiters.task_publish,
                    || async {
                        execute_with_backoff(
                            || async {
                                self.publisher
                                    .publish_task_result(&result)
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
                if let Err(e) = msg.ack().await {
                    error!("Failed to ack message: {}", e);
                }
                return Ok(());
            }
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
                    let limiters = self.limiters.clone();
                    let _ = oasis_core::rate_limit::rate_limited_operation(
                        &limiters.task_publish,
                        || async {
                            execute_with_backoff(
                                || async {
                                    self.publisher
                                        .publish_task_result(&result)
                                        .await
                                        .map_err(|e| e.to_string())
                                },
                                network_publish_backoff(),
                            )
                            .await
                            .map_err(|e| {
                                crate::error::CoreError::Internal {
                                    message: e.to_string(),
                                }
                            })
                        },
                        None,
                        "results.publish",
                    )
                    .await;
                    if let Err(e) = msg.ack().await {
                        error!("Failed to ack message: {}", e);
                    }
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
        let limiters = self.limiters.clone();
        oasis_core::rate_limit::rate_limited_operation(
            &limiters.task_publish,
            || async {
                execute_with_backoff(
                    || async {
                        self.publisher
                            .publish_task_result(&result)
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
        .await?;
        if let Err(e) = msg.ack().await {
            error!("Failed to ack message: {}", e);
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
        publish_dlq(&self.dlq_js, &entry)
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })
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
