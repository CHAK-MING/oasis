use async_nats::HeaderMap;
use async_trait::async_trait;
use futures::StreamExt;
use oasis_core::{error::CoreError, types::TaskExecution};

use crate::application::ports::repositories::{ResultConsumer, TaskRepository};
use crate::domain::models::task::{Task, TaskResult};
use crate::infrastructure::persistence::utils as persist;

/// 任务仓储实现 - 基于JetStream
pub struct NatsTaskRepository {
    jetstream: async_nats::jetstream::Context,
}

impl NatsTaskRepository {
    pub fn new(jetstream: async_nats::jetstream::Context) -> Self {
        Self { jetstream }
    }

    /// 确保任务流存在
    async fn ensure_task_stream(&self) -> Result<(), CoreError> {
        // 首先确保 DLQ 流存在
        self.ensure_dlq_stream().await?;

        if self
            .jetstream
            .get_stream(oasis_core::JS_STREAM_TASKS)
            .await
            .is_err()
        {
            let cfg = async_nats::jetstream::stream::Config {
                name: oasis_core::JS_STREAM_TASKS.to_string(),
                subjects: vec![
                    "tasks.exec.default".to_string(),
                    "tasks.exec.agent.>".to_string(),
                    "tasks.exec.group.>".to_string(),
                ],
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                max_age: std::time::Duration::from_secs(3600), // 1小时
                duplicate_window: std::time::Duration::from_secs(30),
                num_replicas: 1,
                storage: async_nats::jetstream::stream::StorageType::File,
                max_messages: 10000,
                max_bytes: 1024 * 1024 * 1024, // 1GB
                ..Default::default()
            };

            // 带重试的流创建
            let mut last_error = None;
            for attempt in 0..3 {
                match self.jetstream.create_stream(cfg.clone()).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        last_error = Some(e);
                        if attempt < 2 {
                            let backoff = oasis_core::backoff::network_publish_backoff();
                            let delay = oasis_core::backoff::delay_for_attempt(&backoff, attempt);
                            tracing::warn!(
                                attempt = attempt + 1,
                                error = %last_error.as_ref().unwrap(),
                                delay_ms = delay.as_millis(),
                                "Retrying task stream creation"
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }

            return Err(CoreError::Nats {
                message: format!(
                    "Failed to create task stream after retries: {}",
                    last_error.unwrap()
                ),
            });
        }
        Ok(())
    }

    /// 确保 DLQ 流存在
    async fn ensure_dlq_stream(&self) -> Result<(), CoreError> {
        if self
            .jetstream
            .get_stream(oasis_core::JS_STREAM_TASKS_DLQ)
            .await
            .is_err()
        {
            let cfg = async_nats::jetstream::stream::Config {
                name: oasis_core::JS_STREAM_TASKS_DLQ.to_string(),
                subjects: vec![oasis_core::JS_STREAM_TASKS_DLQ.to_string()],
                retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
                max_age: std::time::Duration::from_secs(24 * 3600), // 24小时
                num_replicas: 1,
                storage: async_nats::jetstream::stream::StorageType::File,
                max_messages: 1000,
                max_bytes: 100 * 1024 * 1024, // 100MB
                ..Default::default()
            };

            // 带重试的流创建
            let mut last_error = None;
            for attempt in 0..3 {
                match self.jetstream.create_stream(cfg.clone()).await {
                    Ok(_) => {
                        tracing::info!("DLQ stream created successfully");
                        return Ok(());
                    }
                    Err(e) => {
                        last_error = Some(e);
                        if attempt < 2 {
                            let backoff = oasis_core::backoff::network_publish_backoff();
                            let delay = oasis_core::backoff::delay_for_attempt(&backoff, attempt);
                            tracing::warn!(
                                attempt = attempt + 1,
                                error = %last_error.as_ref().unwrap(),
                                delay_ms = delay.as_millis(),
                                "Retrying DLQ stream creation"
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }

            return Err(CoreError::Nats {
                message: format!(
                    "Failed to create DLQ stream after retries: {}",
                    last_error.unwrap()
                ),
            });
        }
        Ok(())
    }

    /// 确保结果流存在
    async fn ensure_result_stream(&self) -> Result<(), CoreError> {
        if self
            .jetstream
            .get_stream(oasis_core::JS_STREAM_RESULTS)
            .await
            .is_err()
        {
            let cfg = async_nats::jetstream::stream::Config {
                name: oasis_core::JS_STREAM_RESULTS.to_string(),
                subjects: vec!["results.>".to_string()],
                retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
                max_age: std::time::Duration::from_secs(24 * 3600), // 24小时
                num_replicas: 1,
                storage: async_nats::jetstream::stream::StorageType::File,
                max_messages: 100000,
                max_bytes: 1024 * 1024 * 1024, // 1GB
                ..Default::default()
            };

            // 带重试的流创建
            let mut last_error = None;
            for attempt in 0..3 {
                match self.jetstream.create_stream(cfg.clone()).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        last_error = Some(e);
                        if attempt < 2 {
                            let backoff = oasis_core::backoff::network_publish_backoff();
                            let delay = oasis_core::backoff::delay_for_attempt(&backoff, attempt);
                            tracing::warn!(
                                attempt = attempt + 1,
                                error = %last_error.as_ref().unwrap(),
                                delay_ms = delay.as_millis(),
                                "Retrying result stream creation"
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }

            return Err(CoreError::Nats {
                message: format!(
                    "Failed to create result stream after retries: {}",
                    last_error.unwrap()
                ),
            });
        }
        Ok(())
    }

    /// 确保任务KV存储存在
    async fn ensure_task_kv_store(&self) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        match self.jetstream.get_key_value("tasks").await {
            Ok(store) => Ok(store),
            Err(_) => {
                let cfg = async_nats::jetstream::kv::Config {
                    bucket: "tasks".to_string(),
                    description: "Task state storage".to_string(),
                    max_value_size: 1024 * 1024, // 1MB
                    history: 10,
                    max_age: std::time::Duration::from_secs(24 * 3600), // 24小时
                    max_bytes: 1024 * 1024 * 1024,                      // 1GB
                    storage: async_nats::jetstream::stream::StorageType::File,
                    num_replicas: 1,
                    ..Default::default()
                };

                // 带重试的KV存储创建
                let mut last_error = None;
                for attempt in 0..3 {
                    match self.jetstream.create_key_value(cfg.clone()).await {
                        Ok(store) => return Ok(store),
                        Err(e) => {
                            last_error = Some(e);
                            if attempt < 2 {
                                let backoff = oasis_core::backoff::kv_operations_backoff();
                                let delay =
                                    oasis_core::backoff::delay_for_attempt(&backoff, attempt);
                                tracing::warn!(
                                    attempt = attempt + 1,
                                    error = %last_error.as_ref().unwrap(),
                                    delay_ms = delay.as_millis(),
                                    "Retrying KV store creation"
                                );
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                }

                Err(CoreError::Nats {
                    message: format!(
                        "Failed to create KV store after retries: {}",
                        last_error.unwrap()
                    ),
                })
            }
        }
    }
}

#[async_trait]
impl TaskRepository for NatsTaskRepository {
    async fn create(&self, task: Task) -> Result<String, CoreError> {
        let kv = self.ensure_task_kv_store().await?;
        let key = format!("task:{}", task.id);

        let task_data = persist::to_json_vec(&task)?;

        kv.put(&key, task_data.into())
            .await
            .map_err(persist::map_nats_err)?;

        Ok(task.id.to_string())
    }

    async fn get_result(
        &self,
        task_id: &str,
        agent_id: &str,
    ) -> Result<Option<TaskResult>, CoreError> {
        self.ensure_result_stream().await?;

        // 创建临时消费者来获取特定结果（避免资源泄漏）
        let filter_subject = format!("results.{}.{}", task_id, agent_id);

        let consumer = self
            .jetstream
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: None, // 使用临时消费者，避免资源泄漏
                    filter_subject: filter_subject.clone(),
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::LastPerSubject,
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    max_deliver: 1,
                    ..Default::default()
                },
                oasis_core::JS_STREAM_RESULTS,
            )
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to create consumer: {}", e),
            })?;

        // 获取结果
        let fetch_result = consumer
            .fetch()
            .max_messages(1)
            .expires(std::time::Duration::from_millis(100))
            .messages()
            .await;

        match fetch_result {
            Ok(mut batch) => {
                tracing::info!("Fetching messages for subject: {}", filter_subject);
                if let Some(Ok(msg)) = batch.next().await {
                    if let Ok(task_execution) =
                        serde_json::from_slice::<TaskExecution>(&msg.payload)
                    {
                        // 将 TaskExecution 转换为 TaskResult
                        let task_result = TaskResult {
                            task_id: task_execution.task_id.clone(),
                            agent_id: task_execution.agent_id.clone(),
                            status: match task_execution.exit_code {
                                Some(0) => crate::domain::models::task::TaskStatus::Completed {
                                    exit_code: 0,
                                },
                                Some(code) => crate::domain::models::task::TaskStatus::Completed {
                                    exit_code: code,
                                },
                                None => crate::domain::models::task::TaskStatus::Failed {
                                    error: "No exit code".to_string(),
                                },
                            },
                            stdout: task_execution.stdout,
                            stderr: task_execution.stderr,
                            duration_ms: task_execution.duration_ms,
                            timestamp: task_execution.timestamp,
                        };
                        let _ = msg.ack().await;
                        return Ok(Some(task_result));
                    }
                }
                tracing::info!("No messages found for subject: {}", filter_subject);
                Ok(None)
            }
            Err(_) => Ok(None),
        }
    }

    async fn get_results_batch(
        &self,
        task_agent_pairs: &[(String, String)],
    ) -> Result<Vec<Option<TaskResult>>, CoreError> {
        self.ensure_result_stream().await?;

        use futures::stream::{self, StreamExt as _};

        let max_concurrent = 10usize;
        let js = self.jetstream.clone();

        let stream =
            stream::iter(task_agent_pairs.iter().cloned()).map(move |(task_id, agent_id)| {
                let js = js.clone();
                async move {
                    let filter_subject = format!("results.{}.{}", task_id, agent_id);
                    let consumer = match js
                        .create_consumer_on_stream(
                            async_nats::jetstream::consumer::pull::Config {
                                durable_name: None,
                                filter_subject: filter_subject.clone(),
                                deliver_policy:
                                    async_nats::jetstream::consumer::DeliverPolicy::LastPerSubject,
                                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                                max_deliver: 1,
                                ..Default::default()
                            },
                            oasis_core::JS_STREAM_RESULTS,
                        )
                        .await
                    {
                        Ok(c) => c,
                        Err(_) => return Ok(None),
                    };

                    let fetch_result = consumer
                        .fetch()
                        .max_messages(1)
                        .expires(std::time::Duration::from_millis(100))
                        .messages()
                        .await;

                    match fetch_result {
                        Ok(mut batch) => {
                            if let Some(Ok(msg)) = batch.next().await {
                                if let Ok(task_execution) =
                                    serde_json::from_slice::<TaskExecution>(&msg.payload)
                                {
                                    // 将 TaskExecution 转换为 TaskResult
                                    let task_result = TaskResult {
                                        task_id: task_execution.task_id.clone(),
                                        agent_id: task_execution.agent_id.clone(),
                                        status: match task_execution.exit_code {
                                            Some(0) => {
                                                crate::domain::models::task::TaskStatus::Completed {
                                                    exit_code: 0,
                                                }
                                            }
                                            Some(code) => {
                                                crate::domain::models::task::TaskStatus::Completed {
                                                    exit_code: code,
                                                }
                                            }
                                            None => {
                                                crate::domain::models::task::TaskStatus::Failed {
                                                    error: "No exit code".to_string(),
                                                }
                                            }
                                        },
                                        stdout: task_execution.stdout,
                                        stderr: task_execution.stderr,
                                        duration_ms: task_execution.duration_ms,
                                        timestamp: task_execution.timestamp,
                                    };
                                    let _ = msg.ack().await;
                                    return Ok(Some(task_result));
                                }
                            }
                            Ok(None)
                        }
                        Err(_) => Ok(None),
                    }
                }
            });

        let results: Vec<Option<TaskResult>> = stream
            .buffer_unordered(max_concurrent)
            .map(|res: Result<Option<TaskResult>, CoreError>| match res {
                Ok(v) => v,
                Err(_) => None,
            })
            .collect()
            .await;

        Ok(results)
    }

    async fn stream_results(&self, task_id: &str) -> Result<Vec<TaskResult>, CoreError> {
        self.ensure_result_stream().await?;

        // 创建临时消费者来获取所有结果（避免资源泄漏）
        let filter_subject = format!("results.{}.>", task_id);

        let consumer = self
            .jetstream
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: None, // 使用临时消费者，避免资源泄漏
                    filter_subject: filter_subject.clone(),
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                oasis_core::JS_STREAM_RESULTS,
            )
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to create consumer: {}", e),
            })?;

        let mut results = Vec::new();
        let mut batch = consumer
            .fetch()
            .max_messages(100)
            .expires(std::time::Duration::from_millis(2000))
            .messages()
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to fetch messages: {}", e),
            })?;

        while let Some(Ok(msg)) = batch.next().await {
            if let Ok(task_execution) = serde_json::from_slice::<TaskExecution>(&msg.payload) {
                tracing::info!(
                    "Successfully deserialized TaskExecution: {:?}",
                    task_execution
                );
                // 将 TaskExecution 转换为 TaskResult
                let task_result = TaskResult {
                    task_id: task_execution.task_id.clone(),
                    agent_id: task_execution.agent_id.clone(),
                    status: match task_execution.exit_code {
                        Some(0) => {
                            crate::domain::models::task::TaskStatus::Completed { exit_code: 0 }
                        }
                        Some(code) => {
                            crate::domain::models::task::TaskStatus::Completed { exit_code: code }
                        }
                        None => crate::domain::models::task::TaskStatus::Failed {
                            error: "No exit code".to_string(),
                        },
                    },
                    stdout: task_execution.stdout,
                    stderr: task_execution.stderr,
                    duration_ms: task_execution.duration_ms,
                    timestamp: task_execution.timestamp,
                };
                results.push(task_result);
            } else {
                tracing::warn!(
                    "Failed to deserialize message as TaskExecution, payload: {:?}",
                    String::from_utf8_lossy(&msg.payload)
                );
            }
            let _ = msg.ack().await;
        }

        Ok(results)
    }

    async fn create_result_consumer(
        &self,
        task_id: &str,
    ) -> Result<Box<dyn ResultConsumer>, CoreError> {
        self.ensure_result_stream().await?;

        // 创建消费者配置
        let consumer_name = format!("result-consumer-{}", task_id);
        let filter_subject = format!("results.{}.>", task_id);

        let consumer = self
            .jetstream
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    filter_subject: filter_subject.clone(),
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    max_deliver: 3,
                    ack_wait: std::time::Duration::from_secs(30),
                    max_ack_pending: 100,
                    ..Default::default()
                },
                oasis_core::JS_STREAM_RESULTS,
            )
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to create consumer: {}", e),
            })?;

        Ok(Box::new(NatsResultConsumer {
            consumer,
            consumer_name,
        }))
    }

    async fn publish(&self, task: Task) -> Result<String, CoreError> {
        self.ensure_task_stream().await?;

        // 使用任务本身的 ID，保持端到端一致
        let task_id = task.id.clone();

        // 转换为TaskSpec（包含相同 ID）
        let task_spec = task.to_spec();

        // 根据目标节点发布任务（幂等 + 去重 + Ack 确认 + 轻量重试）
        for agent_id in &task.target_agents() {
            let subject = format!("tasks.exec.agent.{}", agent_id);
            let data = serde_json::to_vec(&task_spec).map_err(|e| CoreError::Serialization {
                message: e.to_string(),
            })?;

            // 设置 Nats-Msg-Id = <task_id>@<subject>，确保同一任务在不同 subject 上不会误去重
            let mut headers = HeaderMap::new();
            let dedupe_key = format!("{}@{}", task_id, subject);
            headers.insert("Nats-Msg-Id", dedupe_key);

            // 轻量重试（最多 3 次）并等待 Publish Ack
            let mut last_err: Option<CoreError> = None;
            for attempt in 0..3u32 {
                match self
                    .jetstream
                    .publish_with_headers(subject.clone(), headers.clone(), data.clone().into())
                    .await
                {
                    Ok(ack_future) => match ack_future.await {
                        Ok(_) => {
                            last_err = None;
                            break;
                        }
                        Err(e) => {
                            last_err = Some(CoreError::Nats {
                                message: e.to_string(),
                            });
                        }
                    },
                    Err(e) => {
                        last_err = Some(CoreError::Nats {
                            message: e.to_string(),
                        });
                    }
                }

                if attempt < 2 {
                    let backoff = oasis_core::backoff::network_publish_backoff();
                    let delay = oasis_core::backoff::delay_for_attempt(&backoff, attempt);
                    tokio::time::sleep(delay).await;
                }
            }

            if let Some(err) = last_err {
                return Err(err);
            }
        }

        tracing::info!(task_id = %task_id, "Task published to JetStream");

        Ok(task_id.to_string())
    }
}

/// NATS 结果消费者实现
pub struct NatsResultConsumer {
    consumer:
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    consumer_name: String,
}

#[async_trait]
impl ResultConsumer for NatsResultConsumer {
    async fn next_result(&mut self) -> Result<Option<TaskResult>, CoreError> {
        let fetch_result = self
            .consumer
            .fetch()
            .max_messages(1)
            .expires(std::time::Duration::from_millis(500))
            .messages()
            .await;

        match fetch_result {
            Ok(mut batch) => {
                if let Some(Ok(msg)) = batch.next().await {
                    if let Ok(task_execution) =
                        serde_json::from_slice::<TaskExecution>(&msg.payload)
                    {
                        // 将 TaskExecution 转换为 TaskResult
                        let task_result = TaskResult {
                            task_id: task_execution.task_id.clone(),
                            agent_id: task_execution.agent_id.clone(),
                            status: match task_execution.exit_code {
                                Some(0) => crate::domain::models::task::TaskStatus::Completed {
                                    exit_code: 0,
                                },
                                Some(code) => crate::domain::models::task::TaskStatus::Completed {
                                    exit_code: code,
                                },
                                None => crate::domain::models::task::TaskStatus::Failed {
                                    error: "No exit code".to_string(),
                                },
                            },
                            stdout: task_execution.stdout,
                            stderr: task_execution.stderr,
                            duration_ms: task_execution.duration_ms,
                            timestamp: task_execution.timestamp,
                        };
                        let _ = msg.ack().await;
                        return Ok(Some(task_result));
                    }
                }
                Ok(None)
            }
            Err(_) => Ok(None),
        }
    }
}

impl Drop for NatsResultConsumer {
    fn drop(&mut self) {
        // 不在 Drop 中执行异步操作
        // 使用持久化消费者，让 NATS 自动清理
        tracing::debug!(
            consumer = %self.consumer_name,
            "NatsResultConsumer dropped, relying on NATS auto-cleanup"
        );
    }
}
