use async_nats::jetstream::Context;
use dashmap::DashMap;
use futures_util::StreamExt;
use oasis_core::{
    constants,
    core_types::{BatchId, TaskId},
    error::{CoreError, Result},
    task_types::{Batch, Task, TaskExecution, TaskState},
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct TaskMonitorConfig {
    pub max_cache_size: usize,
    pub cache_ttl_seconds: u64,
    pub cleanup_interval_seconds: u64,
}

impl Default for TaskMonitorConfig {
    fn default() -> Self {
        Self {
            max_cache_size: 10000,
            cache_ttl_seconds: 3600,
            cleanup_interval_seconds: 300,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedExecution {
    pub execution: Arc<TaskExecution>,
    pub cached_at: i64,
}

/// Task 监控器：监听结果流，维护任务/执行缓存
pub struct TaskMonitor {
    jetstream: Arc<Context>,
    shutdown_token: CancellationToken,

    // 缓存
    pub batch_cache: DashMap<BatchId, Arc<Batch>>,
    pub batch_tasks_cache: DashMap<BatchId, Vec<TaskId>>,
    pub task_batch_cache: DashMap<TaskId, BatchId>,
    pub task_cache: DashMap<TaskId, Arc<Task>>,
    pub execution_cache: DashMap<TaskId, Vec<CachedExecution>>,

    pub config: TaskMonitorConfig,
}

impl TaskMonitor {
    pub fn new(jetstream: Arc<Context>, shutdown_token: CancellationToken) -> Self {
        Self {
            jetstream,
            shutdown_token,
            batch_cache: DashMap::new(),
            batch_tasks_cache: DashMap::new(),
            task_batch_cache: DashMap::new(),
            task_cache: DashMap::new(),
            execution_cache: DashMap::new(),
            config: TaskMonitorConfig::default(),
        }
    }

    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        let monitor = self.clone();
        tokio::spawn(async move {
            let cleanup_handle = monitor.start_cache_cleanup();

            let monitor_handle = async {
                if let Err(e) = monitor.run().await {
                    error!("TaskMonitor run error: {}", e);
                }
            };

            tokio::select! {
                _ = cleanup_handle => {},
                _ = monitor_handle => {},
                _ = monitor.shutdown_token.cancelled() => {
                    info!("TaskMonitor shutdown requested");
                }
            }
        })
    }

    async fn run(&self) -> Result<()> {
        info!("Starting TaskMonitor");
        let subject = "results.>";

        match self
            .jetstream
            .get_stream(&constants::JS_STREAM_RESULTS)
            .await
        {
            Ok(stream) => {
                let consumer_config = async_nats::jetstream::consumer::pull::Config {
                    filter_subject: subject.to_string(),
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    durable_name: Some("oasis-server-result-listener".to_string()),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: std::time::Duration::from_secs(10),
                    max_deliver: 3,
                    ..Default::default()
                };

                match stream.create_consumer(consumer_config).await {
                    Ok(consumer) => {
                        info!("Task result listener started successfully");

                        match consumer.messages().await {
                            Ok(mut messages) => loop {
                                tokio::select! {
                                    _ = self.shutdown_token.cancelled() => {
                                        info!("TaskMonitor stopped by shutdown signal");
                                        break;
                                    }
                                    message = messages.next() => {
                                        match message {
                                            Some(Ok(msg)) => {
                                                if let Ok(execution) = Self::parse_execution_message(&msg.payload) {
                                                    let task_id = &execution.task_id;
                                                    debug!("Received execution result for task: {}", task_id);
                                                    self.execution_cache
                                                        .entry(task_id.clone())
                                                        .or_insert_with(Vec::new)
                                                        .push(CachedExecution {
                                                            execution: Arc::new(execution.clone()),
                                                            cached_at: chrono::Utc::now().timestamp(),
                                                        });

                                                    if execution.state.is_terminal() {
                                                        if let Some(mut cached_task) = self.task_cache.get_mut(task_id) {
                                                            let task = Arc::make_mut(&mut cached_task);
                                                            if !task.state.is_terminal() {
                                                                let _ = task.transition_to(execution.state);
                                                                info!("Updated task {} status to {:?}", task_id, execution.state);
                                                            }
                                                        }
                                                    }
                                                }
                                                let _ = msg.ack().await;
                                            }
                                            Some(Err(e)) => warn!("Error receiving execution result: {}", e),
                                            None => { warn!("Result stream ended unexpectedly"); break; }
                                        }
                                    }
                                }
                            },
                            Err(e) => error!("Failed to get messages from result consumer: {}", e),
                        }
                    }
                    Err(e) => error!("Failed to create result monitoring consumer: {}", e),
                }
            }
            Err(e) => error!("Failed to get results stream: {}", e),
        }

        Ok(())
    }

    fn parse_execution_message(payload: &[u8]) -> Result<TaskExecution> {
        let proto_execution =
            oasis_core::proto::TaskExecutionMsg::decode(payload).map_err(|e| {
                CoreError::from_anyhow(anyhow::anyhow!("Failed to parse execution: {}", e), None)
            })?;
        Ok(TaskExecution::from(proto_execution))
    }

    // 对 TaskService 暴露的查询接口
    pub fn cache_insert_task(&self, task: Task) {
        self.task_cache.insert(task.task_id.clone(), Arc::new(task));
    }

    pub fn cache_insert_batch(&self, batch: Batch) {
        self.batch_cache
            .insert(batch.batch_id.clone(), Arc::new(batch));
    }

    pub fn cache_insert_batch_tasks(&self, batch_id: BatchId, task_ids: Vec<TaskId>) {
        self.batch_tasks_cache.insert(batch_id, task_ids);
    }

    pub fn cache_insert_task_batch(&self, task_id: TaskId, batch_id: BatchId) {
        self.task_batch_cache.insert(task_id, batch_id);
    }

    pub fn list_batches_from_cache(
        &self,
        limit: u32,
        _state_filter: Option<&[TaskState]>,
    ) -> (Vec<Batch>, u32) {
        let mut batches: Vec<Batch> = self
            .batch_cache
            .iter()
            .map(|entry| (**entry.value()).clone())
            .collect();

        batches.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let total = batches.len() as u32;
        if batches.len() > limit as usize {
            batches.truncate(limit as usize);
        }
        (batches, total)
    }

    pub fn latest_execution_from_cache(&self, task_id: &TaskId) -> Option<TaskExecution> {
        self.execution_cache
            .get(task_id)
            .and_then(|v| v.last().map(|e| (*e.execution).clone()))
    }

    pub fn get_batch_task_ids(&self, batch_id: &BatchId) -> Option<Vec<TaskId>> {
        self.batch_tasks_cache.get(batch_id).map(|v| v.clone())
    }

    async fn start_cache_cleanup(&self) {
        let shutdown_token = self.shutdown_token.clone();

        let mut interval =
            tokio::time::interval(Duration::from_secs(self.config.cleanup_interval_seconds));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.cleanup_expired_cache().await;
                }
                _ = shutdown_token.cancelled() => {
                    info!("Cache cleanup task stopped");
                    break;
                }
            }
        }
    }

    async fn cleanup_expired_cache(&self) {
        let now = chrono::Utc::now().timestamp();
        let ttl = self.config.cache_ttl_seconds as i64;

        // 新增：清理过期的 batch 和相关缓存
        let batch_cache = &self.batch_cache; // 需要传入这些参数
        let task_cache = &self.task_cache;
        let batch_tasks_cache = &self.batch_tasks_cache;
        let task_batch_cache = &self.task_batch_cache;
        let execution_cache = &self.execution_cache;

        let mut cleaned_batches = 0;
        let mut cleaned_tasks = 0;
        let mut cleaned_executions = 0;

        // 清理过期的 batch（超过 TTL 且所有任务都已完成）
        batch_cache.retain(|batch_id, batch| {
            let is_expired = now - batch.created_at > ttl;
            if is_expired {
                // 清理相关的映射缓存
                batch_tasks_cache.remove(batch_id);
                cleaned_batches += 1;
                false
            } else {
                true
            }
        });

        // 清理过期的已完成任务
        task_cache.retain(|task_id, task| {
            let is_terminal_and_old = task.state.is_terminal() && (now - task.created_at > ttl);
            if is_terminal_and_old {
                task_batch_cache.remove(task_id);
                cleaned_tasks += 1;
                false
            } else {
                true
            }
        });

        // 清理过期的执行记录
        execution_cache.retain(|_, executions| {
            let is_expired = now - executions.last().map(|e| e.cached_at).unwrap_or(0) > ttl;
            if is_expired {
                cleaned_executions += 1;
                false
            } else {
                true
            }
        });

        if cleaned_batches > 0 || cleaned_tasks > 0 || cleaned_executions > 0 {
            debug!(
                "Cache cleanup: removed {} batches, {} tasks",
                cleaned_batches, cleaned_tasks
            );
        }
    }
}
