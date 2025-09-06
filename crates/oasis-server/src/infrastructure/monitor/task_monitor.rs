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
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Task 监控器：监听结果流，维护任务/执行缓存
pub struct TaskMonitor {
    jetstream: Arc<Context>,
    shutdown_token: CancellationToken,

    // 缓存
    pub batch_cache: DashMap<BatchId, Arc<Batch>>,
    pub batch_tasks_cache: DashMap<BatchId, Vec<TaskId>>,
    pub task_batch_cache: DashMap<TaskId, BatchId>,
    pub task_cache: DashMap<TaskId, Arc<Task>>,
    pub execution_cache: DashMap<TaskId, Vec<Arc<TaskExecution>>>,
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
        }
    }

    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!("TaskMonitor run error: {}", e);
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
                                                        .push(Arc::new(execution.clone()));

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
            .and_then(|v| v.last().map(|e| (**e).clone()))
    }

    pub fn get_batch_task_ids(&self, batch_id: &BatchId) -> Option<Vec<TaskId>> {
        self.batch_tasks_cache.get(batch_id).map(|v| v.clone())
    }
}
