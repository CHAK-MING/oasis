use crate::application::services::task_worker::TaskWorker;
use crate::domain::agent::Agent;
use crate::infrastructure::{
    nats::{
        attributes_repository::NatsAttributesRepository, consumer::NatsConsumer,
        publisher::NatsPublisher,
    },
    system::{executor::CommandExecutor, file_apply_handler::FileApplyHandler},
};
use futures::StreamExt;
use oasis_core::backoff::{execute_with_backoff, network_publish_backoff};
use oasis_core::{
    constants,
    rate_limit::{RateLimiterCollection, rate_limited_operation},
    shutdown::GracefulShutdown,
};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock, mpsc};
use tracing::{debug, error, info, warn};

pub struct TaskProcessor {
    agent: Arc<RwLock<Agent>>,
    nats_client: crate::infrastructure::nats::client::NatsClient,
    executor: Arc<CommandExecutor>,
    limiters: Arc<RateLimiterCollection>,
    shutdown: GracefulShutdown,
}

impl TaskProcessor {
    pub fn new(
        agent: Arc<RwLock<Agent>>,
        nats_client: crate::infrastructure::nats::client::NatsClient,
        executor: Arc<CommandExecutor>,
        limiters: Arc<RateLimiterCollection>,
        shutdown: GracefulShutdown,
    ) -> Self {
        Self {
            agent,
            nats_client,
            executor,
            limiters,
            shutdown,
        }
    }

    pub async fn run(self) {
        let consumer_api = NatsConsumer::new(self.nats_client.clone());
        let publisher = Arc::new(NatsPublisher::new(self.nats_client.clone()));
        let labels_repo = Arc::new(NatsAttributesRepository::new(&self.nats_client.client));
        let file_handler = Arc::new(FileApplyHandler::new());
        let dlq_js = Arc::new(self.nats_client.jetstream.clone());

        // 获取 agent ID
        let agent_id = {
            let agent = self.agent.read().await;
            agent.id.clone()
        };

        info!("Starting task processor for agent: {}", agent_id);

        // 绑定消费者
        let consumer = {
            let shutdown = self.shutdown.clone();
            let agent_id_cl = agent_id.clone();
            let backoff = network_publish_backoff();
            let result = execute_with_backoff(
                || async {
                    if shutdown.is_cancelled() {
                        return Err(anyhow::anyhow!("shutdown"));
                    }
                    consumer_api
                        .consume_tasks(agent_id_cl.as_str())
                        .await
                        .map_err(|e| anyhow::anyhow!(e))
                },
                backoff,
            )
            .await;
            match result {
                Ok(c) => {
                    info!("Successfully created task consumer for agent: {}", agent_id);
                    c
                }
                Err(e) => {
                    error!("Failed to create task consumer after retries: {}", e);
                    return;
                }
            }
        };

        // 并发与缓冲（与 fetch 批量对齐：并发×4）
        let max_concurrent: usize = 4;
        let channel_buffer: usize = max_concurrent.saturating_mul(4);

        // 创建有界通道
        let (tx, rx) = mpsc::channel::<async_nats::jetstream::Message>(channel_buffer);
        let shared_rx = Arc::new(Mutex::new(rx));

        // 构建共享的 TaskWorker 实例
        let worker = Arc::new(TaskWorker::new(
            self.agent.clone(),
            self.executor.clone(),
            file_handler.clone(),
            publisher.clone(),
            labels_repo.clone(),
            agent_id.to_string(),
            dlq_js.clone(),
            self.limiters.clone(),
        ));

        // 启动工作池
        let workers_left = Arc::new(std::sync::atomic::AtomicUsize::new(max_concurrent));
        let all_workers_done = Arc::new(Notify::new());
        for i in 0..max_concurrent {
            let worker_clone = worker.clone();
            let shared_rx_clone = shared_rx.clone();
            let workers_left_clone = workers_left.clone();
            let all_workers_done_clone = all_workers_done.clone();
            tokio::spawn(async move {
                loop {
                    let msg_opt = {
                        let mut guard = shared_rx_clone.lock().await;
                        guard.recv().await
                    };
                    match msg_opt {
                        Some(msg) => {
                            info!("Worker {} received task message", i);
                            if let Err(e) = worker_clone.process_message(msg).await {
                                error!("Worker {} failed to process task: {}", i, e);
                            } else {
                                info!("Worker {} successfully processed task", i);
                            }
                        }
                        None => {
                            if workers_left_clone.fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
                                == 1
                            {
                                all_workers_done_clone.notify_waiters();
                            }
                            break;
                        }
                    }
                }
            });
        }

        // 生产者循环：使用 select! 模式，确保可以即时响应 shutdown 信号
        let mut last_cleanup = std::time::Instant::now();
        loop {
            // 优先检查 shutdown 信号
            if self.shutdown.is_cancelled() {
                info!("Shutdown signal received, breaking task processor loop.");
                break;
            }

            // 检查 agent 是否可以接受任务
            let can_accept_tasks = self.agent.read().await.can_accept_tasks();
            if !can_accept_tasks {
                warn!("Agent is draining; stop fetching new tasks and wait for workers to finish.");
                break;
            }

            // 定期清理过期的任务记录（每小时一次）
            if last_cleanup.elapsed() > std::time::Duration::from_secs(3600) {
                let mut agent = self.agent.write().await;
                agent.cleanup_old_records();
                last_cleanup = std::time::Instant::now();
                debug!("Cleaned up old task records");
            }

            // 使用 select! 优雅地处理消息拉取和关闭信号
            tokio::select! {
                //  biased to prefer shutdown signal
                biased;

                _ = self.shutdown.cancelled() => {
                    info!("Shutdown received while waiting for tasks, exiting.");
                    break;
                }

                fetch_result = async {
                    // 限流 NATS 拉取
                    let _ = rate_limited_operation(
                        &self.limiters.nats,
                        || async { Ok(()) },
                        None,
                        "tasks.fetch",
                    ).await;

                    consumer
                        .fetch()
                        // 将批量与并发对齐（并发×4）以避免占满 pending
                        .max_messages(max_concurrent.saturating_mul(4))
                        .expires(std::time::Duration::from_millis(
                            constants::DEFAULT_FETCH_EXPIRES_MS,
                        ))
                        .messages()
                        .await
                } => {
                    match fetch_result {
                        Ok(mut batch) => {
                            let mut messages_processed = 0;
                            while let Some(next_msg) = batch.next().await {
                                messages_processed += 1;
                                match next_msg {
                                    Ok(msg) => {
                                        debug!("Received task message, sending to worker");
                                        if tx.send(msg).await.is_err() {
                                            error!("All workers dropped, stopping task processor");
                                            // tx 端已关闭，无法恢复，直接返回
                                            return;
                                        }
                                    }
                                    Err(e) => error!("Error receiving message from batch: {}", e),
                                }
                            }

                            if messages_processed == 0 {
                                debug!("No messages received in this batch");
                                // 没有消息时短暂 sleep，避免在空闲时消耗过多CPU
                                // 使用 select! 确保在 sleep 期间也能响应 shutdown 信号
                                tokio::select! {
                                    _ = tokio::time::sleep(std::time::Duration::from_millis(1000)) => {
                                        // 正常 sleep 完成，继续循环
                                    }
                                    _ = self.shutdown.cancelled() => {
                                        info!("Shutdown received during idle sleep, exiting.");
                                        return;
                                    }
                                }
                            } else {
                                info!("Processed {} messages in this batch", messages_processed);
                            }
                        }
                        Err(e) => {
                            error!("Failed to fetch messages: {}. Backing off before retry...", e);
                            // 更清晰的退避语义：根据尝试次数计算延迟，并在休眠期间保持对关机的响应
                            let cfg = network_publish_backoff();
                            // 这里简单采用 max_interval 作为一次等待上限，避免携带 attempt 计数状态
                            let wait = cfg.max_interval;
                            tokio::select! {
                                _ = tokio::time::sleep(wait) => {}
                                _ = self.shutdown.cancelled() => {
                                    info!("Shutdown received during backoff sleep, exiting.");
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }

        // 停止生产：关闭发送端，等待所有 worker 退出
        drop(tx);
        all_workers_done.notified().await;
    }
}
