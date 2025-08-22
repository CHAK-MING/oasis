use anyhow::Result;
use futures::stream::{self, StreamExt};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::application::ports::repositories::{NodeRepository, RolloutRepository, TaskRepository};
use crate::domain::models::rollout::{BatchResult, Rollout, RolloutState};
use crate::domain::services::SelectorEngine;

/// 灰度发布管理器 - 负责自动化驱动灰度发布流程
pub struct RolloutManager {
    rollout_repo: Arc<dyn RolloutRepository>,
    node_repo: Arc<dyn NodeRepository>,
    task_repo: Arc<dyn TaskRepository>,
    selector_engine: Arc<dyn SelectorEngine>,
    shutdown_token: CancellationToken,
}

impl RolloutManager {
    pub fn new(
        rollout_repo: Arc<dyn RolloutRepository>,
        node_repo: Arc<dyn NodeRepository>,
        task_repo: Arc<dyn TaskRepository>,
        selector_engine: Arc<dyn SelectorEngine>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            rollout_repo,
            node_repo,
            task_repo,
            selector_engine,
            shutdown_token,
        }
    }

    /// 主循环，驱动所有灰度发布
    pub async fn run(&self) {
        info!("RolloutManager started");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    info!("RolloutManager is shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = self.process_active_rollouts().await {
                        error!("Error processing rollouts: {}", e);
                    }
                }
            }
        }

        info!("RolloutManager stopped");
    }

    /// 处理所有活动的灰度发布
    async fn process_active_rollouts(&self) -> Result<()> {
        let active_rollouts = self.rollout_repo.list_active().await?;

        // 并发处理活跃的灰度发布，限制最大并发以保护系统
        stream::iter(active_rollouts)
            .for_each_concurrent(Some(10), |mut rollout| async move {
                if let Err(e) = self.process_rollout(&mut rollout).await {
                    error!(rollout_id = %rollout.id, error = %e, "Error processing rollout");
                }
            })
            .await;

        Ok(())
    }

    /// 单个灰度发布的状态机逻辑
    async fn process_rollout(&self, rollout: &mut Rollout) -> Result<()> {
        match rollout.state {
            RolloutState::RunningBatch { current_batch } => {
                self.process_running_batch(rollout, current_batch).await?;
            }
            RolloutState::WaitingForNextBatch { batch_completed_at } => {
                self.process_waiting_batch(rollout, batch_completed_at)
                    .await?;
            }
            _ => {
                // 其他状态不需要处理
                debug!(rollout_id = %rollout.id, state = ?rollout.state, "Rollout in non-active state");
            }
        }

        Ok(())
    }

    /// 处理正在运行的批次
    async fn process_running_batch(
        &self,
        rollout: &mut Rollout,
        current_batch: usize,
    ) -> Result<()> {
        debug!(rollout_id = %rollout.id, batch = current_batch, "Processing running batch");

        // 如果当前批次任务为空，需要创建任务
        if rollout.current_batch_tasks.is_empty() {
            self.create_batch_tasks(rollout, current_batch).await?;
            return Ok(());
        }

        // 检查当前批次的任务是否已全部完成
        let mut completed_tasks = Vec::new();
        let mut successful_count = 0;
        let mut failed_count = 0;

        // 准备批量获取任务结果
        let task_agent_pairs: Vec<(String, String)> = rollout
            .current_batch_tasks
            .iter()
            .map(|(node_id, task_id)| (task_id.clone(), node_id.clone()))
            .collect();

        // 批量获取所有任务结果
        match self.task_repo.get_results_batch(&task_agent_pairs).await {
            Ok(results) => {
                for ((node_id, task_id), result_opt) in
                    rollout.current_batch_tasks.iter().zip(results.iter())
                {
                    match result_opt {
                        Some(result) => {
                            completed_tasks.push(node_id.clone());
                            match result.status {
                                crate::domain::models::task::TaskStatus::Completed { .. } => {
                                    successful_count += 1;
                                }
                                _ => {
                                    failed_count += 1;
                                }
                            }
                        }
                        None => {
                            // 任务还在运行，继续等待
                            debug!(rollout_id = %rollout.id, task_id = %task_id, "Task still running");
                        }
                    }
                }
            }
            Err(e) => {
                warn!(rollout_id = %rollout.id, error = %e, "Failed to get batch task results");
            }
        }

        // 如果所有任务都完成了
        if completed_tasks.len() == rollout.current_batch_tasks.len() {
            self.complete_batch(rollout, current_batch, successful_count, failed_count)
                .await?;
        }

        Ok(())
    }

    /// 处理等待中的批次
    async fn process_waiting_batch(
        &self,
        rollout: &mut Rollout,
        batch_completed_at: i64,
    ) -> Result<()> {
        let now = chrono::Utc::now().timestamp();
        let delay_secs = rollout.batch_delay_secs();
        let elapsed_secs = now - batch_completed_at;

        if elapsed_secs >= delay_secs as i64 {
            // 延迟时间已到，进入下一个批次
            let next_batch = rollout.current_batch().unwrap_or(0) + 1;
            rollout.state = RolloutState::RunningBatch {
                current_batch: next_batch,
            };
            rollout.updated_at = now;
            rollout.version += 1;

            info!(rollout_id = %rollout.id, next_batch = next_batch, "Starting next batch");

            // 保存状态
            self.rollout_repo.update(rollout.clone()).await?;
        } else {
            debug!(
                rollout_id = %rollout.id,
                elapsed_secs = elapsed_secs,
                delay_secs = delay_secs,
                "Waiting for batch delay"
            );
        }

        Ok(())
    }

    /// 为当前批次创建任务
    async fn create_batch_tasks(&self, rollout: &mut Rollout, current_batch: usize) -> Result<()> {
        info!(rollout_id = %rollout.id, batch = current_batch, "Creating batch tasks");

        // 获取目标节点（优先使用缓存，避免重复解析选择器）
        let target_nodes = if let Some(ref cached_nodes) = rollout.cached_target_nodes {
            cached_nodes.clone()
        } else {
            // 首次解析选择器并缓存结果
            // 1. 获取所有在线节点的属性
            let online_ids = self.node_repo.list_online().await?;
            let nodes_details = self.node_repo.get_nodes_batch(&online_ids).await?;
            let attrs: Vec<_> = nodes_details
                .into_iter()
                .map(|n| n.to_attributes())
                .collect();

            // 2. 使用节点属性解析选择器
            let resolved_nodes = self
                .selector_engine
                .resolve(&rollout.target_selector, &attrs)
                .await?;
            rollout.cached_target_nodes = Some(resolved_nodes.clone());
            resolved_nodes
        };

        // 初始化进度信息（如果是第一次创建批次）
        if rollout.progress.total_nodes == 0 {
            rollout.progress.total_nodes = target_nodes.len();

            // 计算总批次数
            let batch_size = self.calculate_batch_size(rollout, target_nodes.len());
            rollout.progress.total_batches = (target_nodes.len() + batch_size - 1) / batch_size; // 向上取整

            info!(
                rollout_id = %rollout.id,
                total_nodes = rollout.progress.total_nodes,
                total_batches = rollout.progress.total_batches,
                "Initialized rollout progress"
            );
        }

        // 过滤出未处理的节点
        let unprocessed_nodes: Vec<String> = target_nodes
            .into_iter()
            .filter(|node_id| !rollout.is_node_processed(node_id))
            .collect();

        if unprocessed_nodes.is_empty() {
            // 所有节点都已处理，标记为成功
            rollout.state = RolloutState::Succeeded;
            rollout.updated_at = chrono::Utc::now().timestamp();
            rollout.version += 1;

            info!(rollout_id = %rollout.id, "All nodes processed, rollout succeeded");
            self.rollout_repo.update(rollout.clone()).await?;
            return Ok(());
        }

        // 计算当前批次要处理的节点数量
        let batch_size = self.calculate_batch_size(rollout, unprocessed_nodes.len());
        let batch_nodes: Vec<String> = unprocessed_nodes.into_iter().take(batch_size).collect();

        info!(
            rollout_id = %rollout.id,
            batch = current_batch,
            batch_size = batch_nodes.len(),
            "Creating tasks for batch nodes"
        );

        // 记录批次开始时间
        let batch_start_time = chrono::Utc::now().timestamp();
        rollout.current_batch_started_at = Some(batch_start_time);

        // 为每个节点创建任务
        for node_id in &batch_nodes {
            let task_spec = rollout
                .task
                .clone()
                .with_args(rollout.task.args.clone())
                .with_env(rollout.task.env.clone())
                .with_timeout(rollout.task.timeout_seconds);
            let mut task_spec = task_spec;
            task_spec.target = oasis_core::task::TaskTarget::Agents(vec![oasis_core::types::AgentId::from(
                node_id.clone(),
            )]);

            let task = crate::domain::models::task::Task::from_spec(task_spec);
            let task_id = self.task_repo.publish(task).await?;
            rollout.add_batch_task(node_id.clone(), task_id);
        }

        // 更新进度
        rollout.progress.current_batch = Some(current_batch);
        rollout.updated_at = chrono::Utc::now().timestamp();
        rollout.version += 1;

        // 保存状态
        self.rollout_repo.update(rollout.clone()).await?;

        Ok(())
    }

    /// 完成当前批次
    async fn complete_batch(
        &self,
        rollout: &mut Rollout,
        current_batch: usize,
        successful_count: usize,
        failed_count: usize,
    ) -> Result<()> {
        info!(
            rollout_id = %rollout.id,
            batch = current_batch,
            successful = successful_count,
            failed = failed_count,
            "Completing batch"
        );

        // 计算批次耗时
        let completed_at = chrono::Utc::now().timestamp();
        let duration_secs = if let Some(started_at) = rollout.current_batch_started_at {
            (completed_at - started_at).max(0) as u64
        } else {
            0
        };

        // 记录批次结果
        let batch_result = BatchResult {
            batch_index: current_batch,
            node_count: successful_count + failed_count,
            successful_count,
            failed_count,
            duration_secs,
            completed_at,
        };
        rollout.batch_results.push(batch_result);

        // 更新进度
        rollout.progress.processed_nodes += successful_count + failed_count;
        rollout.progress.successful_nodes += successful_count;
        rollout.progress.failed_nodes += failed_count;
        // 重新计算完成比例（0.0~1.0），避免除零
        if rollout.progress.total_nodes > 0 {
            rollout.progress.completion_rate =
                (rollout.progress.processed_nodes as f64) / (rollout.progress.total_nodes as f64);
        } else {
            rollout.progress.completion_rate = 0.0;
        }

        // 标记节点为已处理
        let processed_node_ids: Vec<String> = rollout.current_batch_tasks.keys().cloned().collect();
        for node_id in processed_node_ids {
            rollout.mark_node_processed(node_id);
        }

        // 检查失败阈值
        let max_failures = rollout.max_failures();
        if rollout.progress.failed_nodes > max_failures {
            rollout.state = RolloutState::Failed {
                error: format!(
                    "Failed nodes ({}) exceeded maximum ({})",
                    rollout.progress.failed_nodes, max_failures
                ),
            };
            rollout.updated_at = chrono::Utc::now().timestamp();
            rollout.version += 1;

            error!(
                rollout_id = %rollout.id,
                failed_nodes = rollout.progress.failed_nodes,
                max_failures = max_failures,
                "Rollout failed due to too many failures"
            );
        } else {
            // 检查是否还有未处理的节点
            // 使用缓存的目标节点列表，避免在发布过程中动态改变目标范围
            let target_nodes = rollout
                .cached_target_nodes
                .as_ref()
                .cloned()
                .unwrap_or_default();

            let remaining_nodes: Vec<String> = target_nodes
                .into_iter()
                .filter(|node_id| !rollout.is_node_processed(node_id))
                .collect();

            if remaining_nodes.is_empty() {
                // 所有节点都已处理，标记为成功
                rollout.state = RolloutState::Succeeded;
                rollout.updated_at = chrono::Utc::now().timestamp();
                rollout.version += 1;

                info!(rollout_id = %rollout.id, "All nodes processed, rollout succeeded");
            } else {
                // 还有未处理的节点，进入等待状态
                rollout.state = RolloutState::WaitingForNextBatch {
                    batch_completed_at: chrono::Utc::now().timestamp(),
                };
                rollout.updated_at = chrono::Utc::now().timestamp();
                rollout.version += 1;

                info!(
                    rollout_id = %rollout.id,
                    remaining_nodes = remaining_nodes.len(),
                    "Batch completed, waiting for next batch"
                );
            }
        }

        // 清空当前批次任务和开始时间
        rollout.current_batch_tasks.clear();
        rollout.current_batch_started_at = None;

        // 保存状态
        self.rollout_repo.update(rollout.clone()).await?;

        Ok(())
    }

    /// 计算批次大小
    fn calculate_batch_size(&self, rollout: &Rollout, total_nodes: usize) -> usize {
        match &rollout.config.strategy {
            crate::domain::models::rollout::RolloutStrategy::Rolling { batch_size, .. } => {
                batch_size.compute(total_nodes)
            }
            _ => total_nodes, // 其他策略一次性处理所有节点
        }
    }
}
