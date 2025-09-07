//! 灰度发布服务
use crate::infrastructure::monitor::task_monitor::TaskMonitor;
use async_nats::jetstream::Context;
use dashmap::DashMap;
use oasis_core::core_types::{AgentId, BatchId, RolloutId};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use oasis_core::rollout_types::*;
use oasis_core::task_types::TaskState;
use std::sync::Arc;
use tracing::info;

/// 灰度发布服务 - 仅负责状态管理
pub struct RolloutService {
    _jetstream: Arc<Context>,
    task_monitor: Arc<TaskMonitor>,
    /// 内存中的发布状态缓存
    rollout_cache: Arc<DashMap<RolloutId, RolloutStatus>>,
}

impl RolloutService {
    pub async fn new(_jetstream: Arc<Context>, task_monitor: Arc<TaskMonitor>) -> Result<Self> {
        info!("Initializing RolloutService");

        Ok(Self {
            _jetstream,
            task_monitor,
            rollout_cache: Arc::new(DashMap::new()),
        })
    }

    /// 创建灰度发布 - 只创建状态，不执行任务
    pub async fn create_rollout(
        &self,
        request: CreateRolloutRequest,
        all_target_agents: Vec<AgentId>,
    ) -> Result<RolloutId> {
        // 验证请求
        request.validate().map_err(|e| CoreError::InvalidTask {
            reason: e,
            severity: ErrorSeverity::Error,
        })?;

        let rollout_id = RolloutId::generate();
        info!("Creating rollout: {} - {}", rollout_id, request.name);

        if all_target_agents.is_empty() {
            return Err(CoreError::InvalidTask {
                reason: "没有找到匹配的在线Agent".to_string(),
                severity: ErrorSeverity::Error,
            });
        }

        // 创建发布配置
        let config = RolloutConfig {
            rollout_id: rollout_id.clone(),
            name: request.name,
            target: request.target,
            strategy: request.strategy,
            task_type: request.task_type,
            auto_advance: request.auto_advance,
            advance_interval_seconds: request.advance_interval_seconds,
            created_at: chrono::Utc::now().timestamp(),
        };

        // 创建发布状态，包含智能阶段划分
        let status = Self::create_rollout_status_with_smart_stages(config, all_target_agents);

        // 缓存状态
        self.rollout_cache.insert(rollout_id.clone(), status);

        info!("Rollout created successfully: {}", rollout_id);
        Ok(rollout_id)
    }

    /// 智能创建发布状态 - 处理Agent数量少的特殊情况
    fn create_rollout_status_with_smart_stages(
        config: RolloutConfig,
        all_target_agents: Vec<AgentId>,
    ) -> RolloutStatus {
        let total_agents = all_target_agents.len();

        // 检查是否需要合并阶段（Agent数量过少）
        let should_merge_stages = match &config.strategy {
            RolloutStrategy::Percentage { stages } => {
                total_agents <= 2 || stages.len() > total_agents
            }
            RolloutStrategy::Count { stages } => total_agents <= 2 || stages.len() > total_agents,
            RolloutStrategy::Groups { groups } => total_agents <= 2 || groups.len() > total_agents,
        };

        if should_merge_stages {
            info!("Agent数量较少({}), 合并为单阶段执行", total_agents);
            Self::create_single_stage_rollout(config, all_target_agents)
        } else {
            // 使用原有的阶段划分逻辑
            RolloutStatus::new(config, all_target_agents)
        }
    }

    /// 创建单阶段发布（用于Agent数量少的情况）
    fn create_single_stage_rollout(
        config: RolloutConfig,
        all_target_agents: Vec<AgentId>,
    ) -> RolloutStatus {
        let stage = RolloutStageStatus {
            stage_index: 0,
            stage_name: "全部节点".to_string(),
            target_agents: all_target_agents.clone(),
            batch_id: None,
            started_count: 0,
            completed_count: 0,
            failed_count: 0,
            state: RolloutState::Created,
            started_at: None,
            completed_at: None,
            failed_executions: Vec::new(),
        };

        RolloutStatus {
            config,
            state: RolloutState::Created,
            current_stage: 0,
            stages: vec![stage],
            all_target_agents,
            updated_at: chrono::Utc::now().timestamp(),
            error_message: None,
        }
    }

    /// 获取灰度发布状态
    pub async fn get_rollout_status(&self, rollout_id: &RolloutId) -> Result<RolloutStatus> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            // 更新阶段状态
            self.update_stage_status_from_monitor(status.value_mut())
                .await;
            Ok(status.value().clone())
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 列出灰度发布
    pub async fn list_rollouts(
        &self,
        limit: u32,
        states: Option<Vec<RolloutState>>,
    ) -> Result<Vec<RolloutStatus>> {
        let mut rollouts: Vec<RolloutStatus> = Vec::new();

        // 遍历所有缓存的 rollout，并更新最新状态
        for mut entry in self.rollout_cache.iter_mut() {
            let status = entry.value_mut();

            self.update_stage_status_from_monitor(status).await;

            let updated_status = status.clone();

            // 应用状态过滤
            if states
                .as_ref()
                .map_or(true, |s| s.contains(&updated_status.state))
            {
                rollouts.push(updated_status);
            }
        }

        // 按创建时间倒序排列
        rollouts.sort_by(|a, b| b.config.created_at.cmp(&a.config.created_at));
        rollouts.truncate(limit as usize);

        Ok(rollouts)
    }

    /// 获取下一阶段的信息
    pub async fn get_next_stage_info(
        &self,
        rollout_id: &RolloutId,
    ) -> Result<Option<(u32, Vec<AgentId>, RolloutTaskType)>> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();

            // 更新状态
            self.update_stage_status_from_monitor(status_val).await;

            // 检查是否可以推进
            if !status_val.can_advance() {
                return Err(CoreError::InvalidTask {
                    reason: format!("Rollout {} 当前状态不允许推进", rollout_id),
                    severity: ErrorSeverity::Error,
                });
            }

            // 获取下一个要执行的阶段
            if let Some(stage) = status_val.stages.get(status_val.current_stage as usize) {
                Ok(Some((
                    stage.stage_index,
                    stage.target_agents.clone(),
                    status_val.config.task_type.clone(),
                )))
            } else {
                Ok(None) // 已完成所有阶段
            }
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 记录阶段开始执行（由 handlers 调用）
    pub async fn mark_stage_started(
        &self,
        rollout_id: &RolloutId,
        batch_id: BatchId,
    ) -> Result<u32> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();

            if let Some(current_stage) =
                status_val.stages.get_mut(status_val.current_stage as usize)
            {
                current_stage.batch_id = Some(batch_id.clone());
                current_stage.state = RolloutState::Running;
                current_stage.started_at = Some(chrono::Utc::now().timestamp());

                status_val.state = RolloutState::Running;
                status_val.updated_at = chrono::Utc::now().timestamp();

                // 推进到下一阶段（为下次 advance 做准备）
                let executed_stage = status_val.current_stage + 1;
                status_val.current_stage += 1;

                info!(
                    "Stage {} of rollout {} started with batch {}",
                    executed_stage, rollout_id, batch_id
                );
                Ok(executed_stage)
            } else {
                Err(CoreError::InvalidTask {
                    reason: "无效的阶段索引".to_string(),
                    severity: ErrorSeverity::Error,
                })
            }
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 获取已完成阶段的 Agent 列表（供回滚使用）
    pub async fn get_completed_agents(&self, rollout_id: &RolloutId) -> Result<Vec<AgentId>> {
        if let Some(status) = self.rollout_cache.get(rollout_id) {
            let completed_agents: Vec<AgentId> = status
                .stages
                .iter()
                .filter(|s| s.state == RolloutState::Completed)
                .flat_map(|s| s.target_agents.iter().cloned())
                .collect();
            Ok(completed_agents)
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 回滚灰度发布 - TODO: 需要实现完整的回滚逻辑
    pub async fn rollback_rollout(
        &self,
        rollout_id: &RolloutId,
        _rollback_command: Option<String>,
    ) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();
            if !status_val.state.can_rollback() {
                return Err(CoreError::InvalidTask {
                    reason: format!("Rollout {} 当前状态不允许回滚", rollout_id),
                    severity: ErrorSeverity::Error,
                });
            }

            // TODO: 实现完整的回滚逻辑
            // 1. 根据任务类型确定回滚策略
            // 2. 对于文件部署：恢复备份文件
            // 3. 对于命令执行：执行用户提供的回滚命令
            // 4. 提交回滚任务到已完成的Agent
            // 5. 更新状态为 RolledBack

            status_val.state = RolloutState::RolledBack;
            status_val.updated_at = chrono::Utc::now().timestamp();

            info!(
                "Rollout {} marked as rolled back (TODO: implement full rollback)",
                rollout_id
            );
            Ok(())
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 从 TaskMonitor 更新阶段状态
    async fn update_stage_status_from_monitor(&self, status: &mut RolloutStatus) {
        for stage in &mut status.stages {
            if let Some(batch_id) = &stage.batch_id {
                // 从 task_monitor 获取任务执行详情
                if let Some(task_ids) = self.task_monitor.get_batch_task_ids(batch_id) {
                    let mut started = 0u32;
                    let mut completed = 0u32;
                    let mut failed = 0u32;
                    let mut failed_executions = Vec::new();

                    // 统计各种状态的任务数量并收集失败详情
                    for task_id in task_ids {
                        if let Some(execution) =
                            self.task_monitor.latest_execution_from_cache(&task_id)
                        {
                            match execution.state {
                                TaskState::Running => started += 1,
                                TaskState::Success => completed += 1,
                                TaskState::Failed => {
                                    failed += 1;
                                    failed_executions.push(execution);
                                }
                                _ => {}
                            }
                        }
                    }

                    stage.started_count = started + completed + failed;
                    stage.completed_count = completed;
                    stage.failed_count = failed;
                    stage.failed_executions = failed_executions;

                    // 更新阶段状态
                    let total_targets = stage.target_agents.len() as u32;
                    if completed + failed >= total_targets {
                        if failed > 0 {
                            // 有失败任务，阶段失败
                            stage.state = RolloutState::Failed;
                        } else {
                            // 全部成功，阶段完成
                            stage.state = RolloutState::Completed;
                        }
                        if stage.completed_at.is_none() {
                            stage.completed_at = Some(chrono::Utc::now().timestamp());
                        }
                    } else if started > 0 {
                        // 阶段执行中
                        stage.state = RolloutState::Running;
                    }
                }
            }
        }

        // 更新整体状态
        if status.is_completed() {
            status.state = RolloutState::Completed;
        } else if status
            .stages
            .iter()
            .any(|s| s.state == RolloutState::Failed)
        {
            status.state = RolloutState::Failed;
        } else if status
            .stages
            .iter()
            .any(|s| s.state == RolloutState::Running)
        {
            status.state = RolloutState::Running;
        }

        status.updated_at = chrono::Utc::now().timestamp();
    }
}
