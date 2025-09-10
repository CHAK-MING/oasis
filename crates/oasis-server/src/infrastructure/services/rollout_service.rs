//! 灰度发布服务
use crate::infrastructure::monitor::task_monitor::TaskMonitor;
use async_nats::jetstream::Context;
use dashmap::DashMap;
use futures::StreamExt;
use oasis_core::constants::JS_KV_ROLLOUTS;
use oasis_core::core_types::{AgentId, BatchId, RolloutId};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use oasis_core::rollout_types::*;
use oasis_core::task_types::TaskState;
use prost::Message;
use std::sync::Arc;
use tracing::{error, info, warn};

/// 灰度发布服务 - 负责状态管理和JetStream持久化
pub struct RolloutService {
    jetstream: Arc<Context>,
    task_monitor: Arc<TaskMonitor>,
    /// 内存中的发布状态缓存
    rollout_cache: Arc<DashMap<RolloutId, RolloutStatus>>,
}

impl RolloutService {
    pub async fn new(jetstream: Arc<Context>, task_monitor: Arc<TaskMonitor>) -> Result<Self> {
        info!("Initializing RolloutService");

        let service = Self {
            jetstream,
            task_monitor,
            rollout_cache: Arc::new(DashMap::new()),
        };

        // 启动时从 JetStream 恢复状态
        if let Err(e) = service.load_rollouts_from_jetstream().await {
            warn!("Failed to load rollouts from JetStream: {}", e);
        }

        Ok(service)
    }

    /// 从 JetStream 加载所有 rollout 状态到内存缓存
    async fn load_rollouts_from_jetstream(&self) -> Result<()> {
        let kv_store = match self.jetstream.get_key_value(JS_KV_ROLLOUTS).await {
            Ok(store) => store,
            Err(e) => {
                warn!("Failed to get rollouts KV store: {}, skipping load", e);
                return Ok(()); // KV 不存在时不是错误，可能是首次启动
            }
        };

        match kv_store.keys().await {
            Ok(mut keys) => {
                let mut loaded_count = 0;
                while let Some(key) = keys.next().await {
                    if let Ok(key_str) = key {
                        if let Ok(entry) = kv_store.get(&key_str).await {
                            if let Some(bytes) = entry {
                                match oasis_core::proto::RolloutStatusMsg::decode(bytes.as_ref()) {
                                    Ok(proto_status) => {
                                        let status: RolloutStatus = (&proto_status).into();
                                        let rollout_id = status.config.rollout_id.clone();
                                        self.rollout_cache.insert(rollout_id.clone(), status);
                                        loaded_count += 1;
                                        info!("Loaded rollout {} from JetStream", rollout_id);
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to decode rollout from key {}: {}",
                                            key_str, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                info!("Loaded {} rollouts from JetStream", loaded_count);
            }
            Err(e) => {
                warn!("Failed to list rollout keys: {}", e);
            }
        }

        Ok(())
    }

    /// 持久化 rollout 状态到 JetStream
    async fn persist_rollout_to_jetstream(&self, rollout_status: &RolloutStatus) -> Result<()> {
        let kv_store = self
            .jetstream
            .get_key_value(JS_KV_ROLLOUTS)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to get rollouts KV store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        let key = format!("rollout.{}", rollout_status.config.rollout_id);
        let proto: oasis_core::proto::RolloutStatusMsg =
            oasis_core::proto::RolloutStatusMsg::from(rollout_status.clone());
        let data = proto.encode_to_vec();

        kv_store
            .put(&key, data.into())
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to persist rollout to JetStream: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        Ok(())
    }

    /// 创建灰度发布 - 创建状态并持久化到JetStream
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
        let mut status = Self::create_rollout_status_with_smart_stages(config, all_target_agents);

        status.current_action = "创建发布".to_string();

        // 持久化到 JetStream
        self.persist_rollout_to_jetstream(&status).await?;

        // 缓存状态
        self.rollout_cache.insert(rollout_id.clone(), status);

        info!("Rollout created successfully: {}", rollout_id);
        Ok(rollout_id)
    }

    /// 智能创建发布状态
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
        };

        if should_merge_stages {
            info!("Agent number is less than 2, merge to single stage");
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
            stage_name: "全部节点".to_string(),
            target_agents: all_target_agents.clone(),
            batch_id: None,
            started_count: 0,
            completed_count: 0,
            failed_count: 0,
            started_at: None,
            completed_at: None,
            failed_executions: Vec::new(),
            version_snapshot: None,
        };

        RolloutStatus {
            config,
            state: RolloutState::Created,
            current_stage_idx: 0,
            stages: vec![stage],
            all_target_agents,
            updated_at: chrono::Utc::now().timestamp(),
            error_message: None,
            current_action: "".to_string(),
        }
    }

    /// 获取灰度发布状态 - 优先内存缓存，后JetStream
    pub async fn get_rollout_status(&self, rollout_id: &RolloutId) -> Result<RolloutStatus> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            // 更新阶段状态
            self.update_stage_status_from_task(status.value_mut()).await;
            self.update_stage_status_from_file(status.value_mut()).await;
            Ok(status.value().clone())
        } else {
            // 从 JetStream 加载
            match self.load_rollout_from_jetstream(rollout_id).await {
                Ok(mut status) => {
                    // 更新状态并缓存
                    self.update_stage_status_from_task(&mut status).await;
                    self.update_stage_status_from_file(&mut status).await;
                    self.rollout_cache
                        .insert(rollout_id.clone(), status.clone());
                    Ok(status)
                }
                Err(_) => Err(CoreError::NotFound {
                    entity_type: "Rollout".to_string(),
                    entity_id: rollout_id.to_string(),
                    severity: ErrorSeverity::Error,
                }),
            }
        }
    }

    /// 从 JetStream 加载单个 rollout
    async fn load_rollout_from_jetstream(&self, rollout_id: &RolloutId) -> Result<RolloutStatus> {
        let kv_store = self
            .jetstream
            .get_key_value(JS_KV_ROLLOUTS)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to get rollouts KV store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        let key = format!("rollout.{}", rollout_id);
        let entry = kv_store.get(&key).await.map_err(|e| CoreError::Nats {
            message: format!("Failed to get rollout from JetStream: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        if let Some(bytes) = entry {
            let proto =
                oasis_core::proto::RolloutStatusMsg::decode(bytes.as_ref()).map_err(|e| {
                    CoreError::InvalidTask {
                        reason: format!("Failed to decode rollout: {}", e),
                        severity: ErrorSeverity::Error,
                    }
                })?;
            let status: RolloutStatus = (&proto).into();
            Ok(status)
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 列出灰度发布 - 优先内存缓存，后JetStream
    pub async fn list_rollouts(
        &self,
        limit: u32,
        states: Option<Vec<RolloutState>>,
    ) -> Result<Vec<RolloutStatus>> {
        let mut rollouts: Vec<RolloutStatus> = Vec::new();

        // 遍历所有缓存的 rollout，并更新最新状态
        for mut entry in self.rollout_cache.iter_mut() {
            let status = entry.value_mut();
            self.update_stage_status_from_task(status).await;
            self.update_stage_status_from_file(status).await;
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
    ) -> Result<Option<(Vec<AgentId>, RolloutTaskType)>> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();

            // 更新状态
            self.update_stage_status_from_task(status_val).await;
            self.update_stage_status_from_file(status_val).await;

            // 检查是否可以推进
            if !status_val.can_advance() {
                return Err(CoreError::InvalidTask {
                    reason: format!("Rollout {} 当前状态不允许推进", rollout_id),
                    severity: ErrorSeverity::Error,
                });
            }

            // 获取下一个要执行的阶段
            if let Some(stage) = status_val.current_stage_status() {
                Ok(Some((
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

    // 记录推进下一个阶段
    pub async fn mark_advance_next_stage(
        &self,
        rollout_id: &RolloutId,
        task_type: RolloutTaskType,
        batch_id: Option<BatchId>,
        version_snapshot: Option<VersionSnapshot>,
    ) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();

            status_val.current_action = match task_type {
                RolloutTaskType::Command { command, args, .. } => {
                    format!("{} {}", command, args.join(" "))
                }
                RolloutTaskType::FileDeployment { config } => {
                    let filename = config.source_path.split("/").last().unwrap_or_default();
                    format!("部署文件: {}", filename)
                }
            };
            status_val.updated_at = chrono::Utc::now().timestamp();
            status_val.state = RolloutState::Running;
            let current_stage = status_val.current_stage_status_mut().unwrap();
            current_stage.batch_id = batch_id;
            current_stage.version_snapshot = version_snapshot;
            if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                warn!("Failed to persist advance next stage: {}", e);
            }
        }
        Ok(())
    }

    /// 标记回滚阶段
    pub async fn mark_rollback_stage(
        &self,
        rollout_id: &RolloutId,
        task_type: RolloutTaskType,
        rollback_command: Option<String>,
        batch_id: Option<BatchId>,
    ) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();

            status_val.state = RolloutState::RollingBack;
            status_val.updated_at = chrono::Utc::now().timestamp();
            match task_type {
                RolloutTaskType::Command { .. } => {
                    // 如果没有提供 rollback_command，则报错
                    status_val.current_action =
                        rollback_command.ok_or_else(|| CoreError::InvalidTask {
                            reason: "命令回滚需要提供 rollback_command".to_string(),
                            severity: ErrorSeverity::Error,
                        })?;
                }
                RolloutTaskType::FileDeployment { config } => {
                    status_val.current_action = format!(
                        "部署文件回滚: {}",
                        config.source_path.split("/").last().unwrap_or_default()
                    );
                }
            }

            // 回滚时，将 batch_id 设置到前一个阶段
            if let Some(previous_stage) = status_val.previous_stage_status_mut() {
                previous_stage.batch_id = batch_id;
            }

            if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                warn!("Failed to persist rollback started: {}", e);
            }
            Ok(())
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 获取需要回滚的阶段信息（返回阶段索引、目标 agents、任务类型、版本快照）
    pub async fn get_rollback_stage_info(
        &self,
        rollout_id: &RolloutId,
    ) -> Result<Option<(Vec<AgentId>, RolloutTaskType, Option<VersionSnapshot>)>> {
        if let Some(status) = self.rollout_cache.get(rollout_id) {
            if !status.can_rollback() {
                return Err(CoreError::InvalidTask {
                    reason: format!("Rollout {} 当前状态不允许回滚", rollout_id),
                    severity: ErrorSeverity::Error,
                });
            }
            // 取上次执行的阶段
            if let Some(stage) = status.previous_stage_status() {
                return Ok(Some((
                    stage.target_agents.clone(),
                    status.config.task_type.clone(),
                    stage.version_snapshot.clone(),
                )));
            }
            Ok(None)
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 从 TaskMonitor 更新阶段状态
    async fn update_stage_status_from_task(&self, status: &mut RolloutStatus) {
        if (status.state == RolloutState::Running || status.state == RolloutState::RollingBack)
            && !status.current_action.starts_with("部署文件")
        {
            // 这里只需要更新最新的阶段状态（一般需要更新的情况是当前处在执行中/回滚中) 的命令执行的情况
            // 从 status 拿到 current_stage 作为当前 status.stages 的索引
            // 如果已经拿到结果了，就更新当前阶段的
            // 如果没有拿到结果，就直接返回，这里不阻塞
            let mut failed_count = 0;
            let mut completed_count = 0;
            let mut started_count = 0;
            let mut failed_executions = Vec::new();

            // 先确定要更新的阶段
            let is_rolling_back = status.state == RolloutState::RollingBack;
            if is_rolling_back && status.current_stage_idx > 0 {
                // 回滚时更新前一个阶段
                status.current_stage_idx = status.current_stage_idx - 1;
            }

            // 获取阶段并更新统计信息
            if let Some(stage) = status.current_stage_status_mut() {
                if let Some(batch_id) = &stage.batch_id {
                    if let Some(task_ids) = self.task_monitor.get_batch_task_ids(batch_id) {
                        // 统计各种状态的任务数量并收集失败详情
                        for task_id in task_ids {
                            if let Some(execution) =
                                self.task_monitor.latest_execution_from_cache(&task_id)
                            {
                                match execution.state {
                                    TaskState::Running => started_count += 1,
                                    TaskState::Success => completed_count += 1,
                                    TaskState::Failed => {
                                        failed_count += 1;
                                        failed_executions.push(execution);
                                    }
                                    TaskState::Timeout => {
                                        failed_count += 1;
                                        failed_executions.push(execution);
                                    }
                                    _ => {}
                                }
                            }
                        }

                        stage.started_count = started_count + completed_count + failed_count;
                        stage.completed_count = completed_count;
                        stage.failed_count = failed_count;
                        stage.failed_executions = failed_executions;
                        stage.completed_at = Some(chrono::Utc::now().timestamp());
                    }
                } else {
                    return;
                }
            } else {
                return;
            }

            // 检查是否所有任务都已完成
            let total_targets = if let Some(stage) = status.current_stage_status() {
                stage.target_agents.len() as u32
            } else {
                return;
            };

            if completed_count + failed_count >= total_targets {
                if failed_count > 0 {
                    // 有失败任务
                    if is_rolling_back {
                        status.state = RolloutState::RollbackFailed;
                    } else {
                        status.state = RolloutState::Failed;
                        status.current_stage_idx += 1;
                    }
                } else {
                    // 全部成功
                    if is_rolling_back {
                        status.state = RolloutState::RolledBack;
                    } else {
                        status.state = RolloutState::Completed;
                        // 正常完成时，推进到下一阶段
                        status.current_stage_idx += 1;
                    }
                }
            }

            status.updated_at = chrono::Utc::now().timestamp();

            // 持久化更新的状态
            if let Err(e) = self.persist_rollout_to_jetstream(status).await {
                error!("Failed to persist status update: {}", e);
            }
        }
    }

    pub async fn update_stage_status_from_file(&self, status: &mut RolloutStatus) {
        if (status.state == RolloutState::Running || status.state == RolloutState::RollingBack)
            && status.current_action.starts_with("部署文件")
        {
            // 先确定要更新的阶段
            let is_rolling_back = status.state == RolloutState::RollingBack;
            if is_rolling_back && status.current_stage_idx > 0 {
                status.current_stage_idx = status.current_stage_idx - 1;
            }

            // 这里默认设置所有文件部署成功
            if let Some(stage) = status.current_stage_status_mut() {
                stage.completed_count = stage.target_agents.len() as u32;
                stage.failed_count = 0;
                stage.failed_executions = Vec::new();
                stage.completed_at = Some(chrono::Utc::now().timestamp());
            }

            status.updated_at = chrono::Utc::now().timestamp();

            if is_rolling_back {
                status.state = RolloutState::RolledBack;
            } else {
                // 正常完成时，推进到下一阶段
                status.current_stage_idx += 1;
                // 还需要判断是不是最后一个阶段
                if status.current_stage_idx == status.stages.len() as u64 {
                    status.state = RolloutState::Completed;
                } else {
                    status.state = RolloutState::Running;
                }
            }

            if let Err(e) = self.persist_rollout_to_jetstream(status).await {
                error!("Failed to persist status update: {}", e);
            }
        }
    }
}
