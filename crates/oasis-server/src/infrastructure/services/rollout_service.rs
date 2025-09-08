//! 灰度发布服务
use crate::infrastructure::monitor::task_monitor::TaskMonitor;
use async_nats::jetstream::Context;
use dashmap::DashMap;
use futures::StreamExt;
use oasis_core::constants::JS_KV_ROLLOUTS;
use oasis_core::core_types::{AgentId, BatchId, RolloutId, SelectorExpression};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use oasis_core::rollout_types::*;
use oasis_core::task_types::{TaskExecution, TaskState};
use prost::Message;
use std::sync::Arc;
use tracing::{error, info, warn};

/// 灰度发布服务 - 负责状态管理和JetStream持久化
pub struct RolloutService {
    jetstream: Arc<Context>,
    task_monitor: Arc<TaskMonitor>,
    /// 内存中的发布状态缓存
    rollout_cache: Arc<DashMap<RolloutId, RolloutStatus>>,
    /// 仅用于展示的回滚行（不持久化）
    rollback_runtime_rows: Arc<DashMap<RolloutId, RolloutStageStatus>>,
}

impl RolloutService {
    pub async fn new(jetstream: Arc<Context>, task_monitor: Arc<TaskMonitor>) -> Result<Self> {
        info!("Initializing RolloutService");

        let service = Self {
            jetstream,
            task_monitor,
            rollout_cache: Arc::new(DashMap::new()),
            rollback_runtime_rows: Arc::new(DashMap::new()),
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
                                        match Self::proto_to_domain_status(proto_status) {
                                            Ok(status) => {
                                                let rollout_id = status.config.rollout_id.clone();
                                                self.rollout_cache
                                                    .insert(rollout_id.clone(), status);
                                                loaded_count += 1;
                                                info!(
                                                    "Loaded rollout {} from JetStream",
                                                    rollout_id
                                                );
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "Failed to convert proto rollout from key {}: {}",
                                                    key_str, e
                                                );
                                            }
                                        }
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
        let status = Self::create_rollout_status_with_smart_stages(config, all_target_agents);

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
            version_snapshot: None,
        };

        RolloutStatus {
            config,
            state: RolloutState::Created,
            current_stage: 0,
            stages: vec![stage],
            all_target_agents,
            updated_at: chrono::Utc::now().timestamp(),
            error_message: None,
            current_action: None,
        }
    }

    /// 获取灰度发布状态 - 优先内存缓存，后JetStream
    pub async fn get_rollout_status(&self, rollout_id: &RolloutId) -> Result<RolloutStatus> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            // 更新阶段状态
            self.update_stage_status_from_monitor(status.value_mut())
                .await;
            let mut out = status.value().clone();
            if let Some(row) = self.rollback_runtime_rows.get(rollout_id) {
                // 附加回滚行用于展示，不影响持久化状态
                out.stages.push(row.value().clone());
            }
            Ok(out)
        } else {
            // 从 JetStream 加载
            match self.load_rollout_from_jetstream(rollout_id).await {
                Ok(mut status) => {
                    // 更新状态并缓存
                    self.update_stage_status_from_monitor(&mut status).await;
                    self.rollout_cache
                        .insert(rollout_id.clone(), status.clone());
                    if let Some(row) = self.rollback_runtime_rows.get(rollout_id) {
                        let mut out = status.clone();
                        out.stages.push(row.value().clone());
                        Ok(out)
                    } else {
                        Ok(status)
                    }
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
            let status = Self::proto_to_domain_status(proto)?;
            Ok(status)
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    fn proto_to_domain_status(
        proto_status: oasis_core::proto::RolloutStatusMsg,
    ) -> Result<RolloutStatus> {
        // 转换 RolloutConfig
        let config_msg = proto_status.config.ok_or_else(|| CoreError::InvalidTask {
            reason: "missing rollout config".to_string(),
            severity: ErrorSeverity::Error,
        })?;

        // strategy
        let strategy = match config_msg.strategy.and_then(|s| s.strategy) {
            Some(oasis_core::proto::rollout_strategy_msg::Strategy::Percentage(p)) => {
                RolloutStrategy::Percentage {
                    stages: p.stages.into_iter().map(|v| v as u8).collect(),
                }
            }
            Some(oasis_core::proto::rollout_strategy_msg::Strategy::Count(c)) => {
                RolloutStrategy::Count { stages: c.stages }
            }
            Some(oasis_core::proto::rollout_strategy_msg::Strategy::Groups(g)) => {
                RolloutStrategy::Groups { groups: g.groups }
            }
            None => {
                return Err(CoreError::InvalidTask {
                    reason: "missing strategy".to_string(),
                    severity: ErrorSeverity::Error,
                });
            }
        };

        // task_type
        let task_type = match config_msg.task_type.and_then(|t| t.task_type) {
            Some(oasis_core::proto::rollout_task_type_msg::TaskType::Command(cmd)) => {
                RolloutTaskType::Command {
                    command: cmd.command,
                    args: cmd.args,
                    timeout_seconds: cmd.timeout_seconds,
                }
            }
            Some(oasis_core::proto::rollout_task_type_msg::TaskType::FileDeployment(file)) => {
                // 这里复用 FileConfigMsg -> FileConfig 的 TryFrom
                let cfg =
                    oasis_core::file_types::FileConfig::try_from(file.config.ok_or_else(|| {
                        CoreError::InvalidTask {
                            reason: "missing file config".to_string(),
                            severity: ErrorSeverity::Error,
                        }
                    })?)
                    .map_err(|e| CoreError::InvalidTask {
                        reason: format!("invalid file config: {}", e),
                        severity: ErrorSeverity::Error,
                    })?;
                RolloutTaskType::FileDeployment { config: cfg }
            }
            None => {
                return Err(CoreError::InvalidTask {
                    reason: "missing task_type".to_string(),
                    severity: ErrorSeverity::Error,
                });
            }
        };

        let rollout_config = RolloutConfig {
            rollout_id: RolloutId::from(
                config_msg
                    .rollout_id
                    .ok_or_else(|| CoreError::InvalidTask {
                        reason: "missing rollout_id".to_string(),
                        severity: ErrorSeverity::Error,
                    })?
                    .value,
            ),
            name: config_msg.name,
            target: SelectorExpression::from(
                config_msg
                    .target
                    .ok_or_else(|| CoreError::InvalidTask {
                        reason: "missing target".to_string(),
                        severity: ErrorSeverity::Error,
                    })?
                    .expression,
            ),
            strategy,
            task_type,
            auto_advance: config_msg.auto_advance,
            advance_interval_seconds: config_msg.advance_interval_seconds,
            created_at: config_msg.created_at,
        };

        // stages
        let mut stages: Vec<RolloutStageStatus> = Vec::new();
        for s in proto_status.stages {
            let target_agents: Vec<AgentId> = s
                .target_agents
                .into_iter()
                .map(|id| AgentId::from(id.value))
                .collect();
            let state = RolloutState::from(s.state);
            let failed_execs: Vec<TaskExecution> = s
                .failed_executions
                .into_iter()
                .map(TaskExecution::from)
                .collect();
            stages.push(RolloutStageStatus {
                stage_index: s.stage_index,
                stage_name: s.stage_name,
                target_agents,
                batch_id: s.batch_id.map(|b| BatchId::from(b.value)),
                started_count: s.started_count,
                completed_count: s.completed_count,
                failed_count: s.failed_count,
                state,
                started_at: s.started_at,
                completed_at: s.completed_at,
                failed_executions: failed_execs,
                version_snapshot: None, // 版本快照不在 proto 中持久化
            });
        }

        let all_target_agents: Vec<AgentId> = proto_status
            .all_target_agents
            .into_iter()
            .map(|id| AgentId::from(id.value))
            .collect();

        let status = RolloutStatus {
            config: rollout_config,
            state: RolloutState::from(proto_status.state),
            current_stage: proto_status.current_stage,
            stages,
            all_target_agents,
            updated_at: proto_status.updated_at,
            error_message: proto_status.error_message,
            current_action: proto_status.current_action,
        };
        Ok(status)
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

    /// 保存版本快照到指定阶段
    pub async fn save_version_snapshot(
        &self,
        rollout_id: &RolloutId,
        stage_index: u32,
        snapshot: VersionSnapshot,
    ) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();
            if let Some(stage) = status_val.stages.get_mut(stage_index as usize) {
                stage.version_snapshot = Some(snapshot);
                status_val.updated_at = chrono::Utc::now().timestamp();

                // 持久化到 JetStream
                if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                    warn!("Failed to persist version snapshot: {}", e);
                }

                info!(
                    "Saved version snapshot for rollout {} stage {}",
                    rollout_id, stage_index
                );
            }
        }
        Ok(())
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

                // 持久化到 JetStream
                if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                    warn!("Failed to persist stage started: {}", e);
                }

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

    /// 标记回滚开始：将状态置为 RollingBack 并持久化
    pub async fn mark_rollback_started(&self, rollout_id: &RolloutId) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();
            match status_val.state {
                RolloutState::Running
                | RolloutState::RollingBack
                | RolloutState::RollbackFailed => {
                    return Err(CoreError::InvalidTask {
                        reason: format!("Rollout {} 当前状态不允许回滚", rollout_id),
                        severity: ErrorSeverity::Error,
                    });
                }
                _ => {}
            }
            status_val.state = RolloutState::RollingBack;
            status_val.updated_at = chrono::Utc::now().timestamp();
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

    /// 设置当前动作（例如回滚命令），并持久化
    pub async fn set_current_action(
        &self,
        rollout_id: &RolloutId,
        action: Option<String>,
    ) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();
            status_val.current_action = action;
            status_val.updated_at = chrono::Utc::now().timestamp();
            if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                warn!("Failed to persist current_action: {}", e);
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

    /// 标记回滚阶段开始（不推进 current_stage），并持久化
    pub async fn mark_rollback_stage_started(
        &self,
        rollout_id: &RolloutId,
        stage_index: u32,
        batch_id: BatchId,
    ) -> Result<()> {
        if let Some(status) = self.rollout_cache.get(rollout_id) {
            // 构造一个只用于展示的“回滚行”
            let mut row = RolloutStageStatus {
                stage_index,
                stage_name: format!("阶段 {} 回滚", stage_index + 1),
                target_agents: status
                    .value()
                    .stages
                    .get(stage_index as usize)
                    .map(|s| s.target_agents.clone())
                    .unwrap_or_default(),
                batch_id: Some(batch_id),
                started_count: 0,
                completed_count: 0,
                failed_count: 0,
                state: RolloutState::Running,
                started_at: Some(chrono::Utc::now().timestamp()),
                completed_at: None,
                failed_executions: Vec::new(),
                version_snapshot: None,
            };
            // 用当前监控快照填充 counters（若有）
            if let Some(task_ids) = self
                .task_monitor
                .get_batch_task_ids(row.batch_id.as_ref().unwrap())
            {
                let mut started = 0u32;
                let mut completed = 0u32;
                let mut failed = 0u32;
                let mut failed_execs = Vec::new();
                for task_id in task_ids {
                    if let Some(exec) = self.task_monitor.latest_execution_from_cache(&task_id) {
                        match exec.state {
                            TaskState::Running => started += 1,
                            TaskState::Success => completed += 1,
                            TaskState::Failed => {
                                failed += 1;
                                failed_execs.push(exec);
                            }
                            _ => {}
                        }
                    }
                }
                row.started_count = started + completed + failed;
                row.completed_count = completed;
                row.failed_count = failed;
                row.failed_executions = failed_execs;
            }
            self.rollback_runtime_rows.insert(rollout_id.clone(), row);
            Ok(())
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    /// 标记回滚完成并聚合总体状态
    pub async fn mark_rollback_completed(&self, rollout_id: &RolloutId) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();
            // 聚合总体状态：是否还有未执行阶段
            if (status_val.current_stage as usize) < status_val.stages.len() {
                status_val.state = RolloutState::Running;
            } else if status_val
                .stages
                .iter()
                .all(|s| s.state == RolloutState::Completed)
            {
                status_val.state = RolloutState::Completed;
            } else {
                status_val.state = RolloutState::Running;
            }
            status_val.current_action = None;
            status_val.updated_at = chrono::Utc::now().timestamp();
            if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                warn!("Failed to persist rollback completed: {}", e);
            }
        }
        // 清除仅用于展示的回滚行
        self.rollback_runtime_rows.remove(rollout_id);
        Ok(())
    }

    /// 标记回滚失败
    pub async fn mark_rollback_failed(&self, rollout_id: &RolloutId, reason: String) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();
            status_val.state = RolloutState::RollbackFailed;
            status_val.error_message = Some(format!("回滚失败: {}", reason));
            status_val.current_action = None;
            status_val.updated_at = chrono::Utc::now().timestamp();
            if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                warn!("Failed to persist rollback failed: {}", e);
            }
        }
        // 清除仅用于展示的回滚行
        self.rollback_runtime_rows.remove(rollout_id);
        Ok(())
    }

    /// 获取需要回滚的阶段信息（返回阶段索引、目标 agents、任务类型、版本快照）
    pub async fn get_rollback_stage_info(
        &self,
        rollout_id: &RolloutId,
    ) -> Result<Option<(u32, Vec<AgentId>, RolloutTaskType, Option<VersionSnapshot>)>> {
        if let Some(status) = self.rollout_cache.get(rollout_id) {
            // 优先取最近执行的阶段（Running/Failed/Completed），否则无
            let prev_idx = status.current_stage.saturating_sub(1);
            if let Some(stage) = status.stages.get(prev_idx as usize) {
                if matches!(
                    stage.state,
                    RolloutState::Running | RolloutState::Failed | RolloutState::Completed
                ) {
                    return Ok(Some((
                        stage.stage_index,
                        stage.target_agents.clone(),
                        status.config.task_type.clone(),
                        stage.version_snapshot.clone(),
                    )));
                }
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

    /// 标记文件阶段完成（文件部署为同步操作，直接完成阶段并推进）
    pub async fn mark_file_stage_completed(
        &self,
        rollout_id: &RolloutId,
        stage_index: u32,
    ) -> Result<u32> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();

            // 安全检查：阶段索引有效
            if let Some(stage) = status_val.stages.get_mut(stage_index as usize) {
                // 仅当处于 Created 或 Running 时可置为 Completed
                stage.state = RolloutState::Completed;
                if stage.completed_at.is_none() {
                    stage.completed_at = Some(chrono::Utc::now().timestamp());
                }

                // 文件部署为同步完成：按目标节点数量统计完成数
                let total_targets = stage.target_agents.len() as u32;
                stage.started_count = total_targets;
                stage.completed_count = total_targets;
                stage.failed_count = 0;

                // 更新整体状态并推进 current_stage
                status_val.state = if (stage_index as usize) + 1 >= status_val.stages.len() {
                    // 这是最后一个阶段，整体完成
                    RolloutState::Completed
                } else {
                    // 仍有阶段待执行
                    RolloutState::Running
                };

                // 如果当前阶段等于传入索引，则推进
                if status_val.current_stage == stage_index {
                    status_val.current_stage += 1;
                }
                status_val.updated_at = chrono::Utc::now().timestamp();

                // 持久化到 JetStream
                if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                    warn!("Failed to persist file stage completed: {}", e);
                }

                info!(
                    "File stage {} of rollout {} marked completed",
                    stage_index, rollout_id
                );
                Ok(stage_index)
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

    /// 从 TaskMonitor 更新阶段状态
    async fn update_stage_status_from_monitor(&self, status: &mut RolloutStatus) {
        // 若处于回滚中，保持 RollingBack 状态，不用阶段失败覆盖
        if status.state == RolloutState::RollingBack {
            status.updated_at = chrono::Utc::now().timestamp();
            if let Err(e) = self.persist_rollout_to_jetstream(status).await {
                error!("Failed to persist status update: {}", e);
            }
            return;
        }

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

        // 持久化更新的状态
        if let Err(e) = self.persist_rollout_to_jetstream(status).await {
            error!("Failed to persist status update: {}", e);
        }
    }
}
