//! Rollout gRPC handlers - 灰度发布处理器

use oasis_core::core_types::SelectorExpression;
use std::time::Duration;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

use oasis_core::core_types::RolloutId;
use oasis_core::proto;
use oasis_core::rollout_types::*;
use oasis_core::task_types::BatchRequest;

use crate::infrastructure::services::file_service::FileService;
use crate::infrastructure::services::rollout_service::RolloutService;
use crate::infrastructure::services::task_service::TaskService;
use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct RolloutHandlers;

impl RolloutHandlers {
    /// 执行一次推进（基于服务Arc，便于在后台任务中复用）
    async fn advance_once_with_services(
        rollout_service: std::sync::Arc<RolloutService>,
        task_service: std::sync::Arc<TaskService>,
        file_service: std::sync::Arc<FileService>,
        rollout_id: &RolloutId,
    ) -> std::result::Result<(), Status> {
        // 获取下一阶段信息
        let stage_info = rollout_service
            .get_next_stage_info(rollout_id)
            .await
            .map_err(map_core_error)?
            .ok_or_else(|| Status::failed_precondition("Rollout已完成所有阶段"))?;

        let (stage_index, target_agents, task_type) = stage_info;

        // 根据任务类型构建并执行
        let maybe_batch_id = match &task_type {
            RolloutTaskType::Command {
                command,
                args,
                timeout_seconds,
            } => {
                let agent_ids: Vec<String> =
                    target_agents.iter().map(|id| id.to_string()).collect();
                let selector = format!("agent_id in [{}]", agent_ids.join(","));

                let batch_request = BatchRequest {
                    command: command.clone(),
                    args: args.clone(),
                    selector: SelectorExpression::from(selector),
                    timeout_seconds: *timeout_seconds,
                };

                let bid = task_service
                    .submit_batch(batch_request, target_agents)
                    .await
                    .map_err(map_core_error)?;
                Some(bid)
            }
            RolloutTaskType::FileDeployment { config } => {
                let file_config = proto::FileConfigMsg {
                    source_path: config.source_path.clone(),
                    destination_path: config.destination_path.clone(),
                    revision: config.revision,
                    owner: config.owner.clone().unwrap_or_default(),
                    mode: config.mode.clone().unwrap_or_default(),
                    target: Some(proto::SelectorExpression {
                        expression: format!(
                            "agent_id in [{}]",
                            target_agents
                                .iter()
                                .map(|id| id.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        ),
                    }),
                };

                // 文件部署（按约定默认成功）
                file_service
                    .apply(&file_config, target_agents.clone())
                    .await
                    .map_err(map_core_error)?;

                // 保存文件版本快照
                let version_snapshot =
                    VersionSnapshot::new_file_snapshot(config.clone(), Some(config.revision));
                rollout_service
                    .save_version_snapshot(rollout_id, stage_index, version_snapshot)
                    .await
                    .map_err(map_core_error)?;

                // 文件部署为同步完成，直接标记阶段完成并推进
                rollout_service
                    .mark_file_stage_completed(rollout_id, stage_index)
                    .await
                    .map_err(map_core_error)?;

                None
            }
        };

        // 命令任务：标记阶段开始（推进到下一阶段）
        if let Some(batch_id) = maybe_batch_id {
            rollout_service
                .mark_stage_started(rollout_id, batch_id.clone())
                .await
                .map_err(map_core_error)?;
        }

        Ok(())
    }
    /// 创建灰度发布
    #[instrument(skip_all)]
    pub async fn create_rollout(
        srv: &OasisServer,
        request: Request<proto::CreateRolloutRequest>,
    ) -> std::result::Result<Response<proto::CreateRolloutResponse>, Status> {
        let proto_request = request.into_inner();

        // 验证请求
        if proto_request.name.trim().is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        // 转换策略
        let strategy = match proto_request.strategy {
            Some(strategy_msg) => match strategy_msg.strategy {
                Some(proto::rollout_strategy_msg::Strategy::Percentage(p)) => {
                    RolloutStrategy::Percentage {
                        stages: p.stages.into_iter().map(|s| s as u8).collect(),
                    }
                }
                Some(proto::rollout_strategy_msg::Strategy::Count(c)) => {
                    RolloutStrategy::Count { stages: c.stages }
                }
                Some(proto::rollout_strategy_msg::Strategy::Groups(g)) => {
                    RolloutStrategy::Groups { groups: g.groups }
                }
                None => {
                    return Err(Status::invalid_argument("strategy is required"));
                }
            },
            None => {
                return Err(Status::invalid_argument("strategy is required"));
            }
        };

        // 转换任务类型
        let task_type = match proto_request.task_type {
            Some(task_type_msg) => match task_type_msg.task_type {
                Some(proto::rollout_task_type_msg::TaskType::Command(cmd)) => {
                    RolloutTaskType::Command {
                        command: cmd.command,
                        args: cmd.args,
                        timeout_seconds: cmd.timeout_seconds,
                    }
                }
                Some(proto::rollout_task_type_msg::TaskType::FileDeployment(file)) => {
                    let config = match file.config {
                        Some(config_msg) => {
                            oasis_core::file_types::FileConfig::try_from(config_msg).map_err(
                                |e| Status::invalid_argument(format!("Invalid file config: {}", e)),
                            )?
                        }
                        None => {
                            return Err(Status::invalid_argument("file config is required"));
                        }
                    };
                    RolloutTaskType::FileDeployment { config }
                }
                None => {
                    return Err(Status::invalid_argument("task_type is required"));
                }
            },
            None => {
                return Err(Status::invalid_argument("task_type is required"));
            }
        };

        // 转换目标选择器
        let target = match proto_request.target {
            Some(target_msg) => target_msg.expression,
            None => {
                return Err(Status::invalid_argument("target is required"));
            }
        };

        // 构建创建请求
        let create_request = CreateRolloutRequest {
            name: proto_request.name,
            target: target.clone().into(),
            strategy,
            task_type,
            auto_advance: proto_request.auto_advance,
            advance_interval_seconds: proto_request.advance_interval_seconds,
        };

        // 解析目标代理
        let result = srv
            .context()
            .agent_service
            .query(&target)
            .await
            .map_err(map_core_error)?;

        let target_agents = result.to_online_agents();
        let total_agents = target_agents.len() as i64;
        if target_agents.is_empty() {
            return Err(Status::failed_precondition(
                "No online agents match the target",
            ));
        }

        info!(
            "Creating rollout with {} target agents",
            target_agents.len()
        );

        // 创建发布
        let create_result = srv
            .context()
            .rollout_service
            .create_rollout(create_request, target_agents)
            .await;

        match create_result {
            Ok(rollout_id) => {
                let response = proto::CreateRolloutResponse {
                    rollout_id: Some(proto::RolloutId {
                        value: rollout_id.to_string(),
                    }),
                    total_agents,
                    success: true,
                    message: "Rollout created successfully".to_string(),
                };

                // 若开启 auto_advance，后台循环自动推进直到完成/失败/回滚
                if proto_request.auto_advance {
                    let rollout_service = srv.context().rollout_service.clone();
                    let task_service = srv.context().task_service.clone();
                    let file_service = srv.context().file_service.clone();
                    let rid = rollout_id.clone();
                    tokio::spawn(async move {
                        loop {
                            // 拉取最新状态
                            let status = match rollout_service.get_rollout_status(&rid).await {
                                Ok(s) => s,
                                Err(_) => break,
                            };

                            // 结束条件
                            if status.state == RolloutState::Failed
                                || status.state == RolloutState::RollingBack
                                || status.state == RolloutState::RollbackFailed
                                || status.is_completed()
                            {
                                break;
                            }

                            // 不可推进则结束
                            if !status.can_advance() {
                                break;
                            }

                            // 执行一次推进
                            if let Err(e) = RolloutHandlers::advance_once_with_services(
                                rollout_service.clone(),
                                task_service.clone(),
                                file_service.clone(),
                                &rid,
                            )
                            .await
                            {
                                warn!("auto_advance failed to advance: {}", e);
                                break;
                            }

                            // 间隔
                            let interval = status.config.advance_interval_seconds.max(1);
                            tokio::time::sleep(Duration::from_secs(interval as u64)).await;
                        }
                    });
                }

                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to create rollout: {}", e);
                Err(map_core_error(e))
            }
        }
    }

    /// 获取灰度发布状态
    #[instrument(skip_all)]
    pub async fn get_rollout_status(
        srv: &OasisServer,
        request: Request<proto::GetRolloutStatusRequest>,
    ) -> std::result::Result<Response<proto::GetRolloutStatusResponse>, Status> {
        let proto_request = request.into_inner();

        let rollout_id = match proto_request.rollout_id {
            Some(id) => RolloutId::from(id.value),
            None => {
                return Err(Status::invalid_argument("rollout_id is required"));
            }
        };

        match srv
            .context()
            .rollout_service
            .get_rollout_status(&rollout_id)
            .await
        {
            Ok(status) => {
                let response = proto::GetRolloutStatusResponse {
                    status: Some(proto::RolloutStatusMsg::from(status)),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to get rollout status: {}", e);
                Err(map_core_error(e))
            }
        }
    }

    /// 列出灰度发布
    #[instrument(skip_all)]
    pub async fn list_rollouts(
        srv: &OasisServer,
        request: Request<proto::ListRolloutsRequest>,
    ) -> std::result::Result<Response<proto::ListRolloutsResponse>, Status> {
        let proto_request = request.into_inner();

        let state_filter = if proto_request.states.is_empty() {
            None
        } else {
            Some(
                proto_request
                    .states
                    .into_iter()
                    .map(RolloutState::from)
                    .collect(),
            )
        };

        match srv
            .context()
            .rollout_service
            .list_rollouts(proto_request.limit, state_filter)
            .await
        {
            Ok(rollouts) => {
                let total_count = rollouts.len() as u32;
                let response = proto::ListRolloutsResponse {
                    rollouts: rollouts
                        .into_iter()
                        .map(proto::RolloutStatusMsg::from)
                        .collect(),
                    total_count,
                    has_more: false,
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to list rollouts: {}", e);
                Err(map_core_error(e))
            }
        }
    }

    /// 推进灰度发布
    #[instrument(skip_all)]
    pub async fn advance_rollout(
        srv: &OasisServer,
        request: Request<proto::AdvanceRolloutRequest>,
    ) -> std::result::Result<Response<proto::AdvanceRolloutResponse>, Status> {
        let proto_request = request.into_inner();

        let rollout_id = match proto_request.rollout_id {
            Some(id) => RolloutId::from(id.value),
            None => {
                return Err(Status::invalid_argument("rollout_id is required"));
            }
        };

        info!("Advancing rollout: {}", rollout_id);

        // 复用内部推进实现
        let rollout_service = srv.context().rollout_service.clone();
        let task_service = srv.context().task_service.clone();
        let file_service = srv.context().file_service.clone();
        if let Err(e) = Self::advance_once_with_services(
            rollout_service,
            task_service,
            file_service,
            &rollout_id,
        )
        .await
        {
            return Err(e);
        }
        let response = proto::AdvanceRolloutResponse {
            success: true,
            message: "已推进到下一阶段".to_string(),
            next_stage: None,
        };
        Ok(Response::new(response))
    }

    /// 回滚灰度发布 - 完整实现
    #[instrument(skip_all)]
    pub async fn rollback_rollout(
        srv: &OasisServer,
        request: Request<proto::RollbackRolloutRequest>,
    ) -> std::result::Result<Response<proto::RollbackRolloutResponse>, Status> {
        let proto_request = request.into_inner();

        let rollout_id = match proto_request.rollout_id {
            Some(id) => RolloutId::from(id.value),
            None => {
                return Err(Status::invalid_argument("rollout_id is required"));
            }
        };

        // 1) 回滚开始：校验并置 RollingBack，持久化；命令回滚设置 current_action
        srv.context()
            .rollout_service
            .mark_rollback_started(&rollout_id)
            .await
            .map_err(map_core_error)?;

        // 若是命令回滚，记录当前动作
        if let Some(cmd) = proto_request.rollback_command.clone() {
            let _ = srv
                .context()
                .rollout_service
                .set_current_action(&rollout_id, Some(format!("rollback: {}", cmd)))
                .await;
        }

        // 2) 选择回滚阶段与目标 agents
        let (stage_index, target_agents, task_type, version_snapshot) = srv
            .context()
            .rollout_service
            .get_rollback_stage_info(&rollout_id)
            .await
            .map_err(map_core_error)?
            .ok_or_else(|| Status::failed_precondition("没有找到可回滚的阶段"))?;

        // 若该阶段有失败任务，仅选择失败的 agents
        let agents = {
            // 直接从状态缓存中读取失败任务的 agent
            let status = srv
                .context()
                .rollout_service
                .get_rollout_status(&rollout_id)
                .await
                .map_err(map_core_error)?;
            if let Some(stage) = status.stages.get(stage_index as usize) {
                if stage.failed_count > 0 && !stage.failed_executions.is_empty() {
                    stage
                        .failed_executions
                        .iter()
                        .map(|e| e.agent_id.clone())
                        .collect::<Vec<_>>()
                } else {
                    target_agents.clone()
                }
            } else {
                target_agents.clone()
            }
        };

        // 3) 按类型执行回滚
        let message = match &task_type {
            RolloutTaskType::Command { .. } => {
                let rollback_cmd = proto_request
                    .rollback_command
                    .ok_or_else(|| Status::invalid_argument("命令回滚需要提供 rollback_command"))?;

                // 提交批次
                let batch_request = oasis_core::task_types::BatchRequest {
                    command: rollback_cmd.clone(),
                    args: vec![],
                    selector: SelectorExpression::from(format!(
                        "agent_id in [{}]",
                        agents
                            .iter()
                            .map(|id| format!("\"{}\"", id))
                            .collect::<Vec<_>>()
                            .join(",")
                    )),
                    timeout_seconds: 300,
                };

                let batch_id = srv
                    .context()
                    .task_service
                    .submit_batch(batch_request, agents.clone())
                    .await
                    .map_err(map_core_error)?;

                // 关联到回滚阶段（不推进 current_stage）并持久化
                srv.context()
                    .rollout_service
                    .mark_rollback_stage_started(&rollout_id, stage_index, batch_id.clone())
                    .await
                    .map_err(map_core_error)?;

                // 监控：使用 backoff 直到批次完成
                let rollout_service = srv.context().rollout_service.clone();
                let task_monitor = srv.context().task_service.monitor().clone();
                let rid = rollout_id.clone();
                tokio::spawn(async move {
                    let backoff = oasis_core::backoff::network_publish_backoff();
                    let check_once = || async {
                        if let Some(task_ids) = task_monitor.get_batch_task_ids(&batch_id) {
                            let mut all_done = true;
                            let mut any_failed = false;
                            for task_id in task_ids {
                                if let Some(exec) =
                                    task_monitor.latest_execution_from_cache(&task_id)
                                {
                                    match exec.state {
                                        oasis_core::task_types::TaskState::Success => {}
                                        oasis_core::task_types::TaskState::Failed => {
                                            any_failed = true;
                                        }
                                        _ => {
                                            all_done = false;
                                        }
                                    }
                                } else {
                                    all_done = false;
                                }
                            }
                            if all_done {
                                if any_failed {
                                    let _ = rollout_service
                                        .mark_rollback_failed(
                                            &rid,
                                            "部分回滚任务执行失败".to_string(),
                                        )
                                        .await;
                                } else {
                                    let _ = rollout_service.mark_rollback_completed(&rid).await;
                                }
                                return Ok::<(), anyhow::Error>(());
                            }
                        }
                        Err(anyhow::anyhow!("rollback not completed"))
                    };

                    let _ = oasis_core::backoff::execute_with_backoff(check_once, backoff)
                        .await
                        .map_err(|_| ());
                });

                format!(
                    "回滚命令已提交，影响 {} 个Agent，命令: {}",
                    agents.len(),
                    rollback_cmd
                )
            }
            RolloutTaskType::FileDeployment { .. } => {
                // 从快照取上一版本
                if let Some(snapshot) = &version_snapshot {
                    if let SnapshotData::FileSnapshot {
                        file_config,
                        previous_revision,
                    } = &snapshot.snapshot_data
                    {
                        if let Some(prev) = previous_revision {
                            let cfg = proto::FileConfigMsg {
                                source_path: file_config.source_path.clone(),
                                destination_path: file_config.destination_path.clone(),
                                revision: *prev,
                                owner: file_config.owner.clone().unwrap_or_default(),
                                mode: file_config.mode.clone().unwrap_or_default(),
                                target: Some(proto::SelectorExpression {
                                    expression: format!(
                                        "agent_id in [{}]",
                                        agents
                                            .iter()
                                            .map(|id| id.to_string())
                                            .collect::<Vec<_>>()
                                            .join(",")
                                    ),
                                }),
                            };
                            // 执行回滚
                            srv.context()
                                .file_service
                                .apply(&cfg, agents.clone())
                                .await
                                .map_err(map_core_error)?;
                            srv.context()
                                .rollout_service
                                .mark_rollback_completed(&rollout_id)
                                .await
                                .map_err(map_core_error)?;
                            format!("文件回滚成功，影响 {} 个Agent，版本 {}", agents.len(), prev)
                        } else {
                            return Err(Status::failed_precondition(
                                "缺少 previous_revision，无法回滚",
                            ));
                        }
                    } else {
                        return Err(Status::failed_precondition(
                            "版本快照类型不匹配，无法文件回滚",
                        ));
                    }
                } else {
                    return Err(Status::failed_precondition(
                        "缺少版本快照信息，无法文件回滚",
                    ));
                }
            }
        };

        Ok(Response::new(proto::RollbackRolloutResponse {
            success: true,
            message,
        }))
    }
}
