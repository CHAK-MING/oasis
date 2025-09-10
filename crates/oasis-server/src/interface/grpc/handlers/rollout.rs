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
    /// 执行一次推进
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
            .ok_or_else(|| Status::failed_precondition("Rollout 已完成所有阶段"))?;

        let (target_agents, task_type) = stage_info;

        // 根据任务类型构建并执行
        let mut batch_id = None;
        let mut version_snapshot = None;
        match &task_type {
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

                batch_id = Some(
                    task_service
                        .submit_batch(batch_request, target_agents)
                        .await
                        .map_err(map_core_error)?,
                );
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
                version_snapshot = Some(VersionSnapshot::new_file_snapshot(
                    config.clone(),
                    Some(config.revision),
                ));
            }
        };

        rollout_service
            .mark_advance_next_stage(
                rollout_id,
                task_type.clone(),
                batch_id.clone(),
                version_snapshot.clone(),
            )
            .await
            .map_err(map_core_error)?;

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
            return Err(Status::failed_precondition("没有在线的Agent匹配目标"));
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
                    message: "灰度发布创建成功".to_string(),
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
                                || status.state == RolloutState::Completed
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

        //  获取回滚阶段与目标 agents 信息
        let (target_agents, task_type, version_snapshot) = srv
            .context()
            .rollout_service
            .get_rollback_stage_info(&rollout_id)
            .await
            .map_err(map_core_error)?
            .ok_or_else(|| Status::failed_precondition("没有找到可回滚的阶段"))?;

        // 按类型执行回滚
        let mut batch_id = None;
        let message = match &task_type {
            RolloutTaskType::Command { .. } => {
                // 提交批次
                let batch_request = oasis_core::task_types::BatchRequest {
                    command: proto_request.rollback_command.clone().unwrap_or_default(),
                    args: vec![],
                    selector: SelectorExpression::from(format!(
                        "agent_id in [{}]",
                        target_agents
                            .iter()
                            .map(|id| format!("\"{}\"", id))
                            .collect::<Vec<_>>()
                            .join(",")
                    )),
                    timeout_seconds: 300,
                };

                batch_id = Some(
                    srv.context()
                        .task_service
                        .submit_batch(batch_request, target_agents.clone())
                        .await
                        .map_err(map_core_error)?,
                );

                format!(
                    "回滚命令已提交，影响 {} 个Agent，命令: {}",
                    target_agents.len(),
                    proto_request.rollback_command.clone().unwrap_or_default()
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
                                        target_agents
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
                                .rollback_file(&cfg, target_agents.clone())
                                .await
                                .map_err(map_core_error)?;

                            format!(
                                "文件回滚成功，影响 {} 个Agent，版本 {}",
                                target_agents.len(),
                                prev
                            )
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

        srv.context()
            .rollout_service
            .mark_rollback_stage(
                &rollout_id,
                task_type,
                proto_request.rollback_command.clone(),
                batch_id,
            )
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(proto::RollbackRolloutResponse {
            success: true,
            message,
        }))
    }
}
