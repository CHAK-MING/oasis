//! Rollout gRPC handlers - 灰度发布处理器

use oasis_core::core_types::SelectorExpression;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

use oasis_core::core_types::RolloutId;
use oasis_core::proto;
use oasis_core::rollout_types::*;
use oasis_core::task_types::BatchRequest;

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct RolloutHandlers;

impl RolloutHandlers {
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
                    RolloutTaskType::FileDeployment {
                        config,
                        uploaded_file_name: file.uploaded_file_name,
                    }
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
        match srv
            .context()
            .rollout_service
            .create_rollout(create_request, target_agents)
            .await
        {
            Ok(rollout_id) => {
                let response = proto::CreateRolloutResponse {
                    rollout_id: Some(proto::RolloutId {
                        value: rollout_id.to_string(),
                    }),
                    total_agents,
                    success: true,
                    message: "Rollout created successfully".to_string(),
                };
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

    /// 推进灰度发布 - 关键实现：在 handlers 层协调多个服务
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

        // 步骤1: 获取下一阶段信息
        let stage_info = match srv
            .context()
            .rollout_service
            .get_next_stage_info(&rollout_id)
            .await
        {
            Ok(Some(info)) => info,
            Ok(None) => {
                return Ok(Response::new(proto::AdvanceRolloutResponse {
                    success: true,
                    message: "Rollout已完成所有阶段".to_string(),
                    next_stage: None,
                }));
            }
            Err(e) => {
                warn!("Failed to get next stage info: {}", e);
                return Err(map_core_error(e));
            }
        };

        let (stage_index, target_agents, task_type) = stage_info;

        info!(
            "Advancing to stage {} with {} agents",
            stage_index,
            target_agents.len()
        );

        // 步骤2: 根据任务类型构建批次请求并执行
        let batch_id = match &task_type {
            RolloutTaskType::Command {
                command,
                args,
                timeout_seconds,
            } => {
                // 构建命令批次请求
                let agent_ids: Vec<String> =
                    target_agents.iter().map(|id| id.to_string()).collect();
                let selector = format!("agent_id in [{}]", agent_ids.join(","));

                let batch_request = BatchRequest {
                    command: command.clone(),
                    args: args.clone(),
                    selector: SelectorExpression::from(selector),
                    timeout_seconds: *timeout_seconds,
                };

                // 提交命令批次任务
                srv.context()
                    .task_service
                    .submit_batch(batch_request, target_agents)
                    .await
                    .map_err(map_core_error)?
            }
            RolloutTaskType::FileDeployment {
                config,
                uploaded_file_name,
            } => {
                // 构建文件部署的Proto配置
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

                // 调用文件服务进行文件部署
                match srv
                    .context()
                    .file_service
                    .apply(&file_config, target_agents.clone())
                    .await
                {
                    Ok(_) => {
                        // 文件部署成功，但是没有返回BatchId，我们需要创建一个虚拟的批次来跟踪状态
                        // 为了保持一致性，我们创建一个命令批次来跟踪文件部署的状态
                        let agent_ids: Vec<String> =
                            target_agents.iter().map(|id| id.to_string()).collect();
                        let selector = format!("agent_id in [{}]", agent_ids.join(","));

                        let tracking_batch_request = BatchRequest {
                            command: "file_deployment_tracker".to_string(),
                            args: vec![uploaded_file_name.clone(), config.destination_path.clone()],
                            selector: SelectorExpression::from(selector),
                            timeout_seconds: 300, // 文件部署默认超时5分钟
                        };

                        srv.context()
                            .task_service
                            .submit_batch(tracking_batch_request, target_agents)
                            .await
                            .map_err(map_core_error)?
                    }
                    Err(e) => {
                        warn!("Failed to deploy file for rollout: {}", e);
                        return Err(map_core_error(e));
                    }
                }
            }
        };

        // 步骤3: 更新发布状态
        match srv
            .context()
            .rollout_service
            .mark_stage_started(&rollout_id, batch_id.clone())
            .await
        {
            Ok(executed_stage) => {
                info!(
                    "Rollout {} advanced to stage {} with batch {}",
                    rollout_id, executed_stage, batch_id
                );
                let response = proto::AdvanceRolloutResponse {
                    success: true,
                    message: format!("已推进到阶段 {}", executed_stage),
                    next_stage: Some(stage_index),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to mark stage started: {}", e);
                Err(map_core_error(e))
            }
        }
    }

    /// 回滚灰度发布 - TODO: 需要实现完整的回滚逻辑
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

        info!("Rolling back rollout: {}", rollout_id);

        // TODO: 实现完整的回滚逻辑
        //
        // 回滚策略应该根据任务类型来决定：
        //
        // 1. 文件部署回滚：
        //    - 需要在FileService中实现文件版本管理和备份机制
        //    - 回滚时恢复到部署前的文件状态
        //    - 调用 FileService.rollback_file()
        //
        // 2. 命令执行回滚：
        //    - 用户需要在创建时提供rollback_command
        //    - 或者使用请求中的rollback_command
        //    - 对已完成的Agent执行回滚命令
        //
        // 3. 混合策略：
        //    - 根据每个阶段的任务类型选择对应的回滚方式
        //    - 需要记录每个阶段的执行历史和状态快照
        //
        // 当前实现：只获取已完成的Agent并简单标记为回滚状态

        // 获取已完成的代理列表
        let completed_agents = match srv
            .context()
            .rollout_service
            .get_completed_agents(&rollout_id)
            .await
        {
            Ok(agents) => agents,
            Err(e) => {
                warn!("Failed to get completed agents: {}", e);
                return Err(map_core_error(e));
            }
        };

        if completed_agents.is_empty() {
            return Ok(Response::new(proto::RollbackRolloutResponse {
                success: true,
                message: "没有需要回滚的代理".to_string(),
            }));
        }

        // 简单的回滚命令实现（实际应该根据任务类型和回滚策略来决定）
        let rollback_command = proto_request
            .rollback_command
            .unwrap_or_else(|| "echo 'rollback completed - placeholder'".to_string());

        info!(
            "Would rollback {} agents with command: {} (TODO: implement proper rollback)",
            completed_agents.len(),
            rollback_command
        );

        // 标记为已回滚（实际应该在执行回滚任务后再标记）
        match srv
            .context()
            .rollout_service
            .rollback_rollout(&rollout_id, Some(rollback_command))
            .await
        {
            Ok(()) => {
                let response = proto::RollbackRolloutResponse {
                    success: true,
                    message: format!(
                        "回滚已标记完成，涉及 {} 个代理 (TODO: 实现真正的回滚执行)",
                        completed_agents.len()
                    ),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("Failed to rollback rollout: {}", e);
                Err(map_core_error(e))
            }
        }
    }
}
