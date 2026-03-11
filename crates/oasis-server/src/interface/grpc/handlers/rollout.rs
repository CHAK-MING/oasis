//! Rollout gRPC handlers - 灰度发布处理器

use std::time::Duration;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

use oasis_core::core_types::RolloutId;
use oasis_core::proto;
use oasis_core::rollout_types::*;
use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct RolloutHandlers;

impl RolloutHandlers {
    fn spawn_auto_advance_loop(
        rollout_service: std::sync::Arc<crate::infrastructure::services::rollout_service::RolloutService>,
        task_service: std::sync::Arc<crate::infrastructure::services::task_service::TaskService>,
        file_service: std::sync::Arc<crate::infrastructure::services::file_service::FileService>,
        rollout_id: RolloutId,
    ) {
        tokio::spawn(async move {
            loop {
                let status = match rollout_service.get_rollout_status(&rollout_id).await {
                    Ok(s) => s,
                    Err(_) => break,
                };

                if status.state == RolloutState::Failed && status.config.auto_rollback {
                    if let Err(e) = rollout_service
                        .rollback_once(&rollout_id, None, &task_service, &file_service)
                        .await
                    {
                        warn!("auto_rollback failed: {}", e);
                    }
                    break;
                }

                if matches!(
                    status.state,
                    RolloutState::Failed
                        | RolloutState::RollingBack
                        | RolloutState::RollbackFailed
                        | RolloutState::Completed
                        | RolloutState::RolledBack
                        | RolloutState::Paused
                ) {
                    break;
                }

                if !status.can_advance() {
                    break;
                }

                if let Err(e) = rollout_service
                    .advance_once(&rollout_id, &task_service, &file_service)
                    .await
                {
                    warn!("auto_advance failed to advance: {}", e);
                    break;
                }

                let interval = status.config.advance_interval_seconds.max(1);
                tokio::time::sleep(Duration::from_secs(interval as u64)).await;
            }
        });
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
            stage_timeout_seconds: proto_request.stage_timeout_seconds.max(1),
            max_failure_rate_percent: proto_request.max_failure_rate_percent,
            auto_rollback: proto_request.auto_rollback,
            rollback_command: proto_request.rollback_command,
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
                    Self::spawn_auto_advance_loop(
                        srv.context().rollout_service.clone(),
                        srv.context().task_service.clone(),
                        srv.context().file_service.clone(),
                        rollout_id.clone(),
                    );
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
        srv.context()
            .rollout_service
            .advance_once(
                &rollout_id,
                srv.context().task_service.as_ref(),
                srv.context().file_service.as_ref(),
            )
            .await
            .map_err(map_core_error)?;
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

        let (success, message) = srv
            .context()
            .rollout_service
            .rollback_once(
                &rollout_id,
                proto_request.rollback_command.clone(),
                srv.context().task_service.as_ref(),
                srv.context().file_service.as_ref(),
            )
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(proto::RollbackRolloutResponse {
            success,
            message,
        }))
    }

    #[instrument(skip_all)]
    pub async fn pause_rollout(
        srv: &OasisServer,
        request: Request<proto::PauseRolloutRequest>,
    ) -> std::result::Result<Response<proto::PauseRolloutResponse>, Status> {
        let proto_request = request.into_inner();
        let rollout_id = match proto_request.rollout_id {
            Some(id) => RolloutId::from(id.value),
            None => return Err(Status::invalid_argument("rollout_id is required")),
        };

        srv.context()
            .rollout_service
            .pause_rollout(&rollout_id)
            .await
            .map_err(map_core_error)?;

        Ok(Response::new(proto::PauseRolloutResponse {
            success: true,
            message: "发布已暂停".to_string(),
        }))
    }

    #[instrument(skip_all)]
    pub async fn resume_rollout(
        srv: &OasisServer,
        request: Request<proto::ResumeRolloutRequest>,
    ) -> std::result::Result<Response<proto::ResumeRolloutResponse>, Status> {
        let proto_request = request.into_inner();
        let rollout_id = match proto_request.rollout_id {
            Some(id) => RolloutId::from(id.value),
            None => return Err(Status::invalid_argument("rollout_id is required")),
        };

        let status = srv
            .context()
            .rollout_service
            .resume_rollout(&rollout_id)
            .await
            .map_err(map_core_error)?;

        if status.config.auto_advance {
            Self::spawn_auto_advance_loop(
                srv.context().rollout_service.clone(),
                srv.context().task_service.clone(),
                srv.context().file_service.clone(),
                rollout_id.clone(),
            );
        }

        Ok(Response::new(proto::ResumeRolloutResponse {
            success: true,
            message: "发布已恢复".to_string(),
        }))
    }
}
