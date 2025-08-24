use crate::domain::models::rollout::RolloutConfig;
use oasis_core::error::CoreError;
/// Protobuf 转换函数模块
///
/// 负责在领域模型和Protobuf消息之间进行转换
use oasis_core::proto::task_target_msg::Target as ProtoTaskTarget;
use oasis_core::task::TaskTarget;
use oasis_core::types::TaskSpec;

/// 从 Protobuf TaskSpecMsg 转换为领域 TaskSpec
pub fn from_proto_task_spec(msg: &oasis_core::proto::TaskSpecMsg) -> Result<TaskSpec, CoreError> {
    let id: oasis_core::types::TaskId = if let Some(task_id) = &msg.id {
        if task_id.value.is_empty() {
            uuid::Uuid::new_v4().to_string().into()
        } else {
            task_id.value.clone().into()
        }
    } else {
        uuid::Uuid::new_v4().to_string().into()
    };

    // TaskTarget 必须存在 - 不再有垃圾默认值！
    let target = match msg.target.as_ref().and_then(|t| t.target.as_ref()) {
        Some(ProtoTaskTarget::Selector(s)) => {
            if s.is_empty() {
                return Err(CoreError::Internal {
                    message: "Selector cannot be empty".to_string(),
                });
            }
            TaskTarget::Selector(s.clone())
        }
        None => {
            return Err(CoreError::Internal {
                message: "TaskTarget is required".to_string(),
            });
        }
    };

    Ok(TaskSpec {
        id,
        command: msg.command.clone(),
        args: msg.args.clone(),
        env: msg.env.clone(),
        target,
        timeout_seconds: msg.timeout_seconds,
    })
}

/// 从领域 TaskSpec 转换为 Protobuf TaskSpecMsg
pub fn to_proto_task_spec(task: &TaskSpec) -> oasis_core::proto::TaskSpecMsg {
    use oasis_core::proto::{TaskTargetMsg, task_target_msg::Target as ProtoTaskTarget};

    let target_msg = match &task.target {
        TaskTarget::Agents(v) => TaskTargetMsg {
            // 直接转为 selector：agent_id in ["a","b",...]
            target: Some(ProtoTaskTarget::Selector(format!(
                "agent_id in [{}]",
                v.iter()
                    .map(|id| format!("\"{}\"", id))
                    .collect::<Vec<_>>()
                    .join(",")
            ))),
        },
        TaskTarget::Selector(s) => TaskTargetMsg {
            target: Some(ProtoTaskTarget::Selector(s.clone())),
        },
        TaskTarget::AllAgents => TaskTargetMsg {
            target: Some(ProtoTaskTarget::Selector("true".to_string())),
        },
    };

    oasis_core::proto::TaskSpecMsg {
        id: Some(oasis_core::proto::TaskId {
            value: task.id.to_string(),
        }),
        command: task.command.clone(),
        args: task.args.clone(),
        env: task.env.clone(),
        target: Some(target_msg),
        timeout_seconds: task.timeout_seconds,
    }
}

/// 从 Protobuf BatchSizeMsg 转换为领域 BatchSize
pub fn from_proto_batch_size(
    bs: &oasis_core::proto::BatchSizeMsg,
) -> Result<crate::domain::models::rollout::BatchSize, CoreError> {
    match &bs.kind {
        Some(oasis_core::proto::batch_size_msg::Kind::Percentage(p)) => {
            if *p <= 0.0 || *p > 100.0 {
                return Err(CoreError::Internal {
                    message: format!("Invalid percentage: {}. Must be between 0 and 100", p),
                });
            }
            Ok(crate::domain::models::rollout::BatchSize::Percentage(*p))
        }
        Some(oasis_core::proto::batch_size_msg::Kind::Count(c)) => {
            if *c == 0 {
                return Err(CoreError::Internal {
                    message: "Batch count cannot be zero".to_string(),
                });
            }
            Ok(crate::domain::models::rollout::BatchSize::Count(
                *c as usize,
            ))
        }
        None => Err(CoreError::Internal {
            message: "BatchSize kind is required".to_string(),
        }),
    }
}

/// 从领域 BatchSize 转换为 Protobuf BatchSizeMsg
pub fn to_proto_batch_size(
    bs: &crate::domain::models::rollout::BatchSize,
) -> oasis_core::proto::BatchSizeMsg {
    match bs {
        crate::domain::models::rollout::BatchSize::Percentage(p) => {
            oasis_core::proto::BatchSizeMsg {
                kind: Some(oasis_core::proto::batch_size_msg::Kind::Percentage(*p)),
            }
        }
        crate::domain::models::rollout::BatchSize::Count(c) => oasis_core::proto::BatchSizeMsg {
            kind: Some(oasis_core::proto::batch_size_msg::Kind::Count(*c as u32)),
        },
    }
}

/// 从 Protobuf RolloutConfigMsg 转换为领域 RolloutConfig
pub fn from_proto_rollout_config(
    msg: &oasis_core::proto::RolloutConfigMsg,
) -> Result<RolloutConfig, CoreError> {
    let strategy = match &msg.strategy {
        Some(oasis_core::proto::rollout_config_msg::Strategy::Canary(s)) => {
            if s.percentage <= 0.0 || s.percentage > 100.0 {
                return Err(CoreError::Internal {
                    message: format!(
                        "Invalid canary percentage: {}. Must be between 0 and 100",
                        s.percentage
                    ),
                });
            }
            crate::domain::models::rollout::RolloutStrategy::Canary {
                percentage: s.percentage,
                observation_duration_secs: s.observation_duration_secs as u64,
            }
        }
        Some(oasis_core::proto::rollout_config_msg::Strategy::Rolling(s)) => {
            let batch_size = match &s.batch_size {
                Some(bs) => from_proto_batch_size(bs)?,
                None => {
                    return Err(CoreError::Internal {
                        message: "Rolling strategy requires batch_size".to_string(),
                    });
                }
            };
            crate::domain::models::rollout::RolloutStrategy::Rolling {
                batch_size,
                batch_delay_secs: s.batch_delay_secs as u64,
                max_failures: s.max_failures as usize,
            }
        }
        Some(oasis_core::proto::rollout_config_msg::Strategy::BlueGreen(s)) => {
            if s.switch_percentage <= 0.0 || s.switch_percentage > 100.0 {
                return Err(CoreError::Internal {
                    message: format!(
                        "Invalid blue-green switch percentage: {}. Must be between 0 and 100",
                        s.switch_percentage
                    ),
                });
            }
            crate::domain::models::rollout::RolloutStrategy::BlueGreen {
                switch_percentage: s.switch_percentage,
                warmup_secs: s.warmup_secs as u64,
            }
        }
        None => {
            return Err(CoreError::Internal {
                message: "Rollout strategy is required".to_string(),
            });
        }
    };

    Ok(RolloutConfig {
        strategy,
        timeout_seconds: msg.timeout_seconds,
        auto_advance: msg.auto_advance,
        health_check: if msg.health_check.is_empty() {
            None
        } else {
            Some(msg.health_check.clone())
        },
        labels: msg.labels.clone(),
    })
}

/// 从领域 RolloutConfig 转换为 Protobuf RolloutConfigMsg
pub fn to_proto_rollout_config(cfg: &RolloutConfig) -> oasis_core::proto::RolloutConfigMsg {
    let strategy = match &cfg.strategy {
        crate::domain::models::rollout::RolloutStrategy::Canary {
            percentage,
            observation_duration_secs,
        } => Some(oasis_core::proto::rollout_config_msg::Strategy::Canary(
            oasis_core::proto::CanaryStrategyMsg {
                percentage: *percentage,
                observation_duration_secs: *observation_duration_secs,
            },
        )),
        crate::domain::models::rollout::RolloutStrategy::Rolling {
            batch_size,
            batch_delay_secs,
            max_failures,
        } => Some(oasis_core::proto::rollout_config_msg::Strategy::Rolling(
            oasis_core::proto::RollingStrategyMsg {
                batch_size: Some(to_proto_batch_size(batch_size)),
                batch_delay_secs: *batch_delay_secs,
                max_failures: *max_failures as u32,
            },
        )),
        crate::domain::models::rollout::RolloutStrategy::BlueGreen {
            switch_percentage,
            warmup_secs,
        } => Some(oasis_core::proto::rollout_config_msg::Strategy::BlueGreen(
            oasis_core::proto::BlueGreenStrategyMsg {
                switch_percentage: *switch_percentage,
                warmup_secs: *warmup_secs,
            },
        )),
    };

    oasis_core::proto::RolloutConfigMsg {
        strategy,
        timeout_seconds: cfg.timeout_seconds,
        auto_advance: cfg.auto_advance,
        health_check: cfg.health_check.clone().unwrap_or_default(),
        labels: cfg.labels.clone(),
    }
}

/// 从领域 RolloutProgress 转换为 Protobuf RolloutProgressMsg
pub fn to_proto_progress(
    p: &crate::domain::models::rollout::RolloutProgress,
) -> oasis_core::proto::RolloutProgressMsg {
    oasis_core::proto::RolloutProgressMsg {
        total_nodes: p.total_nodes as u32,
        processed_nodes: p.processed_nodes as u32,
        successful_nodes: p.successful_nodes as u32,
        failed_nodes: p.failed_nodes as u32,
        completion_rate: p.completion_rate,
        current_batch: p.current_batch.unwrap_or(0) as u32,
        total_batches: p.total_batches as u32,
    }
}

/// 从领域 BatchResult 转换为 Protobuf BatchResultMsg
pub fn to_proto_batch_result(
    b: &crate::domain::models::rollout::BatchResult,
) -> oasis_core::proto::BatchResultMsg {
    oasis_core::proto::BatchResultMsg {
        batch_index: b.batch_index as u32,
        node_count: b.node_count as u32,
        successful_count: b.successful_count as u32,
        failed_count: b.failed_count as u32,
        duration_secs: b.duration_secs,
        completed_at: b.completed_at,
    }
}

/// 从领域 Rollout 转换状态相关的 Protobuf 消息
pub fn to_proto_state(
    r: &crate::domain::models::rollout::Rollout,
) -> (
    oasis_core::proto::RolloutStateEnum,
    oasis_core::proto::RolloutStateDataMsg,
) {
    use crate::domain::models::rollout::RolloutState as S;
    match &r.state {
        S::Created => (
            oasis_core::proto::RolloutStateEnum::RollCreated,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: 0,
                batch_completed_at: 0,
                reason: String::new(),
                error: String::new(),
            },
        ),
        S::RunningBatch { current_batch } => (
            oasis_core::proto::RolloutStateEnum::RollRunningBatch,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: *current_batch as u32,
                batch_completed_at: 0,
                reason: String::new(),
                error: String::new(),
            },
        ),
        S::WaitingForNextBatch { batch_completed_at } => (
            oasis_core::proto::RolloutStateEnum::RollWaitingNext,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: *batch_completed_at,
                reason: String::new(),
                error: String::new(),
            },
        ),
        S::Paused { reason } => (
            oasis_core::proto::RolloutStateEnum::RollPaused,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: reason.clone(),
                error: String::new(),
            },
        ),
        S::Succeeded => (
            oasis_core::proto::RolloutStateEnum::RollSucceeded,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: String::new(),
                error: String::new(),
            },
        ),
        S::Failed { error } => (
            oasis_core::proto::RolloutStateEnum::RollFailed,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: String::new(),
                error: error.clone(),
            },
        ),
        S::Aborted { reason } => (
            oasis_core::proto::RolloutStateEnum::RollAborted,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: reason.clone(),
                error: String::new(),
            },
        ),
        S::RollingBack { reason } => (
            oasis_core::proto::RolloutStateEnum::RollRollingBack,
            oasis_core::proto::RolloutStateDataMsg {
                current_batch: r.progress.current_batch.unwrap_or(0) as u32,
                batch_completed_at: 0,
                reason: reason.clone(),
                error: String::new(),
            },
        ),
    }
}

/// 从领域 Rollout 转换为完整的 Protobuf RolloutMsg
pub fn to_proto_rollout(
    r: &crate::domain::models::rollout::Rollout,
) -> oasis_core::proto::RolloutMsg {
    let (state, state_data) = to_proto_state(r);

    // 构建 TaskTargetMsg - 使用 target_selector 字段
    let target_msg = oasis_core::proto::TaskTargetMsg {
        target: Some(oasis_core::proto::task_target_msg::Target::Selector(
            r.target_selector.clone(),
        )),
    };

    oasis_core::proto::RolloutMsg {
        id: Some(oasis_core::proto::RolloutId {
            value: r.id.clone(),
        }),
        name: r.name.clone(),
        task: Some(to_proto_task_spec(&r.task)),
        target: Some(target_msg),
        config: Some(to_proto_rollout_config(&r.config)),
        state: state as i32,
        state_data: Some(state_data),
        progress: Some(to_proto_progress(&r.progress)),
        batch_results: r.batch_results.iter().map(to_proto_batch_result).collect(),
        processed_nodes: r
            .processed_nodes
            .iter()
            .map(|id| oasis_core::proto::AgentId { value: id.clone() })
            .collect(),
        current_batch_tasks: r
            .current_batch_tasks
            .iter()
            .map(|(k, v)| (k.clone(), oasis_core::proto::TaskId { value: v.clone() }))
            .collect(),
        current_batch_started_at: r.current_batch_started_at.unwrap_or(0),
        cached_target_nodes: r
            .cached_target_nodes
            .as_ref()
            .map(|nodes| {
                nodes
                    .iter()
                    .map(|id| oasis_core::proto::AgentId { value: id.clone() })
                    .collect()
            })
            .unwrap_or_default(),
        created_at: r.created_at,
        updated_at: r.updated_at,
        version: r.version,
    }
}
