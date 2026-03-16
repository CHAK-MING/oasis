//! 灰度发布服务
use crate::infrastructure::monitor::task_monitor::TaskMonitor;
use crate::infrastructure::services::event_bus::EventBus;
use crate::infrastructure::services::file_service::FileService;
use crate::infrastructure::services::task_service::TaskService;
use async_nats::jetstream::Context;
use dashmap::DashMap;
use futures::StreamExt;
use oasis_core::constants::JS_KV_ROLLOUTS;
use oasis_core::core_types::{AgentId, BatchId, OperationId, RolloutId, SelectorExpression};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use oasis_core::event_types::{OasisEvent, OasisEventKind};
use oasis_core::file_types::FileApplyExecution;
use oasis_core::proto;
use oasis_core::rollout_types::*;
use oasis_core::task_types::BatchRequest;
use oasis_core::task_types::TaskState;
use prost::Message;
use std::sync::Arc;
use tracing::{error, info, warn};

/// 灰度发布服务 - 负责状态管理和JetStream持久化
pub struct RolloutService {
    jetstream: Arc<Context>,
    task_monitor: Arc<TaskMonitor>,
    event_bus: Option<Arc<EventBus>>,
    /// 内存中的发布状态缓存
    rollout_cache: Arc<DashMap<RolloutId, RolloutStatus>>,
}

fn transition_status(status: &mut RolloutStatus, next: RolloutState) {
    if let Err(error) = status.transition_to(next) {
        warn!("{error}");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StageOutcomeEvent {
    Completed {
        stage_idx: u64,
        completed_count: u32,
        failed_count: u32,
    },
    Failed {
        stage_idx: u64,
        completed_count: u32,
        failed_count: u32,
        reason: Option<String>,
    },
}

fn stage_outcome_event(status: &RolloutStatus, stage_idx: usize) -> Option<StageOutcomeEvent> {
    let stage = status.stages.get(stage_idx)?;
    let completed_at = stage.completed_at?;
    let _ = completed_at;
    let target_count = stage.target_agents.len() as u32;
    let allowed_failures =
        allowed_failures_for_target_count(target_count, status.config.max_failure_rate_percent);

    if stage.failed_count > allowed_failures {
        Some(StageOutcomeEvent::Failed {
            stage_idx: stage_idx as u64,
            completed_count: stage.completed_count,
            failed_count: stage.failed_count,
            reason: status.error_message.clone(),
        })
    } else {
        Some(StageOutcomeEvent::Completed {
            stage_idx: stage_idx as u64,
            completed_count: stage.completed_count,
            failed_count: stage.failed_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oasis_core::core_types::AgentId;
    use oasis_core::file_types::FileApplyExecution;
    use oasis_core::task_types::TaskExecution;

    const OP_ID_1: &str = "123e4567-e89b-12d3-a456-426614174000";
    const OP_ID_ROLLBACK: &str = "123e4567-e89b-12d3-a456-426614174099";

    #[test]
    fn test_apply_file_stage_result_uses_actual_failures() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "file rollout".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![2] },
            task_type: RolloutTaskType::FileDeployment {
                config: oasis_core::file_types::FileConfig {
                    source_path: "/tmp/app.conf".to_string(),
                    destination_path: "/etc/app.conf".to_string(),
                    revision: 7,
                    owner: None,
                    mode: None,
                    target: Some(oasis_core::core_types::SelectorExpression::from(
                        "all".to_string(),
                    )),
                    operation_id: Some(OP_ID_1.to_string()),
                },
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let agent_a = AgentId::new("agent-a");
        let agent_b = AgentId::new("agent-b");
        let mut status = RolloutStatus::new(config, vec![agent_a.clone(), agent_b.clone()]);
        status.state = RolloutState::Running;
        status.current_action = "部署文件: app.conf".to_string();

        apply_file_stage_result(
            &mut status,
            &[
                FileApplyExecution::success(
                    agent_a,
                    OP_ID_1.to_string(),
                    "/tmp/app.conf".to_string(),
                    "/etc/app.conf".to_string(),
                    7,
                    "ok".to_string(),
                ),
                FileApplyExecution::failure(
                    agent_b.clone(),
                    OP_ID_1.to_string(),
                    "/tmp/app.conf".to_string(),
                    "/etc/app.conf".to_string(),
                    7,
                    "permission denied".to_string(),
                ),
            ],
        );

        let stage = status.current_stage_status().expect("stage");
        assert_eq!(stage.completed_count, 1);
        assert_eq!(stage.failed_count, 1);
        assert_eq!(stage.failed_executions.len(), 1);
        assert_eq!(stage.failed_executions[0].agent_id, agent_b);
        assert_eq!(status.state, RolloutState::Failed);
    }

    #[test]
    fn test_command_stage_success_advances_without_completing_rollout() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "command rollout".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1, 1] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: vec!["ok".to_string()],
                timeout_seconds: 30,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(
            config,
            vec![AgentId::new("agent-a"), AgentId::new("agent-b")],
        );
        status.state = RolloutState::Running;

        apply_command_stage_result(&mut status, false, 1, 0, Vec::<TaskExecution>::new());

        assert_eq!(status.current_stage_idx, 1);
        assert_eq!(status.state, RolloutState::Running);
        assert!(status.can_advance());
    }

    #[test]
    fn test_file_rollback_result_marks_rollout_as_rolled_back() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "file rollback".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1] },
            task_type: RolloutTaskType::FileDeployment {
                config: oasis_core::file_types::FileConfig {
                    source_path: "/tmp/app.conf".to_string(),
                    destination_path: "/etc/app.conf".to_string(),
                    revision: 9,
                    owner: None,
                    mode: None,
                    target: Some(oasis_core::core_types::SelectorExpression::from(
                        "all".to_string(),
                    )),
                    operation_id: Some(OP_ID_1.to_string()),
                },
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let agent = AgentId::new("agent-a");
        let mut status = RolloutStatus::new(config, vec![agent.clone()]);
        status.current_stage_idx = 1;
        status.state = RolloutState::RollingBack;
        status.current_action = "部署文件回滚: app.conf".to_string();

        apply_file_stage_result(
            &mut status,
            &[FileApplyExecution::success(
                agent,
                OP_ID_ROLLBACK.to_string(),
                "/tmp/app.conf".to_string(),
                "/etc/app.conf".to_string(),
                8,
                "rollback ok".to_string(),
            )],
        );

        assert_eq!(status.state, RolloutState::RolledBack);
    }

    #[test]
    fn test_file_rollback_result_marks_rollout_as_rollback_failed() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "file rollback".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1] },
            task_type: RolloutTaskType::FileDeployment {
                config: oasis_core::file_types::FileConfig {
                    source_path: "/tmp/app.conf".to_string(),
                    destination_path: "/etc/app.conf".to_string(),
                    revision: 9,
                    owner: None,
                    mode: None,
                    target: Some(oasis_core::core_types::SelectorExpression::from(
                        "all".to_string(),
                    )),
                    operation_id: Some(OP_ID_1.to_string()),
                },
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let agent = AgentId::new("agent-a");
        let mut status = RolloutStatus::new(config, vec![agent.clone()]);
        status.current_stage_idx = 1;
        status.state = RolloutState::RollingBack;
        status.current_action = "部署文件回滚: app.conf".to_string();

        apply_file_stage_result(
            &mut status,
            &[FileApplyExecution::failure(
                agent,
                OP_ID_ROLLBACK.to_string(),
                "/tmp/app.conf".to_string(),
                "/etc/app.conf".to_string(),
                8,
                "rollback denied".to_string(),
            )],
        );

        assert_eq!(status.state, RolloutState::RollbackFailed);
    }

    #[test]
    fn test_build_file_version_snapshot_uses_previous_revision() {
        let file_config = oasis_core::file_types::FileConfig {
            source_path: "/tmp/app.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 20,
            owner: None,
            mode: None,
            target: Some(oasis_core::core_types::SelectorExpression::from(
                "all".to_string(),
            )),
            operation_id: Some(OP_ID_1.to_string()),
        };

        let snapshot = build_file_version_snapshot(file_config, Some(12));
        let SnapshotData::FileSnapshot {
            previous_revision, ..
        } = snapshot.snapshot_data
        else {
            panic!("expected file snapshot");
        };
        assert_eq!(previous_revision, Some(12));
    }

    #[test]
    fn test_resolve_file_rollback_revision_uses_rollout_baseline_revision() {
        let file_config = oasis_core::file_types::FileConfig {
            source_path: "/tmp/app.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 20,
            owner: None,
            mode: None,
            target: Some(oasis_core::core_types::SelectorExpression::from(
                "all".to_string(),
            )),
            operation_id: Some(OP_ID_1.to_string()),
        };

        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "file rollback".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1, 1] },
            task_type: RolloutTaskType::FileDeployment {
                config: file_config.clone(),
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(
            config,
            vec![AgentId::new("agent-a"), AgentId::new("agent-b")],
        );
        status.stages[0].version_snapshot =
            Some(build_file_version_snapshot(file_config.clone(), Some(10)));
        status.stages[1].version_snapshot =
            Some(build_file_version_snapshot(file_config, Some(20)));

        assert_eq!(resolve_file_rollback_revision(&status), Some(10));
    }

    #[test]
    fn test_choose_file_rollback_revision_falls_back_to_previous_history_version() {
        let history = oasis_core::file_types::FileHistory {
            name: "app.conf".to_string(),
            current_version: 20,
            versions: vec![
                oasis_core::file_types::FileVersion {
                    name: "app.conf".to_string(),
                    revision: 20,
                    size: 10,
                    checksum: "new".to_string(),
                    created_at: 20,
                    is_current: true,
                },
                oasis_core::file_types::FileVersion {
                    name: "app.conf".to_string(),
                    revision: 10,
                    size: 10,
                    checksum: "old".to_string(),
                    created_at: 10,
                    is_current: false,
                },
            ],
        };

        let chosen = choose_file_rollback_revision(20, 20, Some(&history));
        assert_eq!(chosen, 10);
    }

    #[test]
    fn test_rollback_stage_index_uses_current_stage_when_rollout_failed() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "rollback".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1, 1] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: vec![],
                timeout_seconds: 30,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };
        let mut status = RolloutStatus::new(
            config,
            vec![AgentId::new("agent-a"), AgentId::new("agent-b")],
        );
        status.current_stage_idx = 0;
        status.state = RolloutState::Failed;

        assert_eq!(rollback_stage_index(&status), Some(0));
    }

    #[test]
    fn test_completed_rollout_can_rollback_previous_stage() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "completed rollback".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1] },
            task_type: RolloutTaskType::FileDeployment {
                config: oasis_core::file_types::FileConfig {
                    source_path: "/tmp/app.conf".to_string(),
                    destination_path: "/etc/app.conf".to_string(),
                    revision: 9,
                    owner: None,
                    mode: None,
                    target: Some(oasis_core::core_types::SelectorExpression::from(
                        "all".to_string(),
                    )),
                    operation_id: Some(OP_ID_1.to_string()),
                },
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };
        let mut status = RolloutStatus::new(config, vec![AgentId::new("agent-a")]);
        status.current_stage_idx = 1;
        status.state = RolloutState::Completed;

        assert!(status.can_rollback());
        assert_eq!(rollback_stage_index(&status), Some(0));
    }

    #[test]
    fn test_two_agent_rollout_keeps_explicit_two_stage_strategy() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "two-agent canary".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1, 1] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: vec!["ok".to_string()],
                timeout_seconds: 30,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let status = RolloutService::create_rollout_status_with_smart_stages(
            config,
            vec![AgentId::new("agent-a"), AgentId::new("agent-b")],
        );

        assert_eq!(status.stages.len(), 2);
        assert_eq!(status.stages[0].target_agents.len(), 1);
        assert_eq!(status.stages[1].target_agents.len(), 1);
    }

    #[test]
    fn test_build_stage_selector_matches_existing_style() {
        let selector = RolloutService::build_stage_selector(&[
            AgentId::new("agent-a"),
            AgentId::new("agent-b"),
        ]);
        assert_eq!(selector, "agent_id in [agent-a,agent-b]");
    }

    #[test]
    fn test_build_file_stage_config_uses_stage_selector() {
        let config = oasis_core::file_types::FileConfig {
            source_path: "/tmp/app.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 9,
            owner: None,
            mode: None,
            target: None,
            operation_id: None,
        };

        let file_config = RolloutService::build_file_stage_config(
            &config,
            &[AgentId::new("agent-a")],
            OP_ID_1.to_string(),
            9,
        );

        assert_eq!(
            file_config.target.as_ref().map(|v| v.expression.as_str()),
            Some("agent_id in [agent-a]")
        );
        assert_eq!(file_config.operation_id, OP_ID_1);
    }

    #[test]
    fn test_normalize_target_agents_sorts_and_deduplicates() {
        let normalized = RolloutService::normalize_target_agents(vec![
            AgentId::new("agent-c"),
            AgentId::new("agent-a"),
            AgentId::new("agent-b"),
            AgentId::new("agent-a"),
        ]);

        assert_eq!(
            normalized,
            vec![
                AgentId::new("agent-a"),
                AgentId::new("agent-b"),
                AgentId::new("agent-c"),
            ]
        );
    }

    #[test]
    fn test_resume_rollout_state_restores_created_before_first_stage() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "resume rollout".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: vec!["ok".to_string()],
                timeout_seconds: 30,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(config, vec![AgentId::new("agent-a")]);
        status.state = RolloutState::Paused;

        resume_rollout_state(&mut status);
        assert_eq!(status.state, RolloutState::Created);
    }

    #[test]
    fn test_resume_rollout_state_restores_running_after_stage_started() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "resume rollout".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: vec!["ok".to_string()],
                timeout_seconds: 30,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(config, vec![AgentId::new("agent-a")]);
        status.state = RolloutState::Paused;
        status.stages[0].started_at = Some(chrono::Utc::now().timestamp());

        resume_rollout_state(&mut status);
        assert_eq!(status.state, RolloutState::Running);
    }

    #[test]
    fn test_apply_command_stage_result_allows_failures_within_threshold() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "threshold rollout".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![2] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: vec!["ok".to_string()],
                timeout_seconds: 30,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 50,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(
            config,
            vec![AgentId::new("agent-a"), AgentId::new("agent-b")],
        );
        status.state = RolloutState::Running;

        apply_command_stage_result(&mut status, false, 1, 1, Vec::<TaskExecution>::new());

        assert_eq!(status.state, RolloutState::Completed);
    }

    #[test]
    fn test_enforce_stage_timeout_marks_running_rollout_failed() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "timeout rollout".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: vec!["ok".to_string()],
                timeout_seconds: 30,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 1,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(config, vec![AgentId::new("agent-a")]);
        status.state = RolloutState::Running;
        status.stages[0].started_at = Some(chrono::Utc::now().timestamp() - 5);

        enforce_stage_timeout(&mut status);

        assert_eq!(status.state, RolloutState::Failed);
        assert!(
            status
                .error_message
                .as_deref()
                .unwrap_or_default()
                .contains("超时")
        );
    }

    #[test]
    fn test_mark_stage_operation_error_sets_file_rollout_failed() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "file rollout".to_string(),
            target: oasis_core::core_types::SelectorExpression::from("all".to_string()),
            strategy: RolloutStrategy::Count { stages: vec![1] },
            task_type: RolloutTaskType::FileDeployment {
                config: oasis_core::file_types::FileConfig {
                    source_path: "/tmp/app.conf".to_string(),
                    destination_path: "/etc/app.conf".to_string(),
                    revision: 7,
                    owner: None,
                    mode: None,
                    target: Some(oasis_core::core_types::SelectorExpression::from(
                        "all".to_string(),
                    )),
                    operation_id: Some(OP_ID_1.to_string()),
                },
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(config, vec![AgentId::new("agent-a")]);
        status.state = RolloutState::Running;
        status.current_action = "部署文件: app.conf".to_string();

        mark_stage_operation_error(&mut status, false, "dispatch failed");

        let stage = status.stages.first().expect("stage");
        assert_eq!(status.state, RolloutState::Failed);
        assert_eq!(stage.failed_count, 1);
        assert_eq!(status.error_message.as_deref(), Some("dispatch failed"));
    }

    #[test]
    fn test_stage_outcome_event_marks_completed_stage() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "command rollout".to_string(),
            target: SelectorExpression::new("all"),
            strategy: RolloutStrategy::Count { stages: vec![2] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: Vec::new(),
                timeout_seconds: 10,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 50,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(
            config,
            vec![AgentId::new("agent-a"), AgentId::new("agent-b")],
        );
        status.stages[0].completed_at = Some(chrono::Utc::now().timestamp());
        status.stages[0].completed_count = 2;
        status.stages[0].failed_count = 0;

        assert_eq!(
            stage_outcome_event(&status, 0),
            Some(StageOutcomeEvent::Completed {
                stage_idx: 0,
                completed_count: 2,
                failed_count: 0,
            })
        );
    }

    #[test]
    fn test_stage_outcome_event_marks_failed_stage_when_threshold_exceeded() {
        let config = RolloutConfig {
            rollout_id: RolloutId::generate(),
            name: "command rollout".to_string(),
            target: SelectorExpression::new("all"),
            strategy: RolloutStrategy::Count { stages: vec![2] },
            task_type: RolloutTaskType::Command {
                command: "echo".to_string(),
                args: Vec::new(),
                timeout_seconds: 10,
            },
            auto_advance: false,
            advance_interval_seconds: 60,
            stage_timeout_seconds: 600,
            max_failure_rate_percent: 0,
            auto_rollback: false,
            rollback_command: None,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut status = RolloutStatus::new(
            config,
            vec![AgentId::new("agent-a"), AgentId::new("agent-b")],
        );
        status.error_message = Some("threshold exceeded".to_string());
        status.stages[0].completed_at = Some(chrono::Utc::now().timestamp());
        status.stages[0].completed_count = 1;
        status.stages[0].failed_count = 1;

        assert_eq!(
            stage_outcome_event(&status, 0),
            Some(StageOutcomeEvent::Failed {
                stage_idx: 0,
                completed_count: 1,
                failed_count: 1,
                reason: Some("threshold exceeded".to_string()),
            })
        );
    }
}

impl RolloutService {
    fn normalize_target_agents(mut all_target_agents: Vec<AgentId>) -> Vec<AgentId> {
        all_target_agents.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        all_target_agents.dedup_by(|a, b| a.as_str() == b.as_str());
        all_target_agents
    }

    fn build_stage_selector(target_agents: &[AgentId]) -> String {
        let agent_ids: Vec<String> = target_agents.iter().map(|id| id.to_string()).collect();
        format!("agent_id in [{}]", agent_ids.join(","))
    }

    fn build_rollback_command_selector(target_agents: &[AgentId]) -> String {
        format!(
            "agent_id in [{}]",
            target_agents
                .iter()
                .map(|id| format!("\"{}\"", id))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    fn build_file_stage_config(
        config: &oasis_core::file_types::FileConfig,
        target_agents: &[AgentId],
        operation_id: String,
        revision: u64,
    ) -> proto::FileConfigMsg {
        proto::FileConfigMsg {
            source_path: config.source_path.clone(),
            destination_path: config.destination_path.clone(),
            revision,
            owner: config.owner.clone().unwrap_or_default(),
            mode: config.mode.clone().unwrap_or_default(),
            target: Some(proto::SelectorExpression {
                expression: Self::build_stage_selector(target_agents),
            }),
            operation_id,
        }
    }

    pub async fn new(jetstream: Arc<Context>, task_monitor: Arc<TaskMonitor>) -> Result<Self> {
        info!("Initializing RolloutService");

        let service = Self {
            jetstream,
            task_monitor,
            event_bus: None,
            rollout_cache: Arc::new(DashMap::new()),
        };

        // 启动时从 JetStream 恢复状态
        if let Err(e) = service.load_rollouts_from_jetstream().await {
            warn!("Failed to load rollouts from JetStream: {}", e);
        }

        Ok(service)
    }

    pub fn with_event_bus(mut self, event_bus: Arc<EventBus>) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    async fn publish_stage_outcome_event(
        &self,
        rollout_id: &RolloutId,
        outcome: StageOutcomeEvent,
    ) {
        let Some(event_bus) = &self.event_bus else {
            return;
        };

        let event = match outcome {
            StageOutcomeEvent::Completed {
                stage_idx,
                completed_count,
                failed_count,
            } => OasisEvent::new(OasisEventKind::RolloutStageCompleted {
                rollout_id: rollout_id.clone(),
                stage_idx,
                completed_count,
                failed_count,
            }),
            StageOutcomeEvent::Failed {
                stage_idx,
                completed_count,
                failed_count,
                reason,
            } => OasisEvent::new(OasisEventKind::RolloutStageFailed {
                rollout_id: rollout_id.clone(),
                stage_idx,
                completed_count,
                failed_count,
                reason,
            }),
        };

        if let Err(e) = event_bus.publish(&event).await {
            warn!("Failed to publish rollout stage event: {}", e);
        }
    }

    pub async fn advance_once(
        &self,
        rollout_id: &RolloutId,
        task_service: &TaskService,
        file_service: &FileService,
    ) -> Result<()> {
        let stage_info =
            self.get_next_stage_info(rollout_id)
                .await?
                .ok_or_else(|| CoreError::InvalidTask {
                    reason: "Rollout 已完成所有阶段".to_string(),
                    severity: ErrorSeverity::Error,
                })?;

        let (target_agents, task_type) = stage_info;

        match &task_type {
            RolloutTaskType::Command {
                command,
                args,
                timeout_seconds,
            } => {
                let batch_request = BatchRequest {
                    command: command.clone(),
                    args: args.clone(),
                    selector: SelectorExpression::from(Self::build_stage_selector(&target_agents)),
                    timeout_seconds: *timeout_seconds,
                };

                let batch_id = task_service
                    .submit_batch(batch_request, target_agents)
                    .await?;

                self.mark_advance_next_stage(rollout_id, task_type, Some(batch_id), None)
                    .await
            }
            RolloutTaskType::FileDeployment { config } => {
                let operation_id = OperationId::generate().to_string();
                let previous_revision = file_service
                    .current_active_revision(&config.source_path)
                    .await?;
                let version_snapshot = Some(build_file_version_snapshot(
                    config.clone(),
                    previous_revision,
                ));

                self.mark_advance_next_stage(rollout_id, task_type.clone(), None, version_snapshot)
                    .await?;

                let file_config = Self::build_file_stage_config(
                    config,
                    &target_agents,
                    operation_id,
                    config.revision,
                );

                let summary = match file_service
                    .apply_with_details(&file_config, target_agents)
                    .await
                {
                    Ok(summary) => summary,
                    Err(error) => {
                        let _ = self
                            .mark_stage_operation_error(rollout_id, false, error.to_string())
                            .await;
                        return Err(error);
                    }
                };

                self.mark_file_stage_result(rollout_id, &summary.all_results)
                    .await
            }
        }
    }

    pub async fn rollback_once(
        &self,
        rollout_id: &RolloutId,
        rollback_command: Option<String>,
        task_service: &TaskService,
        file_service: &FileService,
    ) -> Result<(bool, String)> {
        let configured_rollback_command = self
            .rollout_cache
            .get(rollout_id)
            .and_then(|status| status.config.rollback_command.clone());

        let (target_agents, task_type, version_snapshot) = self
            .get_rollback_stage_info(rollout_id)
            .await?
            .ok_or_else(|| CoreError::InvalidTask {
                reason: "没有找到可回滚的阶段".to_string(),
                severity: ErrorSeverity::Error,
            })?;

        match &task_type {
            RolloutTaskType::Command { .. } => {
                let command = rollback_command
                    .or(configured_rollback_command)
                    .ok_or_else(|| CoreError::InvalidTask {
                        reason: "命令回滚需要提供 rollback_command".to_string(),
                        severity: ErrorSeverity::Error,
                    })?;

                let batch_request = BatchRequest {
                    command: command.clone(),
                    args: vec![],
                    selector: SelectorExpression::from(Self::build_rollback_command_selector(
                        &target_agents,
                    )),
                    timeout_seconds: 300,
                };

                let batch_id = task_service
                    .submit_batch(batch_request, target_agents.clone())
                    .await?;

                self.mark_rollback_stage(
                    rollout_id,
                    task_type,
                    Some(command.clone()),
                    Some(batch_id),
                )
                .await?;

                Ok((
                    true,
                    format!(
                        "回滚命令已提交，影响 {} 个Agent，命令: {}",
                        target_agents.len(),
                        command
                    ),
                ))
            }
            RolloutTaskType::FileDeployment { .. } => {
                let snapshot = version_snapshot.ok_or_else(|| CoreError::InvalidTask {
                    reason: "缺少版本快照信息，无法文件回滚".to_string(),
                    severity: ErrorSeverity::Error,
                })?;

                let (file_config, previous_revision) = match snapshot.snapshot_data {
                    SnapshotData::FileSnapshot {
                        file_config,
                        previous_revision,
                    } => (file_config, previous_revision),
                    _ => {
                        return Err(CoreError::InvalidTask {
                            reason: "版本快照类型不匹配，无法文件回滚".to_string(),
                            severity: ErrorSeverity::Error,
                        });
                    }
                };

                let previous_revision =
                    previous_revision.ok_or_else(|| CoreError::InvalidTask {
                        reason: "缺少 previous_revision，无法回滚".to_string(),
                        severity: ErrorSeverity::Error,
                    })?;
                let snapshot_previous_revision = self
                    .rollout_cache
                    .get(rollout_id)
                    .and_then(|status| resolve_file_rollback_revision(status.value()))
                    .unwrap_or(previous_revision);
                let file_history = file_service
                    .get_file_history(&file_config.source_path)
                    .await?;
                let previous_revision = choose_file_rollback_revision(
                    snapshot_previous_revision,
                    file_config.revision,
                    file_history.as_ref(),
                );

                self.mark_rollback_stage(rollout_id, task_type, rollback_command, None)
                    .await?;

                let cfg = Self::build_file_stage_config(
                    &file_config,
                    &target_agents,
                    OperationId::generate().to_string(),
                    previous_revision,
                );

                let summary = match file_service
                    .rollback_file_with_details(&cfg, target_agents.clone())
                    .await
                {
                    Ok(summary) => summary,
                    Err(error) => {
                        let _ = self
                            .mark_stage_operation_error(rollout_id, true, error.to_string())
                            .await;
                        return Err(error);
                    }
                };

                self.mark_file_stage_result(rollout_id, &summary.all_results)
                    .await?;

                if summary.success {
                    Ok((
                        true,
                        format!(
                            "文件回滚成功，影响 {} 个Agent，版本 {}",
                            target_agents.len(),
                            previous_revision
                        ),
                    ))
                } else {
                    Ok((
                        false,
                        format!(
                            "文件回滚失败，影响 {} 个Agent，版本 {}，{}",
                            target_agents.len(),
                            previous_revision,
                            summary.message
                        ),
                    ))
                }
            }
        }
    }

    pub async fn pause_rollout(&self, rollout_id: &RolloutId) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status = status.value_mut();
            if !status.state.can_pause() {
                return Err(CoreError::InvalidTask {
                    reason: format!("Rollout {} 当前状态不允许暂停", rollout_id),
                    severity: ErrorSeverity::Error,
                });
            }

            transition_status(status, RolloutState::Paused);
            status.current_action = "发布已暂停".to_string();
            status.updated_at = chrono::Utc::now().timestamp();
            self.persist_rollout_to_jetstream(status).await?;
            Ok(())
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
    }

    pub async fn resume_rollout(&self, rollout_id: &RolloutId) -> Result<RolloutStatus> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status = status.value_mut();
            if !status.state.can_resume() {
                return Err(CoreError::InvalidTask {
                    reason: format!("Rollout {} 当前状态不允许恢复", rollout_id),
                    severity: ErrorSeverity::Error,
                });
            }

            resume_rollout_state(status);
            status.updated_at = chrono::Utc::now().timestamp();
            self.persist_rollout_to_jetstream(status).await?;
            Ok(status.clone())
        } else {
            Err(CoreError::NotFound {
                entity_type: "Rollout".to_string(),
                entity_id: rollout_id.to_string(),
                severity: ErrorSeverity::Error,
            })
        }
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
        let all_target_agents = Self::normalize_target_agents(all_target_agents);

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
            stage_timeout_seconds: request.stage_timeout_seconds,
            max_failure_rate_percent: request.max_failure_rate_percent,
            auto_rollback: request.auto_rollback,
            rollback_command: request.rollback_command,
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
        RolloutStatus::new(config, all_target_agents)
    }

    /// 获取灰度发布状态 - 优先内存缓存，后JetStream
    pub async fn get_rollout_status(&self, rollout_id: &RolloutId) -> Result<RolloutStatus> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            // 更新阶段状态
            self.update_stage_status_from_task(status.value_mut()).await;
            self.update_stage_status_from_file(status.value_mut()).await;
            if enforce_stage_timeout(status.value_mut()) {
                let _ = self.persist_rollout_to_jetstream(status.value()).await;
            }
            Ok(status.value().clone())
        } else {
            // 从 JetStream 加载
            match self.load_rollout_from_jetstream(rollout_id).await {
                Ok(mut status) => {
                    // 更新状态并缓存
                    self.update_stage_status_from_task(&mut status).await;
                    self.update_stage_status_from_file(&mut status).await;
                    let _ = enforce_stage_timeout(&mut status);
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
            if enforce_stage_timeout(status) {
                let _ = self.persist_rollout_to_jetstream(status).await;
            }
            let updated_status = status.clone();

            // 应用状态过滤
            if states
                .as_ref()
                .is_none_or(|s| s.contains(&updated_status.state))
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
            transition_status(status_val, RolloutState::Running);
            let current_stage = match status_val.current_stage_status_mut() {
                Some(stage) => stage,
                None => {
                    warn!(
                        "Missing current stage while advancing rollout {}",
                        rollout_id
                    );
                    return Err(CoreError::Internal {
                        message: format!("Missing current stage for rollout {}", rollout_id),
                        severity: ErrorSeverity::Error,
                    });
                }
            };
            current_stage.batch_id = batch_id;
            current_stage.version_snapshot = version_snapshot;
            current_stage.started_at = Some(chrono::Utc::now().timestamp());
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

            transition_status(status_val, RolloutState::RollingBack);
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

            if let Some(stage_idx) = rollback_stage_index(status_val) {
                status_val.current_stage_idx = stage_idx as u64;
                if let Some(stage) = status_val.current_stage_status_mut() {
                    stage.batch_id = batch_id;
                    stage.started_at = Some(chrono::Utc::now().timestamp());
                }
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

    pub async fn mark_file_stage_result(
        &self,
        rollout_id: &RolloutId,
        results: &[FileApplyExecution],
    ) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();
            let stage_idx =
                result_stage_index(status_val, status_val.state == RolloutState::RollingBack);
            let previous_completed_at = stage_idx.and_then(|idx| {
                status_val
                    .stages
                    .get(idx)
                    .and_then(|stage| stage.completed_at)
            });
            apply_file_stage_result(status_val, results);
            status_val.updated_at = chrono::Utc::now().timestamp();
            if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                warn!("Failed to persist file stage result: {}", e);
            }
            if let Some(stage_idx) = stage_idx {
                if previous_completed_at.is_none() {
                    if let Some(outcome) = stage_outcome_event(status_val, stage_idx) {
                        self.publish_stage_outcome_event(rollout_id, outcome).await;
                    }
                }
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

    pub async fn mark_stage_operation_error(
        &self,
        rollout_id: &RolloutId,
        is_rolling_back: bool,
        error_message: impl Into<String>,
    ) -> Result<()> {
        if let Some(mut status) = self.rollout_cache.get_mut(rollout_id) {
            let status_val = status.value_mut();
            let stage_idx = result_stage_index(status_val, is_rolling_back);
            let previous_completed_at = stage_idx.and_then(|idx| {
                status_val
                    .stages
                    .get(idx)
                    .and_then(|stage| stage.completed_at)
            });
            mark_stage_operation_error(status_val, is_rolling_back, error_message);
            status_val.updated_at = chrono::Utc::now().timestamp();
            if let Err(e) = self.persist_rollout_to_jetstream(status_val).await {
                warn!("Failed to persist rollout operation error: {}", e);
            }
            if !is_rolling_back {
                if let Some(stage_idx) = stage_idx {
                    if previous_completed_at.is_none() {
                        if let Some(outcome) = stage_outcome_event(status_val, stage_idx) {
                            self.publish_stage_outcome_event(rollout_id, outcome).await;
                        }
                    }
                }
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
            if let Some(stage_idx) = rollback_stage_index(&status) {
                let stage = status
                    .stages
                    .get(stage_idx)
                    .ok_or_else(|| CoreError::Internal {
                        message: format!("Missing rollback stage for rollout {}", rollout_id),
                        severity: ErrorSeverity::Error,
                    })?;
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
        if enforce_stage_timeout(status) {
            return;
        }
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
            let rollout_id = status.config.rollout_id.clone();

            // 获取阶段并更新统计信息
            let stage_idx = match result_stage_index(status, is_rolling_back) {
                Some(idx) => idx,
                None => return,
            };
            let previous_completed_at = status
                .stages
                .get(stage_idx)
                .and_then(|stage| stage.completed_at);

            if let Some(stage) = status.stages.get_mut(stage_idx) {
                if let Some(batch_id) = &stage.batch_id {
                    if let Some(task_ids) = self.task_monitor.get_batch_task_ids(batch_id) {
                        // 统计各种状态的任务数量并收集失败详情
                        for task_id in task_ids {
                            if let Some(execution) =
                                self.task_monitor.latest_execution_from_cache(&task_id)
                            {
                                match execution.state {
                                    TaskState::Running | TaskState::Cancelling => {
                                        started_count += 1
                                    }
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
                    }
                } else {
                    return;
                }
            } else {
                return;
            }

            // 检查是否所有任务都已完成
            let total_targets = if let Some(stage) = status.stages.get(stage_idx) {
                stage.target_agents.len() as u32
            } else {
                return;
            };

            if completed_count + failed_count >= total_targets {
                apply_command_stage_result(
                    status,
                    is_rolling_back,
                    completed_count,
                    failed_count,
                    failed_executions,
                );
            }

            status.updated_at = chrono::Utc::now().timestamp();

            // 持久化更新的状态
            if let Err(e) = self.persist_rollout_to_jetstream(status).await {
                error!("Failed to persist status update: {}", e);
            }

            if !is_rolling_back && previous_completed_at.is_none() {
                if let Some(outcome) = stage_outcome_event(status, stage_idx) {
                    self.publish_stage_outcome_event(&rollout_id, outcome).await;
                }
            }
        }
    }

    pub async fn update_stage_status_from_file(&self, status: &mut RolloutStatus) {
        if enforce_stage_timeout(status) {
            return;
        }
        if (status.state == RolloutState::Running || status.state == RolloutState::RollingBack)
            && status.current_action.starts_with("部署文件")
        {
            // 文件阶段现在由 agent 回执驱动；这里只保留查询入口，不再默认成功。
        }
    }
}

pub(crate) fn mark_stage_operation_error(
    status: &mut RolloutStatus,
    is_rolling_back: bool,
    error_message: impl Into<String>,
) {
    let error_message = error_message.into();
    let stage_idx = match result_stage_index(status, is_rolling_back) {
        Some(idx) => idx,
        None => return,
    };

    let Some(stage) = status.stages.get_mut(stage_idx) else {
        return;
    };

    stage.started_count = stage.target_agents.len() as u32;
    stage.failed_count = stage.target_agents.len() as u32;
    stage.completed_at = Some(chrono::Utc::now().timestamp());
    status.error_message = Some(error_message);
    transition_status(
        status,
        if is_rolling_back {
            RolloutState::RollbackFailed
        } else {
            RolloutState::Failed
        },
    );
}

fn apply_command_stage_result(
    status: &mut RolloutStatus,
    is_rolling_back: bool,
    completed_count: u32,
    failed_count: u32,
    failed_executions: Vec<oasis_core::task_types::TaskExecution>,
) {
    let stage_idx = result_stage_index(status, is_rolling_back);
    let max_failure_rate_percent = status.config.max_failure_rate_percent;

    let Some(stage_idx) = stage_idx else {
        return;
    };
    let Some(stage) = status.stages.get_mut(stage_idx) else {
        return;
    };
    stage.completed_count = completed_count;
    stage.failed_count = failed_count;
    stage.failed_executions = failed_executions;
    stage.completed_at = Some(chrono::Utc::now().timestamp());

    let target_count = stage.target_agents.len() as u32;
    let allowed_failures =
        allowed_failures_for_target_count(target_count, max_failure_rate_percent);
    if failed_count > allowed_failures {
        if is_rolling_back {
            transition_status(status, RolloutState::RollbackFailed);
        } else {
            transition_status(status, RolloutState::Failed);
            status.error_message = Some(format!(
                "阶段失败率超过阈值: {}/{} (允许失败数: {})",
                failed_count, target_count, allowed_failures
            ));
        }
        return;
    }

    if is_rolling_back {
        transition_status(status, RolloutState::RolledBack);
        return;
    }

    status.current_stage_idx += 1;
    if status.current_stage_idx >= status.stages.len() as u64 {
        transition_status(status, RolloutState::Completed);
    } else {
        transition_status(status, RolloutState::Running);
    }
}

pub(crate) fn build_file_version_snapshot(
    file_config: oasis_core::file_types::FileConfig,
    previous_revision: Option<u64>,
) -> VersionSnapshot {
    VersionSnapshot::new_file_snapshot(file_config, previous_revision)
}

fn apply_file_stage_result(status: &mut RolloutStatus, results: &[FileApplyExecution]) {
    let is_rolling_back = status.state == RolloutState::RollingBack;
    let stage_idx = result_stage_index(status, is_rolling_back);
    let max_failure_rate_percent = status.config.max_failure_rate_percent;

    let Some(stage_idx) = stage_idx else {
        return;
    };
    let Some(stage) = status.stages.get_mut(stage_idx) else {
        return;
    };

    let mut completed_count = 0_u32;
    let mut failed_count = 0_u32;
    let mut failed_executions = Vec::new();

    for result in results {
        if result.success {
            completed_count += 1;
        } else {
            failed_count += 1;
            failed_executions.push(oasis_core::task_types::TaskExecution {
                task_id: oasis_core::core_types::TaskId::new(format!(
                    "file-{}-{}",
                    result.operation_id, result.agent_id
                )),
                agent_id: result.agent_id.clone(),
                state: TaskState::Failed,
                exit_code: Some(1),
                stdout: String::new(),
                stderr: result.message.clone(),
                started_at: result.finished_at,
                finished_at: Some(result.finished_at),
                duration_ms: None,
            });
        }
    }

    stage.started_count = stage.target_agents.len() as u32;
    stage.completed_count = completed_count;
    stage.failed_count = failed_count;
    stage.failed_executions = failed_executions;
    stage.completed_at = Some(chrono::Utc::now().timestamp());

    let target_count = stage.target_agents.len() as u32;
    let allowed_failures =
        allowed_failures_for_target_count(target_count, max_failure_rate_percent);
    if failed_count > allowed_failures {
        transition_status(
            status,
            if is_rolling_back {
                RolloutState::RollbackFailed
            } else {
                RolloutState::Failed
            },
        );
        if !is_rolling_back {
            status.error_message = Some(format!(
                "阶段失败率超过阈值: {}/{} (允许失败数: {})",
                failed_count, target_count, allowed_failures
            ));
        }
        return;
    }

    if is_rolling_back {
        transition_status(status, RolloutState::RolledBack);
        return;
    }

    status.current_stage_idx += 1;
    if status.current_stage_idx >= status.stages.len() as u64 {
        transition_status(status, RolloutState::Completed);
    } else {
        transition_status(status, RolloutState::Running);
    }
}

fn rollback_stage_index(status: &RolloutStatus) -> Option<usize> {
    match status.state {
        RolloutState::Failed | RolloutState::RollingBack | RolloutState::RollbackFailed => {
            status.stages.get(status.current_stage_idx as usize)?;
            Some(status.current_stage_idx as usize)
        }
        RolloutState::Running | RolloutState::Completed => status
            .current_stage_idx
            .checked_sub(1)
            .map(|idx| idx as usize),
        _ => None,
    }
}

fn resolve_file_rollback_revision(status: &RolloutStatus) -> Option<u64> {
    status
        .stages
        .iter()
        .find_map(|stage| match &stage.version_snapshot {
            Some(VersionSnapshot {
                snapshot_data:
                    SnapshotData::FileSnapshot {
                        previous_revision: Some(previous_revision),
                        ..
                    },
                ..
            }) => Some(*previous_revision),
            _ => None,
        })
}

fn choose_file_rollback_revision(
    snapshot_previous_revision: u64,
    current_revision: u64,
    file_history: Option<&oasis_core::file_types::FileHistory>,
) -> u64 {
    if snapshot_previous_revision != current_revision {
        return snapshot_previous_revision;
    }

    file_history
        .and_then(|history| {
            history
                .versions
                .iter()
                .find(|version| version.revision != current_revision)
                .map(|version| version.revision)
        })
        .unwrap_or(snapshot_previous_revision)
}

fn result_stage_index(status: &RolloutStatus, is_rolling_back: bool) -> Option<usize> {
    let current_idx = status.current_stage_idx as usize;
    if status.stages.get(current_idx).is_some() {
        return Some(current_idx);
    }

    if is_rolling_back {
        return status
            .current_stage_idx
            .checked_sub(1)
            .map(|idx| idx as usize)
            .filter(|idx| status.stages.get(*idx).is_some());
    }

    None
}

fn resume_rollout_state(status: &mut RolloutStatus) {
    let has_started_current_stage = status
        .current_stage_status()
        .and_then(|stage| stage.started_at)
        .is_some();

    transition_status(
        status,
        if has_started_current_stage {
            RolloutState::Running
        } else {
            RolloutState::Created
        },
    );
    status.current_action = "恢复发布".to_string();
}

fn enforce_stage_timeout(status: &mut RolloutStatus) -> bool {
    if status.state != RolloutState::Running {
        return false;
    }
    let stage_timeout_seconds = status.config.stage_timeout_seconds as i64;

    let Some(stage) = status.current_stage_status_mut() else {
        return false;
    };
    let Some(started_at) = stage.started_at else {
        return false;
    };
    if stage.completed_at.is_some() {
        return false;
    }

    let now = chrono::Utc::now().timestamp();
    if now - started_at < stage_timeout_seconds {
        return false;
    }

    stage.completed_at = Some(now);
    stage.failed_count = stage.target_agents.len() as u32;
    transition_status(status, RolloutState::Failed);
    status.error_message = Some(format!("阶段执行超时，超过 {} 秒", stage_timeout_seconds));
    true
}

fn allowed_failures_for_target_count(target_count: u32, max_failure_rate_percent: u32) -> u32 {
    target_count.saturating_mul(max_failure_rate_percent) / 100
}
