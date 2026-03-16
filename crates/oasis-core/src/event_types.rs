use crate::core_types::{AgentId, BatchId, OperationId, RolloutId, TaskId};
use crate::task_types::TaskState;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OasisEventKind {
    AgentOnline {
        agent_id: AgentId,
    },
    AgentOffline {
        agent_id: AgentId,
        reason: String,
    },
    TaskTerminal {
        task_id: TaskId,
        agent_id: AgentId,
        batch_id: Option<BatchId>,
        state: TaskState,
    },
    FileApplied {
        operation_id: OperationId,
        agent_id: AgentId,
        source_path: String,
        destination_path: String,
        revision: u64,
    },
    FileApplyFailed {
        operation_id: OperationId,
        agent_id: AgentId,
        source_path: String,
        destination_path: String,
        revision: u64,
        reason: String,
    },
    RolloutStageCompleted {
        rollout_id: RolloutId,
        stage_idx: u64,
        completed_count: u32,
        failed_count: u32,
    },
    RolloutStageFailed {
        rollout_id: RolloutId,
        stage_idx: u64,
        completed_count: u32,
        failed_count: u32,
        reason: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OasisEvent {
    pub event_id: String,
    pub occurred_at: i64,
    pub kind: OasisEventKind,
}

impl OasisEvent {
    pub fn new(kind: OasisEventKind) -> Self {
        Self {
            event_id: uuid::Uuid::now_v7().to_string(),
            occurred_at: chrono::Utc::now().timestamp(),
            kind,
        }
    }

    pub fn subject(&self) -> String {
        self.kind.subject()
    }
}

impl OasisEventKind {
    pub fn subject(&self) -> String {
        match self {
            Self::AgentOnline { agent_id } => format!("events.agent.online.{agent_id}"),
            Self::AgentOffline { agent_id, .. } => format!("events.agent.offline.{agent_id}"),
            Self::TaskTerminal {
                task_id,
                agent_id,
                state,
                ..
            } => format!(
                "events.task.terminal.{}.{}.{}",
                state_label(*state),
                task_id,
                agent_id
            ),
            Self::FileApplied { agent_id, .. } => {
                format!("events.file.applied.{agent_id}")
            }
            Self::FileApplyFailed { agent_id, .. } => {
                format!("events.file.apply_failed.{agent_id}")
            }
            Self::RolloutStageCompleted {
                rollout_id,
                stage_idx,
                ..
            } => {
                format!(
                    "events.rollout.stage.completed.{}.{}",
                    rollout_id, stage_idx
                )
            }
            Self::RolloutStageFailed {
                rollout_id,
                stage_idx,
                ..
            } => {
                format!("events.rollout.stage.failed.{}.{}", rollout_id, stage_idx)
            }
        }
    }
}

fn state_label(state: TaskState) -> &'static str {
    match state {
        TaskState::Created => "created",
        TaskState::Pending => "pending",
        TaskState::Running => "running",
        TaskState::Cancelling => "cancelling",
        TaskState::Success => "success",
        TaskState::Failed => "failed",
        TaskState::Timeout => "timeout",
        TaskState::Cancelled => "cancelled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_online_event_subject() {
        let event = OasisEvent::new(OasisEventKind::AgentOnline {
            agent_id: AgentId::new("agent-1"),
        });
        assert_eq!(event.subject(), "events.agent.online.agent-1");
    }

    #[test]
    fn test_task_terminal_event_subject_contains_terminal_state() {
        let event = OasisEvent::new(OasisEventKind::TaskTerminal {
            task_id: TaskId::new("task-1"),
            agent_id: AgentId::new("agent-1"),
            batch_id: Some(BatchId::new("batch-1")),
            state: TaskState::Failed,
        });
        assert_eq!(
            event.subject(),
            "events.task.terminal.failed.task-1.agent-1"
        );
    }

    #[test]
    fn test_rollout_stage_failed_event_subject() {
        let event = OasisEvent::new(OasisEventKind::RolloutStageFailed {
            rollout_id: RolloutId::new("rollout-1"),
            stage_idx: 2,
            completed_count: 3,
            failed_count: 1,
            reason: Some("threshold exceeded".to_string()),
        });
        assert_eq!(event.subject(), "events.rollout.stage.failed.rollout-1.2");
    }

    #[test]
    fn test_file_apply_failed_event_subject() {
        let event = OasisEvent::new(OasisEventKind::FileApplyFailed {
            operation_id: OperationId::new(uuid::Uuid::nil().to_string()),
            agent_id: AgentId::new("agent-1"),
            source_path: "/tmp/a.conf".to_string(),
            destination_path: "/etc/a.conf".to_string(),
            revision: 42,
            reason: "permission denied".to_string(),
        });
        assert_eq!(event.subject(), "events.file.apply_failed.agent-1");
    }
}
