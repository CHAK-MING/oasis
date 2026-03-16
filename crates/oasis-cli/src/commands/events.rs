use crate::time::format_local_ts;
use crate::ui::{print_header, print_info};
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use console::style;
use oasis_core::{
    config::OasisConfig,
    event_types::{OasisEvent, OasisEventKind},
    nats::NatsClientFactory,
};
use tokio_stream::StreamExt;

#[derive(Parser, Debug)]
#[command(
    name = "events",
    about = "查看 Oasis 事件流",
    after_help = r#"示例：
  oasis-cli events tail
  oasis-cli events tail --subject 'events.task.terminal.>'
  oasis-cli events tail --subject 'events.file.apply_failed.>'
  oasis-cli events tail --count 20
"#
)]
pub struct EventsArgs {
    #[command(subcommand)]
    pub cmd: EventsCmd,
}

#[derive(Subcommand, Debug)]
pub enum EventsCmd {
    /// 实时查看事件流
    Tail(EventsTailArgs),
}

#[derive(Parser, Debug)]
pub struct EventsTailArgs {
    /// 订阅的事件 subject
    #[arg(long, default_value = "events.>")]
    pub subject: String,

    /// 最多接收多少条事件后退出；不设置表示持续跟随
    #[arg(long)]
    pub count: Option<u32>,
}

pub async fn run_events(config: &OasisConfig, args: EventsArgs) -> Result<()> {
    match args.cmd {
        EventsCmd::Tail(args) => run_events_tail(config, args).await,
    }
}

async fn run_events_tail(config: &OasisConfig, args: EventsTailArgs) -> Result<()> {
    print_header("实时事件流");
    print_info(&format!("NATS: {}", style(&config.nats.url).cyan()));
    print_info(&format!("Subject: {}", style(&args.subject).bold()));
    print_info("按 Ctrl-C 停止");

    let client = NatsClientFactory::connect_with_config(&config.nats, &config.tls)
        .await
        .context("连接 NATS 失败")?;

    let mut subscriber = client
        .subscribe(args.subject.clone())
        .await
        .with_context(|| format!("订阅事件 subject 失败: {}", args.subject))?;

    let mut received = 0_u32;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                print_info("已停止事件订阅");
                break;
            }
            msg = subscriber.next() => {
                let Some(msg) = msg else {
                    return Err(anyhow!("事件订阅流已关闭"));
                };

                let event: OasisEvent = serde_json::from_slice(&msg.payload)
                    .context("解析事件失败")?;
                println!(
                    "{}  {}  {}",
                    style(format_local_ts(event.occurred_at)).dim(),
                    style(event.subject()).cyan(),
                    render_event_summary(&event.kind),
                );

                received += 1;
                if let Some(limit) = args.count {
                    if received >= limit {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn render_event_summary(kind: &OasisEventKind) -> String {
    match kind {
        OasisEventKind::AgentOnline { agent_id } => {
            format!("agent {} online", style(agent_id).green())
        }
        OasisEventKind::AgentOffline { agent_id, reason } => {
            format!("agent {} offline: {}", style(agent_id).yellow(), reason)
        }
        OasisEventKind::TaskTerminal {
            task_id,
            agent_id,
            batch_id,
            state,
        } => {
            let batch = batch_id
                .as_ref()
                .map(|id| format!(", batch {}", id))
                .unwrap_or_default();
            format!("task {} on {} -> {:?}{}", task_id, agent_id, state, batch)
        }
        OasisEventKind::FileApplied {
            agent_id,
            source_path,
            destination_path,
            revision,
            ..
        } => {
            format!(
                "file {} -> {} applied on {} (revision {})",
                source_path, destination_path, agent_id, revision
            )
        }
        OasisEventKind::FileApplyFailed {
            agent_id,
            source_path,
            destination_path,
            revision,
            reason,
            ..
        } => {
            format!(
                "file {} -> {} failed on {} (revision {}, reason={})",
                source_path, destination_path, agent_id, revision, reason
            )
        }
        OasisEventKind::RolloutStageCompleted {
            rollout_id,
            stage_idx,
            completed_count,
            failed_count,
        } => {
            format!(
                "rollout {} stage {} completed (ok={}, failed={})",
                rollout_id, stage_idx, completed_count, failed_count
            )
        }
        OasisEventKind::RolloutStageFailed {
            rollout_id,
            stage_idx,
            completed_count,
            failed_count,
            reason,
        } => {
            let reason_suffix = reason
                .as_ref()
                .map(|reason| format!(", reason={reason}"))
                .unwrap_or_default();
            format!(
                "rollout {} stage {} failed (ok={}, failed={}{})",
                rollout_id, stage_idx, completed_count, failed_count, reason_suffix
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oasis_core::core_types::{AgentId, BatchId, RolloutId, TaskId};
    use oasis_core::task_types::TaskState;

    #[test]
    fn test_render_event_summary_for_task_terminal() {
        let summary = render_event_summary(&OasisEventKind::TaskTerminal {
            task_id: TaskId::new("task-1"),
            agent_id: AgentId::new("agent-1"),
            batch_id: Some(BatchId::new("batch-1")),
            state: TaskState::Success,
        });

        assert!(summary.contains("task-1"));
        assert!(summary.contains("agent-1"));
        assert!(summary.contains("batch-1"));
    }

    #[test]
    fn test_render_event_summary_for_rollout_failure() {
        let summary = render_event_summary(&OasisEventKind::RolloutStageFailed {
            rollout_id: RolloutId::new("rollout-1"),
            stage_idx: 1,
            completed_count: 3,
            failed_count: 1,
            reason: Some("threshold exceeded".to_string()),
        });

        assert!(summary.contains("rollout-1"));
        assert!(summary.contains("threshold exceeded"));
    }

    #[test]
    fn test_render_event_summary_for_file_apply_failure() {
        let summary = render_event_summary(&OasisEventKind::FileApplyFailed {
            operation_id: oasis_core::core_types::OperationId::new(
                "123e4567-e89b-12d3-a456-426614174000".to_string(),
            ),
            agent_id: AgentId::new("agent-1"),
            source_path: "/tmp/app.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 42,
            reason: "permission denied".to_string(),
        });

        assert!(summary.contains("/tmp/app.conf"));
        assert!(summary.contains("/etc/app.conf"));
        assert!(summary.contains("permission denied"));
    }
}
