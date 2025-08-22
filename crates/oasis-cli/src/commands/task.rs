use anyhow::Result;
use clap::Subcommand;
use indicatif::{ProgressBar, ProgressStyle};
use oasis_core::proto::{
    GetTaskResultRequest, StreamTaskResultsRequest, oasis_service_client::OasisServiceClient,
};

#[derive(Subcommand, Debug)]
#[command(name = "task", about = "Query or watch task results")]
pub enum TaskCommands {
    /// Get a single task result (optionally wait)
    Get {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = 0)]
        wait_ms: u64,
    },
    /// Stream task results in real time
    Watch {
        #[arg(long)]
        id: String,
    },
}

pub async fn run_task(
    mut client: OasisServiceClient<tonic::transport::Channel>,
    cmd: TaskCommands,
) -> Result<()> {
    match cmd {
        TaskCommands::Get { id, wait_ms } => {
            let pb = if wait_ms > 0 {
                Some(spinner("waiting for result"))
            } else {
                None
            };
            let resp = client
                .get_task_result(GetTaskResultRequest {
                    task_id: id.clone(),
                    agent_id: String::new(), // 空字符串：不指定特定 agent（返回任意一个结果）
                    wait_timeout_ms: wait_ms as i64,
                })
                .await?;
            if let Some(pb) = pb {
                pb.finish_and_clear();
            }
            let r = resp.get_ref();
            if !r.found {
                println!("Result not found for task {}", id);
                return Ok(());
            }
            println!(
                "task_id={} agent_id={} exit_code={} duration_ms={}",
                r.task_id, r.agent_id, r.exit_code, r.duration_ms
            );
            if !r.stdout.is_empty() {
                println!("stdout:\n{}", r.stdout);
            }
            if !r.stderr.is_empty() {
                eprintln!("stderr:\n{}", r.stderr);
            }
        }
        TaskCommands::Watch { id } => {
            println!("Watching results for task {}... (Ctrl+C to stop)", id);
            let mut stream = client
                .stream_task_results(StreamTaskResultsRequest {
                    task_id: id.clone(),
                })
                .await?
                .into_inner();
            while let Some(item) = stream.message().await? {
                let status = match item.exit_code {
                    0 => "SUCCESS",
                    -1 => "FAILED",
                    -2 => "CANCELLED",
                    -3 => "TIMEOUT",
                    _ => "RUNNING",
                };
                println!("\n===== Task Result =====");
                println!(
                    "time={}  agent={}  status={}  exit_code={}  duration_ms={}",
                    item.timestamp, item.agent_id, status, item.exit_code, item.duration_ms
                );
                if !item.stdout.is_empty() {
                    println!("--- stdout ---\n{}", item.stdout);
                }
                if !item.stderr.is_empty() {
                    eprintln!("--- stderr ---\n{}", item.stderr);
                }
            }
            println!("\nStream ended.");
        }
    }
    Ok(())
}

fn spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(120));
    pb.set_message(msg.to_string());
    pb
}
