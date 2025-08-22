//! 灰度发布命令
//! 覆盖灰度的发起、监控、控制与回滚全流程。

use anyhow::Result;
use clap::{Args, Subcommand};
use oasis_core::proto::{
    AbortRolloutRequest, CreateRolloutRequest, GetRolloutRequest, ListRolloutsRequest,
    PauseRolloutRequest, ResolveSelectorRequest, ResumeRolloutRequest, RollbackRolloutRequest,
    StartRolloutRequest, oasis_service_client::OasisServiceClient,
};
use oasis_core::selector::CelSelector;
use std::collections::HashMap;
use tonic::transport::Channel;
async fn resolve_rollout_id(
    client: &mut OasisServiceClient<Channel>,
    name_or_id: &str,
) -> Result<String> {
    // 优先按 ID 直接获取
    if client
        .get_rollout(GetRolloutRequest {
            rollout_id: name_or_id.to_string(),
        })
        .await
        .is_ok()
    {
        return Ok(name_or_id.to_string());
    }

    // 回退：列举后按名称匹配
    let list = client
        .list_rollouts(ListRolloutsRequest {
            status_filter: String::new(),
            limit: 100,
        })
        .await?
        .into_inner()
        .rollouts;
    if let Some(r) = list.into_iter().find(|r| r.name == name_or_id) {
        return Ok(r.id);
    }
    anyhow::bail!("Rollout not found by id or name: {}", name_or_id)
}

#[derive(Debug, Subcommand)]
pub enum RolloutCommand {
    /// Start a rollout
    Start(StartArgs),
    /// Show rollout status
    Status(StatusArgs),
    /// Pause rollout
    Pause(PauseArgs),
    /// Resume rollout
    Resume(ResumeArgs),
    /// Abort rollout
    Abort(AbortArgs),
    /// Rollback rollout
    Rollback(RollbackArgs),
    /// List rollouts
    List(ListArgs),
    /// Validate rollout config file
    Validate(ValidateArgs),
}

#[derive(Debug, Args)]
pub struct StartArgs {
    /// Rollout name
    #[arg(short, long)]
    pub name: String,

    /// Strategy (canary, rolling, blue-green)
    #[arg(short, long, default_value = "canary")]
    pub strategy: String,

    /// Target selector (CEL)
    #[arg(long, value_name = "<CEL>")]
    pub selector: String,

    /// Task definition file
    #[arg(short, long)]
    pub task_file: String,

    /// Rollout config file
    #[arg(short, long)]
    pub config_file: Option<String>,

    /// Batch size (percentage or absolute)
    #[arg(short, long, default_value = "10%")]
    pub batch_size: String,

    /// Batch interval (seconds)
    #[arg(short, long, default_value = "300")]
    pub interval: u64,

    /// Auto advance
    #[arg(long)]
    pub auto_advance: bool,

    /// Health check config
    #[arg(long)]
    pub health_check: Option<String>,

    /// Timeout (seconds)
    #[arg(long, default_value = "3600")]
    pub timeout: u64,

    /// Labels (key=value)
    #[arg(long, action = clap::ArgAction::Append)]
    pub label: Vec<String>,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Rollout ID or name
    pub name: String,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Output format (table, json, yaml)
    #[arg(short, long, default_value = "table")]
    pub format: String,

    /// Watch (refresh every N seconds)
    #[arg(short, long)]
    pub watch: Option<u64>,
}

#[derive(Debug, Args)]
pub struct PauseArgs {
    /// Rollout ID or name
    pub name: String,

    /// Reason
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct ResumeArgs {
    /// Rollout ID or name
    pub name: String,
}

#[derive(Debug, Args)]
pub struct AbortArgs {
    /// Rollout ID or name
    pub name: String,

    /// Reason
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct RollbackArgs {
    /// Rollout ID or name
    pub name: String,

    /// Reason
    #[arg(long)]
    pub reason: Option<String>,

    /// Force rollback
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct ListArgs {
    /// Output format (table, json, yaml)
    #[arg(short, long, default_value = "table")]
    pub format: String,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,
}

#[derive(Debug, Args)]
pub struct ValidateArgs {
    /// Config file path
    pub config_file: String,

    /// Output format (table, json, yaml)
    #[arg(short, long, default_value = "table")]
    pub format: String,
}

pub async fn run_rollout(
    command: RolloutCommand,
    client: OasisServiceClient<Channel>,
) -> Result<()> {
    match command {
        RolloutCommand::Start(args) => start_rollout(args, client).await,
        RolloutCommand::Status(args) => show_rollout_status(args, client).await,
        RolloutCommand::Pause(args) => pause_rollout(args, client).await,
        RolloutCommand::Resume(args) => resume_rollout(args, client).await,
        RolloutCommand::Abort(args) => abort_rollout(args, client).await,
        RolloutCommand::Rollback(args) => rollback_rollout(args, client).await,
        RolloutCommand::List(args) => list_rollouts(args, client).await,
        RolloutCommand::Validate(args) => validate_rollout_config(args).await,
    }
}

async fn start_rollout(args: StartArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    println!("Starting rollout: {}", args.name);

    // 本地校验选择器语法
    let _selector = CelSelector::new(args.selector.clone())
        .map_err(|e| anyhow::anyhow!("Invalid selector: {}", e))?;
    println!("Selector validated: {}", args.selector);

    // 服务端解析选择器，获取最终目标集
    let response = client
        .resolve_selector(ResolveSelectorRequest {
            selector_expression: args.selector.clone(),
        })
        .await?;
    let result = response.into_inner();
    if !result.error_message.is_empty() {
        anyhow::bail!("Selector resolution failed: {}", result.error_message);
    }

    println!("Targets:");
    println!("  total: {}", result.total_nodes);
    println!("  matched: {}", result.matched_nodes);
    println!("  ids: {:?}", result.agent_ids);

    // 读取任务脚本/定义
    let task_content = tokio::fs::read_to_string(&args.task_file)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read task file {}: {}", args.task_file, e))?;

    // 解析标签参数 key=value
    let mut labels = HashMap::new();
    for label in &args.label {
        if let Some((key, value)) = label.split_once('=') {
            labels.insert(key.to_string(), value.to_string());
        } else {
            return Err(anyhow::anyhow!(
                "Invalid label: {} (expected key=value)",
                label
            ));
        }
    }

    // 构造 TaskSpec 消息
    let task_msg = oasis_core::proto::TaskSpecMsg {
        id: String::new(),
        command: "sh".to_string(),
        args: vec!["-c".to_string(), task_content],
        env: HashMap::new(),
        target_agents: vec![],
        selector: String::new(),
        timeout_seconds: args.timeout as u32,
    };

    // 构造 RolloutConfig 消息并创建灰度
    let strategy = match args.strategy.as_str() {
        "rolling" => oasis_core::proto::rollout_config_msg::Strategy::Rolling(
            oasis_core::proto::RollingStrategyMsg {
                batch_size: Some(oasis_core::proto::BatchSizeMsg {
                    kind: if args.batch_size.ends_with('%') {
                        let pct: f64 = args
                            .batch_size
                            .trim_end_matches('%')
                            .parse()
                            .unwrap_or(10.0);
                        Some(oasis_core::proto::batch_size_msg::Kind::Percentage(pct))
                    } else {
                        let cnt: u32 = args.batch_size.parse().unwrap_or(1);
                        Some(oasis_core::proto::batch_size_msg::Kind::Count(cnt))
                    },
                }),
                batch_delay_secs: args.interval,
                max_failures: 3,
            },
        ),
        "canary" => oasis_core::proto::rollout_config_msg::Strategy::Canary(
            oasis_core::proto::CanaryStrategyMsg {
                percentage: 10.0,
                observation_duration_secs: args.interval,
            },
        ),
        "blue-green" | "blue_green" => oasis_core::proto::rollout_config_msg::Strategy::BlueGreen(
            oasis_core::proto::BlueGreenStrategyMsg {
                switch_percentage: 100.0,
                warmup_secs: args.interval,
            },
        ),
        _ => oasis_core::proto::rollout_config_msg::Strategy::Rolling(
            oasis_core::proto::RollingStrategyMsg {
                batch_size: Some(oasis_core::proto::BatchSizeMsg {
                    kind: Some(oasis_core::proto::batch_size_msg::Kind::Percentage(10.0)),
                }),
                batch_delay_secs: args.interval,
                max_failures: 3,
            },
        ),
    };

    let config_msg = oasis_core::proto::RolloutConfigMsg {
        strategy: Some(strategy),
        timeout_seconds: args.timeout,
        auto_advance: args.auto_advance,
        health_check: args.health_check.unwrap_or_default(),
        labels: labels.clone(),
    };

    let create_response = client
        .create_rollout(CreateRolloutRequest {
            name: args.name.clone(),
            target_selector: args.selector,
            task: Some(task_msg),
            config: Some(config_msg),
            labels,
        })
        .await?;
    let rollout_id = create_response.into_inner().rollout_id;

    println!("Created rollout: {}", rollout_id);

    // 启动灰度
    let _start_response = client
        .start_rollout(StartRolloutRequest {
            rollout_id: rollout_id.clone(),
        })
        .await?;
    println!("Rollout started: {}", rollout_id);

    Ok(())
}

async fn show_rollout_status(
    args: StatusArgs,
    mut client: OasisServiceClient<Channel>,
) -> Result<()> {
    println!("Rollout status: {}", args.name);
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    match client
        .get_rollout(GetRolloutRequest { rollout_id: rid })
        .await
    {
        Ok(response) => {
            let r = response
                .into_inner()
                .rollout
                .ok_or_else(|| anyhow::anyhow!("not found"))?;
            println!("id: {}", r.id);
            println!("status: {:?}", r.state);
            if let Some(p) = r.progress {
                println!("progress: {}/{} nodes", p.processed_nodes, p.total_nodes);
                println!("success_rate: {:.1}%", p.completion_rate * 100.0);
            }
            println!("created_at: {}", r.created_at);
            println!("updated_at: {}", r.updated_at);
        }
        Err(e) => {
            anyhow::bail!("Failed to get rollout status: {}", e);
        }
    }

    Ok(())
}

async fn pause_rollout(args: PauseArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    println!("Pausing rollout: {}", args.name);
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    let _response = client
        .pause_rollout(PauseRolloutRequest {
            rollout_id: rid,
            reason: args.reason.unwrap_or_else(|| "user".to_string()),
        })
        .await?;
    println!("Paused");
    Ok(())
}

async fn resume_rollout(args: ResumeArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    println!("Resuming rollout: {}", args.name);
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    let _response = client
        .resume_rollout(ResumeRolloutRequest { rollout_id: rid })
        .await?;
    println!("Resumed");
    Ok(())
}

async fn abort_rollout(args: AbortArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    println!("Aborting rollout: {}", args.name);
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    let _response = client
        .abort_rollout(AbortRolloutRequest {
            rollout_id: rid,
            reason: args.reason.unwrap_or_else(|| "user".to_string()),
        })
        .await?;
    println!("Aborted");
    Ok(())
}

async fn rollback_rollout(
    args: RollbackArgs,
    mut client: OasisServiceClient<Channel>,
) -> Result<()> {
    println!("Rolling back rollout: {}", args.name);
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    let _response = client
        .rollback_rollout(RollbackRolloutRequest {
            rollout_id: rid,
            reason: args.reason.unwrap_or_else(|| "user".to_string()),
        })
        .await?;
    println!("Rollback started");
    Ok(())
}

async fn list_rollouts(args: ListArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    println!("Listing rollouts"); // 保持输出英文以与其他 CLI 文本一致

    match client
        .list_rollouts(ListRolloutsRequest {
            status_filter: String::new(),
            limit: if args.verbose { 100 } else { 20 },
        })
        .await
    {
        Ok(response) => {
            let rollouts = response.into_inner().rollouts;
            if rollouts.is_empty() {
                println!("no rollouts");
            } else {
                println!("found {} rollouts:", rollouts.len());
                for r in rollouts {
                    println!("  {} - {} ({:?})", r.id, r.name, r.state);
                    if let Some(p) = r.progress.clone() {
                        if args.verbose {
                            println!("    progress: {}/{}", p.processed_nodes, p.total_nodes);
                            println!("    success_rate: {:.1}%", p.completion_rate * 100.0);
                            println!("    created_at: {}", r.created_at);
                        }
                    }
                }
            }
        }
        Err(e) => {
            anyhow::bail!("Failed to list rollouts: {}", e);
        }
    }

    Ok(())
}

async fn validate_rollout_config(args: ValidateArgs) -> Result<()> {
    println!("Validating rollout config: {}", args.config_file); // 英文输出

    let config_content = tokio::fs::read_to_string(&args.config_file)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read config {}: {}", args.config_file, e))?;

    let config: serde_json::Value = serde_json::from_str(&config_content)
        .map_err(|e| anyhow::anyhow!("Invalid JSON: {}", e))?;

    println!("Valid JSON");
    println!("{}", serde_json::to_string_pretty(&config)?);

    Ok(())
}
