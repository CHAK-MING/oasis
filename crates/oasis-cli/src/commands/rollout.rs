//! 灰度发布命令
//! 覆盖灰度的发起、监控、控制与回滚全流程。

use crate::common::target::TargetSelector;
use anyhow::Result;
use clap::{Args, Subcommand};
use comfy_table::{presets::UTF8_FULL, Attribute, Cell, CellAlignment, ContentArrangement, Table};
use console::style;
use oasis_core::proto::{
    oasis_service_client::OasisServiceClient, task_target_msg, AbortRolloutRequest,
    CreateRolloutRequest, GetRolloutRequest, ListRolloutsRequest, PauseRolloutRequest,
    ResumeRolloutRequest, RollbackRolloutRequest, RolloutId, StartRolloutRequest, TaskId,
    TaskTargetMsg,
};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;

async fn resolve_rollout_id(
    client: &mut OasisServiceClient<Channel>,
    name_or_id: &str,
) -> Result<RolloutId> {
    // 优先按 ID 直接获取
    if client
        .get_rollout(GetRolloutRequest {
            rollout_id: Some(RolloutId {
                value: name_or_id.to_string(),
            }),
        })
        .await
        .is_ok()
    {
        return Ok(RolloutId {
            value: name_or_id.to_string(),
        });
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
        return Ok(r.id.unwrap_or_else(|| RolloutId {
            value: String::new(),
        }));
    }
    anyhow::bail!("未找到指定的灰度（按 id 或 name）: {}", name_or_id)
}

#[derive(Debug, Subcommand)]
pub enum RolloutCommand {
    /// 启动灰度发布
    Start(StartArgs),
    /// 查看灰度状态
    Status(StatusArgs),
    /// 暂停灰度
    Pause(PauseArgs),
    /// 恢复灰度
    Resume(ResumeArgs),
    /// 中止灰度
    Abort(AbortArgs),
    /// 回滚灰度
    Rollback(RollbackArgs),
    /// 列出灰度
    List(ListArgs),
    /// 校验灰度配置
    Validate(ValidateArgs),
}

#[derive(Debug, Args)]
pub struct StartArgs {
    /// 灰度名称
    #[arg(short, long)]
    pub name: String,

    /// 策略（canary, rolling, blue-green）
    #[arg(short, long, default_value = "canary")]
    pub strategy: String,

    /// 目标（CEL 选择器或逗号分隔的 Agent ID）
    #[arg(
        long,
        short = 't',
        value_name = "<TARGET>",
        help = "目标（CEL 选择器或 Agent ID）"
    )]
    pub target: String,

    /// 任务定义文件
    #[arg(long)]
    pub task_file: String,

    /// 灰度配置文件
    #[arg(short, long)]
    pub config_file: Option<String>,

    /// 批次大小（百分比或绝对值）
    #[arg(short, long, default_value = "10%")]
    pub batch_size: String,

    /// 批次间隔（秒）
    #[arg(short, long, default_value = "300")]
    pub interval: u64,

    /// 自动推进
    #[arg(long)]
    pub auto_advance: bool,

    /// 健康检查配置
    #[arg(long)]
    pub health_check: Option<String>,

    /// 超时（秒）
    #[arg(long, default_value = "3600")]
    pub timeout: u64,

    /// 标签（key=value）
    #[arg(long, action = clap::ArgAction::Append)]
    pub label: Vec<String>,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// 灰度 ID 或名称
    pub name: String,

    /// 详细输出
    #[arg(short, long)]
    pub verbose: bool,

    /// 输出格式（table, json, yaml）
    #[arg(short, long, default_value = "table")]
    pub format: String,

    /// 监控（每 N 秒刷新）
    #[arg(short, long)]
    pub watch: Option<u64>,
}

#[derive(Debug, Args)]
pub struct PauseArgs {
    /// 灰度 ID 或名称
    pub name: String,

    /// 原因
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct ResumeArgs {
    /// 灰度 ID 或名称
    pub name: String,
}

#[derive(Debug, Args)]
pub struct AbortArgs {
    /// 灰度 ID 或名称
    pub name: String,

    /// 原因
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct RollbackArgs {
    /// 灰度 ID 或名称
    pub name: String,

    /// 原因
    #[arg(long)]
    pub reason: Option<String>,

    /// 强制回滚
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct ListArgs {
    /// 输出格式（table, json, yaml）
    #[arg(short, long, default_value = "table")]
    pub format: String,

    /// 详细输出
    #[arg(short, long)]
    pub verbose: bool,
}

#[derive(Debug, Args)]
pub struct ValidateArgs {
    /// 配置文件路径
    pub config_file: String,

    /// 输出格式（table, json, yaml）
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
    println!("› 开始创建灰度任务: {}", style(&args.name).cyan());

    if args.target.is_empty() {
        println!("  {} 必须提供 --target 参数。", style("✖").red());
        return Ok(());
    }

    // 读取任务脚本/定义
    let task_content = tokio::fs::read_to_string(&args.task_file)
        .await
        .map_err(|e| anyhow::anyhow!("读取任务文件失败 {}: {}", args.task_file, e))?;
    println!("  {} 读取任务文件", style("✔").green());

    // 解析标签参数 key=value
    let mut labels = HashMap::new();
    for label in &args.label {
        if let Some((key, value)) = label.split_once('=') {
            labels.insert(key.to_string(), value.to_string());
        } else {
            anyhow::bail!("无效标签: {}（需 key=value）", label);
        }
    }
    println!("  {} 解析任务标签", style("✔").green());

    // 使用智能解析器统一处理目标
    let target_selector = TargetSelector::parse(&args.target)?;
    let target_selector = TargetSelector::parse(&args.target)?;
    let target_msg = TaskTargetMsg {
        target: Some(task_target_msg::Target::Selector(
            target_selector.expression().to_string(),
        )),
    };
    println!("  {} 解析目标选择器", style("✔").green());

    // 构造 TaskSpec 消息
    let task_msg = oasis_core::proto::TaskSpecMsg {
        id: Some(TaskId {
            value: String::new(),
        }),
        command: "sh".to_string(),
        args: vec!["-c".to_string(), task_content],
        env: HashMap::new(),
        timeout_seconds: args.timeout as u32,
        target: Some(target_msg),
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
            task: Some(task_msg),
            config: Some(config_msg),
            labels,
            target: Some(TaskTargetMsg {
                target: Some(task_target_msg::Target::Selector(
                    target_selector.expression().to_string(),
                )),
            }),
        })
        .await?;
    let rollout_id = create_response
        .into_inner()
        .rollout_id
        .unwrap_or_else(|| RolloutId {
            value: String::new(),
        });
    println!("  {} 创建灰度任务", style("✔").green());

    // 启动灰度
    client
        .start_rollout(StartRolloutRequest {
            rollout_id: Some(rollout_id.clone()),
        })
        .await?;
    println!("  {} 启动灰度任务", style("✔").green());

    println!(
        "\n{} 灰度任务 '{}' 已成功启动，ID: {}",
        style("✔").green(),
        style(&args.name).cyan(),
        style(rollout_id.value).yellow()
    );

    Ok(())
}

async fn show_rollout_status(
    args: StatusArgs,
    mut client: OasisServiceClient<Channel>,
) -> Result<()> {
    let rid = resolve_rollout_id(&mut client, &args.name).await?;

    loop {
        match client
            .get_rollout(GetRolloutRequest {
                rollout_id: Some(rid.clone()),
            })
            .await
        {
            Ok(response) => {
                let r = response
                    .into_inner()
                    .rollout
                    .ok_or_else(|| anyhow::anyhow!("未找到灰度任务"))?;

                println!("› {}: {}", style("灰度任务").bold(), style(&r.name).cyan());

                println!("  {:<10} {}", "ID:", style(r.id.unwrap().value).dim());
                println!("  {:<10} {}", "状态:", format_rollout_state(r.state));

                if let Some(p) = r.progress {
                    let progress_bar = format!(
                        "[{:27}]",
                        "=".repeat((p.completion_rate * 27.0).round() as usize)
                    );
                    println!(
                        "  {:<10} {} {}/{}, {:.1}%",
                        "进度:",
                        style(progress_bar).cyan(),
                        p.processed_nodes,
                        p.total_nodes,
                        p.completion_rate * 100.0
                    );
                }

                println!("  {:<10} {}", "创建于:", style(r.created_at).dim());
                println!("  {:<10} {}", "更新于:", style(r.updated_at).dim());
            }
            Err(e) => {
                println!("{} 获取状态失败: {}", style("✖").red(), e);
                break;
            }
        }

        if let Some(interval) = args.watch {
            sleep(Duration::from_secs(interval)).await;
            println!(""); // 换行
            continue;
        }
        break;
    }

    Ok(())
}

async fn pause_rollout(args: PauseArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    client
        .pause_rollout(PauseRolloutRequest {
            rollout_id: Some(rid),
            reason: args.reason.unwrap_or_else(|| "用户手动暂停".to_string()),
        })
        .await?;
    println!(
        "{} 灰度任务 '{}' 已暂停。",
        style("⏸").yellow(),
        style(args.name).cyan()
    );
    Ok(())
}

async fn resume_rollout(args: ResumeArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    client
        .resume_rollout(ResumeRolloutRequest {
            rollout_id: Some(rid),
        })
        .await?;
    println!(
        "{} 灰度任务 '{}' 已恢复。",
        style("▶").green(),
        style(args.name).cyan()
    );
    Ok(())
}

async fn abort_rollout(args: AbortArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    client
        .abort_rollout(AbortRolloutRequest {
            rollout_id: Some(rid),
            reason: args.reason.unwrap_or_else(|| "用户手动中止".to_string()),
        })
        .await?;
    println!(
        "{} 灰度任务 '{}' 已中止。",
        style("⏹").red(),
        style(args.name).cyan()
    );
    Ok(())
}

async fn rollback_rollout(
    args: RollbackArgs,
    mut client: OasisServiceClient<Channel>,
) -> Result<()> {
    let rid = resolve_rollout_id(&mut client, &args.name).await?;
    client
        .rollback_rollout(RollbackRolloutRequest {
            rollout_id: Some(rid),
            reason: args.reason.unwrap_or_else(|| "用户手动回滚".to_string()),
        })
        .await?;
    println!(
        "{} 灰度任务 '{}' 已触发回滚。",
        style("🔄").yellow(),
        style(args.name).cyan()
    );
    Ok(())
}

async fn list_rollouts(args: ListArgs, mut client: OasisServiceClient<Channel>) -> Result<()> {
    let response = client
        .list_rollouts(ListRolloutsRequest {
            status_filter: String::new(),
            limit: if args.verbose { 100 } else { 20 },
        })
        .await?;

    let rollouts = response.into_inner().rollouts;
    if rollouts.is_empty() {
        println!("{}", style("未找到任何灰度任务。").yellow());
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        Cell::new("ID").add_attribute(Attribute::Bold),
        Cell::new("名称").add_attribute(Attribute::Bold),
        Cell::new("状态").add_attribute(Attribute::Bold),
        Cell::new("进度").add_attribute(Attribute::Bold),
        Cell::new("创建于").add_attribute(Attribute::Bold),
    ]);
    for r in rollouts {
        let id = r.id.as_ref().map(|x| x.value.clone()).unwrap_or_default();
        let name = r.name;
        let status = format_rollout_state(r.state);
        let progress = r
            .progress
            .as_ref()
            .map(|p| {
                format!(
                    "{}/{} ({:.0}%)",
                    p.processed_nodes,
                    p.total_nodes,
                    p.completion_rate * 100.0
                )
            })
            .unwrap_or_else(|| "-".to_string());
        let created = r.created_at.to_string();
        table.add_row(vec![
            Cell::new(id),
            Cell::new(name),
            Cell::new(status),
            Cell::new(progress),
            Cell::new(created),
        ]);
        if let Some(column) = table.column_mut(2) {
            column.set_cell_alignment(CellAlignment::Center);
        }
    }
    println!("{}", table);

    Ok(())
}

async fn validate_rollout_config(args: ValidateArgs) -> Result<()> {
    println!(
        "› 正在校验灰度配置文件: {}",
        style(&args.config_file).cyan()
    );

    let config_content = tokio::fs::read_to_string(&args.config_file)
        .await
        .map_err(|e| anyhow::anyhow!("读取配置失败 {}: {}", args.config_file, e))?;

    let config: serde_json::Value = serde_json::from_str(&config_content)
        .map_err(|e| anyhow::anyhow!("JSON 格式无效: {}", e))?;

    println!(
        "  {} 配置文件校验通过 (JSON 格式有效)。",
        style("✔").green()
    );
    println!("{}", style(serde_json::to_string_pretty(&config)?).dim());

    Ok(())
}

fn format_rollout_state(state: i32) -> String {
    // 兼容 proto 中的枚举值：参考 oasis_core::proto::RolloutStateEnum
    match state {
        0 => style("创建").dim().to_string(),
        1 => style("批次执行").cyan().to_string(),
        2 => style("等待下一批").cyan().to_string(),
        3 => style("已暂停").yellow().to_string(),
        4 => style("已成功").green().to_string(),
        5 => style("已失败").red().to_string(),
        6 => style("已中止").red().to_string(),
        7 => style("回滚中").yellow().to_string(),
        _ => style("未知").dim().to_string(),
    }
}
