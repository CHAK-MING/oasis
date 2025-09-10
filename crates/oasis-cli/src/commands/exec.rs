use crate::client::format_grpc_error;
use crate::grpc_retry;
use crate::ui::{print_header, print_info, print_next_step, print_status, print_warning};
use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use comfy_table::{
    Attribute, Cell, CellAlignment, Color, ContentArrangement, Table, presets::UTF8_FULL,
};
use console::style;
use oasis_core::proto::{
    BatchMsg, BatchRequestMsg, CancelBatchRequest, GetBatchDetailsRequest, ListBatchesRequest,
    SelectorExpression, SubmitBatchRequest, TaskExecutionMsg, TaskStateEnum,
    oasis_service_client::OasisServiceClient,
};
use std::fmt::Write as FmtWrite;

/// exec 子命令集合
#[derive(Parser, Debug)]
#[command(
    name = "exec",
    about = "在匹配到的 Agent 上执行命令，管理任务生命周期",
    after_help = r#"示例：
  # 提交任务（异步执行）
  oasis-cli exec run -t 'labels["role"] == "web"' -- /usr/bin/uptime

  # 获取批量任务列表
  oasis-cli exec get <batch_id>

  # 列出全部批量任务
  oasis-cli exec list --limit 20

  # 取消批量任务（只支持批量取消）
  oasis-cli exec cancel <batch_id>
"#
)]
pub struct ExecArgs {
    #[command(subcommand)]
    pub cmd: ExecCmd,
}

#[derive(Subcommand, Debug)]
pub enum ExecCmd {
    /// 提交任务
    Run(ExecRunArgs),
    /// 获取批量任务列表
    Get(ExecGetArgs),
    /// 获取任务列表
    List(ExecListArgs),
    /// 取消指定任务
    Cancel(ExecCancelArgs),
}

#[derive(Parser, Debug)]
pub struct ExecRunArgs {
    /// 目标（选择器语法）
    #[arg(long, short = 't', help = "目标（选择器语法）", default_value = "all")]
    pub target: String,

    /// 要执行的命令与参数，必须在 -- 之后给出
    #[arg(
        last = true,
        required = true,
        help = "要执行的命令与参数（置于 -- 之后）"
    )]
    pub command: Vec<String>,

    /// 超时时间（秒），默认300秒
    #[arg(long, default_value_t = 300)]
    pub timeout: u32,

    /// 任务优先级
    #[arg(long, default_value = "normal")]
    pub priority: String,
}

#[derive(Parser, Debug)]
pub struct ExecGetArgs {
    /// 批量任务 ID
    pub batch_id: String,
    /// 筛选状态
    #[arg(long, value_delimiter = ',')]
    pub states: Option<Vec<String>>,
}

#[derive(Parser, Debug)]
pub struct ExecListArgs {
    /// 限制返回数量
    #[arg(long, default_value_t = 20)]
    pub limit: u32,
    /// 筛选状态
    #[arg(long, value_delimiter = ',')]
    pub states: Option<Vec<String>>,
}

#[derive(Parser, Debug)]
pub struct ExecCancelArgs {
    /// 批次 ID
    pub batch_id: String,
}

/// 主入口函数 - 根据子命令分发执行
pub async fn run_exec(
    mut client: OasisServiceClient<tonic::transport::Channel>,
    args: ExecArgs,
) -> Result<()> {
    match args.cmd {
        ExecCmd::Run(run) => run_exec_run(&mut client, run).await,
        ExecCmd::Get(get) => run_exec_get(&mut client, get).await,
        ExecCmd::List(list) => run_exec_list(&mut client, list).await,
        ExecCmd::Cancel(cancel) => run_exec_cancel(&mut client, cancel).await,
    }
}

/// 执行任务提交命令
async fn run_exec_run(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: ExecRunArgs,
) -> Result<()> {
    print_header(&format!(
        "提交批量任务: {}",
        style(args.command.join(" ")).cyan()
    ));

    // 解析命令和参数
    let (cmd, command_args) = args
        .command
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("命令不能为空"))?;

    // 构建请求
    let request = SubmitBatchRequest {
        batch_request: Some(BatchRequestMsg {
            command: cmd.to_string(),
            args: command_args.iter().map(|s| s.to_string()).collect(),
            target: Some(SelectorExpression {
                expression: args.target.clone(),
            }),
            timeout_seconds: args.timeout,
        }),
    };

    // 提交任务
    print_status("正在提交任务...", true);
    let base_req = request.clone();
    let response = grpc_retry!(client, submit_batch(base_req.clone()))
        .await
        .map_err(|e| anyhow!("提交任务失败: {}", format_grpc_error(&e)))?;
    let response = response.into_inner();

    let batch_id = response
        .batch_id
        .ok_or_else(|| anyhow!("服务器没有返回批次ID"))?
        .value;

    print_info(&format!("批量任务已提交成功"));
    print_info(&format!("批次ID: {}", style(&batch_id).green().bold()));
    print_info(&format!("已经创建 {} 个任务", response.agent_nums));

    print_next_step(&format!(
        "使用 'oasis-cli exec get {}' 查看批量任务列表",
        batch_id
    ));
    Ok(())
}

async fn run_exec_get(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: ExecGetArgs,
) -> Result<()> {
    print_header(&format!("批次详情: {}", style(&args.batch_id).cyan()));

    let state_filters = args
        .states
        .map(|states| {
            states
                .into_iter()
                .map(|s| parse_task_state(&s))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;

    // 获取批次详情
    let details_request = GetBatchDetailsRequest {
        batch_id: Some(oasis_core::proto::BatchId {
            value: args.batch_id.clone(),
        }),
        states: state_filters.unwrap_or_default(),
    };

    let base_req = details_request.clone();
    let batch_details = match grpc_retry!(client, get_batch_details(base_req.clone())).await {
        Ok(response) => response.into_inner(),
        Err(e) => {
            print_status("获取批次信息失败", false);
            return Err(anyhow!("查询失败: {}", format_grpc_error(&e)));
        }
    };

    if batch_details.tasks.is_empty() {
        print_info("该批次没有找到任务");
        return Ok(());
    }

    // 显示任务详情表格
    display_task_executions(&batch_details.tasks)?;

    // 显示统计信息
    display_batch_statistics(&batch_details.tasks);

    Ok(())
}

/// 列出任务
async fn run_exec_list(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: ExecListArgs,
) -> Result<()> {
    print_header("批量任务列表");

    // 解析状态筛选
    let state_filters = args
        .states
        .map(|states| {
            states
                .into_iter()
                .map(|s| parse_task_state(&s))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;

    let request = ListBatchesRequest {
        limit: args.limit,
        states: state_filters.unwrap_or_default(),
    };

    let base_req = request.clone();
    let response = grpc_retry!(client, list_batches(base_req.clone()))
        .await
        .map_err(|e| anyhow!("获取批量任务列表失败: {}", format_grpc_error(&e)))?;
    let response = response.into_inner();

    if response.batches.is_empty() {
        print_info("未找到批次");
        return Ok(());
    }

    // 显示批次列表
    display_batch_list(&response.batches)?;

    print_info(&format!(
        "显示 {} 个批次任务，总数: {}",
        response.batches.len(),
        response.total_count
    ));

    if response.has_more {
        print_next_step(&format!("还有更多批次任务，当前显示前 {} 个", args.limit));
    }

    Ok(())
}

/// 取消任务
async fn run_exec_cancel(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: ExecCancelArgs,
) -> Result<()> {
    print_header(&format!("取消批次任务: {}", style(&args.batch_id).cyan()));

    let request = CancelBatchRequest {
        batch_id: Some(oasis_core::proto::BatchId {
            value: args.batch_id.clone(),
        }),
    };

    let base_req = request.clone();
    let response = grpc_retry!(client, cancel_batch(base_req.clone()))
        .await
        .map_err(|e| anyhow!("取消批次任务失败: {}", format_grpc_error(&e)))?;
    let response = response.into_inner();

    if response.success {
        print_info("批次任务已取消");
    } else {
        print_warning("批次任务取消失败");
    }

    Ok(())
}

fn format_output(s: &str, color: Option<Color>) -> Cell {
    if s.is_empty() {
        return Cell::new("-").set_alignment(CellAlignment::Left);
    }
    let lines: Vec<&str> = s.lines().collect();
    let max_lines = 3;
    let max_len = 40;
    let mut formatted = String::new();
    for (i, line) in lines.iter().take(max_lines).enumerate() {
        let mut l = line.chars().take(max_len).collect::<String>();
        if line.chars().count() > max_len {
            l.push_str("...");
        }
        if i > 0 {
            formatted.push('\n');
        }
        write!(formatted, "{}", l).unwrap();
    }
    if lines.len() > max_lines {
        formatted.push_str("\n...");
    }
    let mut cell = Cell::new(formatted).set_alignment(CellAlignment::Left);
    if let Some(c) = color {
        cell = cell.fg(c);
    }
    cell
}

// 你的 display_task_executions 函数
fn display_task_executions(executions: &[TaskExecutionMsg]) -> Result<()> {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_width(120);

    table.set_header(vec![
        Cell::new("任务ID")
            .add_attribute(Attribute::Bold)
            .set_alignment(CellAlignment::Center),
        Cell::new("Agent")
            .add_attribute(Attribute::Bold)
            .set_alignment(CellAlignment::Center),
        Cell::new("状态")
            .add_attribute(Attribute::Bold)
            .set_alignment(CellAlignment::Center),
        Cell::new("退出码")
            .add_attribute(Attribute::Bold)
            .set_alignment(CellAlignment::Center),
        Cell::new("输出")
            .add_attribute(Attribute::Bold)
            .set_alignment(CellAlignment::Center),
        Cell::new("错误")
            .add_attribute(Attribute::Bold)
            .set_alignment(CellAlignment::Center),
        Cell::new("耗时")
            .add_attribute(Attribute::Bold)
            .set_alignment(CellAlignment::Center),
        Cell::new("创建时间")
            .add_attribute(Attribute::Bold)
            .set_alignment(CellAlignment::Center),
    ]);

    for execution in executions {
        let task_id = execution
            .task_id
            .as_ref()
            .map(|id| &id.value[..8])
            .unwrap_or("unknown");
        let agent_id = execution
            .agent_id
            .as_ref()
            .map(|id| id.value.as_str())
            .unwrap_or("unknown");
        let state = execution.state();
        let state_cn = state_to_cn(state);
        let exit_code = execution
            .exit_code
            .map(|code| code.to_string())
            .unwrap_or("-".to_string());
        let duration = if let Some(dur) = execution.duration_ms {
            format!("{:.3}s", dur / 1000.0)
        } else {
            "-".to_string()
        };
        let created_at = format_timestamp(execution.started_at);

        let mut state_cell = Cell::new(state_cn).set_alignment(CellAlignment::Center);
        state_cell = match state {
            TaskStateEnum::TaskCreated => state_cell.fg(Color::DarkGrey),
            TaskStateEnum::TaskPending => state_cell.fg(Color::Yellow),
            TaskStateEnum::TaskRunning => state_cell.fg(Color::Blue),
            TaskStateEnum::TaskSuccess => {
                state_cell.fg(Color::Green).add_attribute(Attribute::Bold)
            }
            TaskStateEnum::TaskFailed => state_cell.fg(Color::Red).add_attribute(Attribute::Bold),
            TaskStateEnum::TaskTimeout => state_cell.fg(Color::Magenta),
            TaskStateEnum::TaskCancelled => state_cell.fg(Color::DarkGrey),
        };

        let stdout_cell = format_output(&execution.stdout, Some(Color::Yellow));
        let stderr_cell = format_output(&execution.stderr, Some(Color::Red));

        table.add_row(vec![
            Cell::new(task_id).set_alignment(CellAlignment::Center),
            Cell::new(agent_id).set_alignment(CellAlignment::Center),
            state_cell,
            Cell::new(exit_code).set_alignment(CellAlignment::Center),
            stdout_cell,
            stderr_cell,
            Cell::new(duration).set_alignment(CellAlignment::Center),
            Cell::new(created_at).set_alignment(CellAlignment::Center),
        ]);
    }

    println!("{}", table);
    Ok(())
}

/// 显示批次统计信息
fn display_batch_statistics(executions: &[TaskExecutionMsg]) {
    let total = executions.len();
    let mut success = 0;
    let mut failed = 0;
    let mut running = 0;
    let mut pending = 0;
    let mut timeout = 0;
    let mut cancelled = 0;

    for execution in executions {
        match execution.state() {
            TaskStateEnum::TaskSuccess => success += 1,
            TaskStateEnum::TaskFailed => failed += 1,
            TaskStateEnum::TaskRunning => running += 1,
            TaskStateEnum::TaskPending | TaskStateEnum::TaskCreated => pending += 1,
            TaskStateEnum::TaskTimeout => timeout += 1,
            TaskStateEnum::TaskCancelled => cancelled += 1,
        }
    }

    println!();
    print_header("统计信息");

    if success > 0 {
        print_info(&format!("✓ 成功: {}", style(success).green().bold()));
    }
    if failed > 0 {
        print_info(&format!("✗ 失败: {}", style(failed).red().bold()));
    }
    if running > 0 {
        print_info(&format!("⏳ 运行中: {}", style(running).blue().bold()));
    }
    if pending > 0 {
        print_info(&format!("⌛ 等待中: {}", style(pending).yellow().bold()));
    }
    if timeout > 0 {
        print_info(&format!("⏰ 超时: {}", style(timeout).magenta().bold()));
    }
    if cancelled > 0 {
        print_info(&format!("🚫 已取消: {}", style(cancelled).dim().bold()));
    }

    let success_rate = if total > 0 {
        (success as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    print_info(&format!(
        "总计: {} | 成功率: {}",
        style(total).bold(),
        style(format!("{:.1}%", success_rate)).green().bold()
    ));
}

/// 解析任务状态字符串
fn parse_task_state(state_str: &str) -> Result<i32> {
    match state_str.to_lowercase().as_str() {
        "created" => Ok(TaskStateEnum::TaskCreated as i32),
        "pending" => Ok(TaskStateEnum::TaskPending as i32),
        "running" => Ok(TaskStateEnum::TaskRunning as i32),
        "success" => Ok(TaskStateEnum::TaskSuccess as i32),
        "failed" => Ok(TaskStateEnum::TaskFailed as i32),
        "timeout" => Ok(TaskStateEnum::TaskTimeout as i32),
        "cancelled" => Ok(TaskStateEnum::TaskCancelled as i32),
        _ => Err(anyhow!("无效的任务状态: {}", state_str)),
    }
}

/// 显示任务列表
fn display_batch_list(batches: &[BatchMsg]) -> Result<()> {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);

    table.set_header(vec![
        Cell::new("批次任务ID").add_attribute(Attribute::Bold),
        Cell::new("命令").add_attribute(Attribute::Bold),
        Cell::new("参数").add_attribute(Attribute::Bold),
        Cell::new("创建时间").add_attribute(Attribute::Bold),
    ]);

    for batch in batches {
        let batch_id = batch
            .batch_id
            .as_ref()
            .map(|id| id.value.clone())
            .unwrap_or_else(|| "unknown".to_string());

        let command = batch.command.clone();
        let args = batch.args.join(" ");
        let created_at = format_timestamp(batch.created_at);

        table.add_row(vec![
            Cell::new(batch_id),
            Cell::new(command),
            Cell::new(args),
            Cell::new(created_at),
        ]);
    }

    println!("{}", table);

    Ok(())
}

/// 格式化时间戳
fn format_timestamp(timestamp: i64) -> String {
    crate::time::format_local_ts(timestamp)
}

// 状态中文映射
fn state_to_cn(state: TaskStateEnum) -> &'static str {
    match state {
        TaskStateEnum::TaskCreated => "已创建",
        TaskStateEnum::TaskPending => "等待中",
        TaskStateEnum::TaskRunning => "执行中",
        TaskStateEnum::TaskSuccess => "成功",
        TaskStateEnum::TaskFailed => "失败",
        TaskStateEnum::TaskTimeout => "超时",
        TaskStateEnum::TaskCancelled => "已取消",
    }
}
