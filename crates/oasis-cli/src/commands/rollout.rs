use crate::client::format_grpc_error;
use crate::grpc_retry;
use crate::ui::{
    confirm_action, log_detail, print_header, print_info, print_next_step, print_warning,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use clap::{Parser, Subcommand};
use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table, presets::UTF8_FULL};
use console::style;
use oasis_core::proto::{
    AdvanceRolloutRequest, CreateRolloutRequest, GetRolloutStatusRequest, ListRolloutsRequest,
    RolloutId, RolloutStateEnum, oasis_service_client::OasisServiceClient,
};
use std::path::PathBuf;

/// 灰度发布管理
#[derive(Parser, Debug)]
#[command(
    name = "rollout",
    about = "管理灰度发布",
    after_help = r#"示例：
  # 创建命令灰度发布
  oasis-cli rollout create \
    --name "系统更新" \
    --target 'labels["role"] == "web"' \
    --strategy percentage:10,30,60,100 \
    --command "apt update && apt upgrade -y" \
    --timeout 300

  # 创建文件灰度发布
  oasis-cli rollout create \
    --name "配置更新" \
    --target 'labels["environment"] == "prod"' \
    --strategy count:2,5,10,0 \
    --file-src ./nginx.conf \
    --file-dest /etc/nginx/nginx.conf \
    --file-mode 0644

  # 查看发布状态
  oasis-cli rollout status rollout-12345678

  # 推进到下一阶段
  oasis-cli rollout advance rollout-12345678

  # 回滚发布
  oasis-cli rollout rollback rollout-12345678 --rollback-cmd "systemctl restart nginx"
"#
)]
pub struct RolloutArgs {
    #[command(subcommand)]
    pub cmd: RolloutCmd,
}

#[derive(Subcommand, Debug)]
pub enum RolloutCmd {
    /// 创建灰度发布
    Create(CreateArgs),
    /// 查看发布状态
    Status(StatusArgs),
    /// 列出发布
    List(ListArgs),
    /// 推进到下一阶段
    Advance(AdvanceArgs),
    /// 回滚发布
    Rollback(RollbackArgs),
}

#[derive(Parser, Debug)]
pub struct CreateArgs {
    /// 发布名称
    #[arg(long, help = "发布名称")]
    name: String,

    /// 目标（选择器语法）
    #[arg(long, short = 't', help = "目标（选择器语法）", default_value = "all")]
    target: String,

    /// 灰度策略
    #[arg(
        long,
        help = "灰度策略: percentage:10,30,100 或 count:2,5,0 或 groups:canary,prod"
    )]
    strategy: String,

    /// 执行命令（与文件部署二选一）
    #[arg(long, help = "要执行的命令")]
    command: Option<String>,

    /// 命令参数
    #[arg(long, help = "命令参数")]
    args: Vec<String>,

    /// 超时时间（秒）
    #[arg(long, default_value_t = 300, help = "超时时间（秒）")]
    timeout: u32,

    /// 本地文件路径（与命令执行二选一）
    #[arg(long, help = "本地文件路径")]
    file_src: Option<PathBuf>,

    /// 目标路径
    #[arg(long, help = "目标路径（用于文件部署）")]
    file_dest: Option<String>,

    /// 文件权限
    #[arg(long, help = "文件权限（如 0644）")]
    file_mode: Option<String>,

    /// 文件所有者
    #[arg(long, help = "文件所有者（如 user:group）")]
    file_owner: Option<String>,

    /// 自动推进
    #[arg(long, help = "自动推进到下一阶段")]
    auto_advance: bool,

    /// 推进间隔（秒）
    #[arg(long, default_value_t = 60, help = "自动推进间隔（秒）")]
    advance_interval: u32,
}

#[derive(Parser, Debug)]
pub struct StatusArgs {
    /// 发布ID
    rollout_id: String,
}

#[derive(Parser, Debug)]
pub struct ListArgs {
    /// 限制返回数量
    #[arg(long, default_value_t = 20)]
    limit: u32,

    /// 筛选状态
    #[arg(long, value_delimiter = ',')]
    states: Option<Vec<String>>,
}

#[derive(Parser, Debug)]
pub struct AdvanceArgs {
    /// 发布ID
    rollout_id: String,
}

#[derive(Parser, Debug)]
pub struct PauseArgs {
    /// 发布ID
    rollout_id: String,
}

#[derive(Parser, Debug)]
pub struct ResumeArgs {
    /// 发布ID
    rollout_id: String,
}

#[derive(Parser, Debug)]
pub struct RollbackArgs {
    /// 发布ID
    rollout_id: String,

    /// 回滚命令
    #[arg(long, help = "回滚命令")]
    rollback_cmd: Option<String>,
}

/// 主入口函数
pub async fn run_rollout(
    mut client: OasisServiceClient<tonic::transport::Channel>,
    args: RolloutArgs,
) -> Result<()> {
    match args.cmd {
        RolloutCmd::Create(create) => run_rollout_create(&mut client, create).await,
        RolloutCmd::Status(status) => run_rollout_status(&mut client, status).await,
        RolloutCmd::List(list) => run_rollout_list(&mut client, list).await,
        RolloutCmd::Advance(advance) => run_rollout_advance(&mut client, advance).await,
        RolloutCmd::Rollback(rollback) => run_rollout_rollback(&mut client, rollback).await,
    }
}

/// 创建灰度发布
async fn run_rollout_create(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: CreateArgs,
) -> Result<()> {
    // 验证参数
    if args.target.is_empty() {
        return Err(anyhow!("必须提供 --target 参数"));
    }

    // 检查是命令还是文件部署
    let (is_command, is_file) = (args.command.is_some(), args.file_src.is_some());
    if !is_command && !is_file {
        return Err(anyhow!("必须提供 --command 或 --file-src 之一"));
    }
    if is_command && is_file {
        return Err(anyhow!("--command 和 --file-src 不能同时使用"));
    }

    print_header(&format!("创建灰度发布: {}", style(&args.name).cyan()));

    // 解析策略
    let strategy = parse_strategy(&args.strategy)?;

    // 根据类型创建任务
    let task_type = if let Some(command) = args.command {
        // 命令执行模式
        print_info(&format!("执行命令: {}", style(&command).yellow()));
        create_command_task_type(command, args.args, args.timeout)
    } else if let Some(file_src) = args.file_src {
        // 文件部署模式
        print_info(&format!(
            "部署文件: {} -> {}",
            style(file_src.display()).cyan(),
            style(args.file_dest.as_ref().unwrap_or(&"未指定".to_string())).cyan()
        ));

        // 先上传文件
        let (uploaded_key, uploaded_revision) = upload_file_for_rollout(client, &file_src).await?;
        create_file_task_type(
            uploaded_key,
            uploaded_revision,
            args.file_dest
                .ok_or_else(|| anyhow!("文件部署必须提供 --file-dest"))?,
            args.target.clone(),
            args.file_mode,
            args.file_owner,
        )?
    } else {
        unreachable!()
    };

    let request = CreateRolloutRequest {
        name: args.name,
        target: Some(oasis_core::proto::SelectorExpression {
            expression: args.target,
        }),
        strategy: Some(strategy),
        task_type: Some(task_type),
        auto_advance: args.auto_advance,
        advance_interval_seconds: args.advance_interval,
    };

    // 发送请求
    match grpc_retry!(client, create_rollout(request.clone())).await {
        Ok(response) => {
            let resp = response.into_inner();
            if resp.success {
                let rollout_id_str = resp
                    .rollout_id
                    .as_ref()
                    .map(|id| id.value.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                print_info(&format!(
                    "灰度发布创建成功: {}",
                    style(rollout_id_str.clone()).green()
                ));
                print_next_step(
                    format!(
                        "使用 'oasis-cli rollout advance {}' 开始第一阶段",
                        rollout_id_str
                    )
                    .as_str(),
                );
            } else {
                print_warning(&format!("创建失败: {}", resp.message));
            }
            Ok(())
        }
        Err(e) => Err(anyhow!("创建灰度发布失败: {}", format_grpc_error(&e))),
    }
}

/// 查看发布状态
async fn run_rollout_status(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: StatusArgs,
) -> Result<()> {
    let request = GetRolloutStatusRequest {
        rollout_id: Some(RolloutId {
            value: args.rollout_id.clone(),
        }),
    };

    match grpc_retry!(client, get_rollout_status(request.clone())).await {
        Ok(response) => {
            let resp = response.into_inner();
            if let Some(status) = resp.status {
                display_rollout_status(&status);
            } else {
                print_warning("未找到发布状态");
            }
            Ok(())
        }
        Err(e) => Err(anyhow!("获取发布状态失败: {}", format_grpc_error(&e))),
    }
}

/// 列出发布
async fn run_rollout_list(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: ListArgs,
) -> Result<()> {
    let states = args.states.map(|states| {
        states
            .into_iter()
            .filter_map(|s| parse_rollout_state(&s))
            .map(|s| s as i32)
            .collect()
    });

    let request = ListRolloutsRequest {
        limit: args.limit,
        states: states.unwrap_or_default(),
    };

    match grpc_retry!(client, list_rollouts(request.clone())).await {
        Ok(response) => {
            let resp = response.into_inner();
            display_rollouts_table(&resp.rollouts);
            Ok(())
        }
        Err(e) => Err(anyhow!("获取发布列表失败: {}", format_grpc_error(&e))),
    }
}

/// 推进发布
async fn run_rollout_advance(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: AdvanceArgs,
) -> Result<()> {
    let request = AdvanceRolloutRequest {
        rollout_id: Some(RolloutId {
            value: args.rollout_id.clone(),
        }),
    };

    match grpc_retry!(client, advance_rollout(request.clone())).await {
        Ok(response) => {
            let resp = response.into_inner();
            if resp.success {
                print_info(&format!("{}", resp.message));
            } else {
                print_warning(&format!("推进失败: {}", resp.message));
            }
            Ok(())
        }
        Err(e) => Err(anyhow!("推进发布失败: {}", format_grpc_error(&e))),
    }
}

/// 回滚发布
async fn run_rollout_rollback(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: RollbackArgs,
) -> Result<()> {
    use oasis_core::proto::*;

    print_header(&format!("回滚灰度发布: {}", style(&args.rollout_id).cyan()));

    // 确认是否继续回滚
    if !confirm_action(&format!("确定要回滚发布 {} 吗?", args.rollout_id), true) {
        print_info("操作已取消");
        return Ok(());
    }

    let request = RollbackRolloutRequest {
        rollout_id: Some(RolloutId {
            value: args.rollout_id.clone(),
        }),
        rollback_command: args.rollback_cmd.clone(),
    };

    print_info("正在提交回滚...");
    let resp = grpc_retry!(client, rollback_rollout(request.clone()))
        .await
        .map_err(|e| anyhow!("回滚发布失败: {}", format_grpc_error(&e)))?
        .into_inner();

    if resp.success {
        print_info(&resp.message);
    } else {
        print_warning(&format!("回滚失败: {}", resp.message));
    }

    Ok(())
}

/// 解析策略字符串
fn parse_strategy(strategy_str: &str) -> Result<oasis_core::proto::RolloutStrategyMsg> {
    use oasis_core::proto::*;

    let re_pct = regex::Regex::new(r"(?i)^percentage:(\d+(?:,\d+)*)$").unwrap();
    let re_cnt = regex::Regex::new(r"(?i)^count:(\d+(?:,\d+)*)$").unwrap();

    if let Some(caps) = re_pct.captures(strategy_str.trim()) {
        let list = caps.get(1).unwrap().as_str();
        let stages: Result<Vec<u32>, _> =
            list.split(',').map(|s| s.trim().parse::<u32>()).collect();

        match stages {
            Ok(stages) => {
                if stages.iter().any(|&p| p == 0 || p > 100) {
                    return Err(anyhow!("百分比必须在1-100之间"));
                }
                Ok(RolloutStrategyMsg {
                    strategy: Some(rollout_strategy_msg::Strategy::Percentage(
                        PercentageStrategy { stages },
                    )),
                })
            }
            Err(_) => Err(anyhow!("无效的百分比格式")),
        }
    } else if let Some(caps) = re_cnt.captures(strategy_str.trim()) {
        let list = caps.get(1).unwrap().as_str();
        let stages: Result<Vec<u32>, _> =
            list.split(',').map(|s| s.trim().parse::<u32>()).collect();

        match stages {
            Ok(stages) => Ok(RolloutStrategyMsg {
                strategy: Some(rollout_strategy_msg::Strategy::Count(CountStrategy {
                    stages,
                })),
            }),
            Err(_) => Err(anyhow!("无效的计数格式")),
        }
    } else {
        Err(anyhow!(
            "策略格式无效，支持: percentage:10,30,100 或 count:2,5,0 或 groups:canary,prod"
        ))
    }
}

/// 创建命令任务类型
fn create_command_task_type(
    command: String,
    args: Vec<String>,
    timeout: u32,
) -> oasis_core::proto::RolloutTaskTypeMsg {
    use oasis_core::proto::*;

    RolloutTaskTypeMsg {
        task_type: Some(rollout_task_type_msg::TaskType::Command(CommandTask {
            command,
            args,
            timeout_seconds: timeout,
        })),
    }
}

/// 创建文件任务类型
fn create_file_task_type(
    source_path: String,
    revision: u64,
    dest_path: String,
    target: String,
    mode: Option<String>,
    owner: Option<String>,
) -> Result<oasis_core::proto::RolloutTaskTypeMsg> {
    use oasis_core::proto::*;

    let config = FileConfigMsg {
        source_path: source_path,
        destination_path: dest_path,
        revision: revision,
        owner: owner.unwrap_or_default(),
        mode: mode.unwrap_or("0644".to_string()),
        target: Some(SelectorExpression { expression: target }),
    };

    Ok(RolloutTaskTypeMsg {
        task_type: Some(rollout_task_type_msg::TaskType::FileDeployment(
            FileDeploymentTask {
                config: Some(config),
            },
        )),
    })
}

/// 上传文件用于发布：使用文件服务的分片上传接口，返回在对象存储中的文件键
async fn upload_file_for_rollout(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    file_path: &PathBuf,
) -> Result<(String, u64)> {
    use oasis_core::proto::*;

    // 绝对路径（与 file 命令一致，用于在服务端计算 path_hash）
    let abs_path = std::fs::canonicalize(file_path)?;
    let abs_path_str = abs_path.to_string_lossy().to_string();

    let meta = std::fs::metadata(&abs_path)?;
    let size = meta.len();

    // 1) 开始上传会话
    let spec = FileSpecMsg {
        source_path: abs_path_str.clone(), // 传绝对路径，服务端据此计算 path_hash
        size,
        checksum: String::new(),
        content_type: String::new(),
        created_at: chrono::Utc::now().timestamp(),
    };

    let session = client
        .begin_file_upload(spec)
        .await
        .map_err(|e| anyhow!("BeginFileUpload 失败: {}", e))?
        .into_inner();

    let upload_id = session.upload_id;
    let mut offset: u64 = 0;
    let chunk_size = if session.chunk_size == 0 {
        256 * 1024
    } else {
        session.chunk_size
    } as usize;

    // 2) 分片上传
    let mut file = std::fs::File::open(&abs_path)?;
    loop {
        let mut buf = vec![0u8; chunk_size];
        let read_bytes = std::io::Read::read(&mut file, &mut buf)?;
        if read_bytes == 0 {
            break;
        }
        buf.truncate(read_bytes);

        let chunk = FileChunkMsg {
            upload_id: upload_id.clone(),
            offset,
            data: buf.into(),
        };

        let resp = client
            .upload_file_chunk(chunk)
            .await
            .map_err(|e| anyhow!("UploadFileChunk 失败: {}", e))?
            .into_inner();

        offset += resp.received_bytes;
    }

    // 3) 提交上传
    let commit = CommitFileMsg {
        upload_id: upload_id.clone(),
        verify_checksum: false,
    };

    let result = client
        .commit_file_upload(commit)
        .await
        .map_err(|e| anyhow!("CommitFileUpload 失败: {}", e))?
        .into_inner();

    if !result.success {
        anyhow::bail!("文件提交失败: {}", result.message);
    }

    // 返回用于构造对象键的原始绝对路径（服务端按此路径计算 path_hash）与 revision
    Ok((abs_path_str, result.revision))
}

/// 显示发布状态
fn display_rollout_status(status: &oasis_core::proto::RolloutStatusMsg) {
    if let Some(config) = &status.config {
        print_header(&format!("灰度发布状态: {}", config.name));

        print_info(&format!(
            "发布ID: {}",
            config
                .rollout_id
                .as_ref()
                .map(|id| id.value.as_str())
                .unwrap_or("unknown")
        ));
        print_info(&format!(
            "状态: {}",
            create_colored_status_text(status.state())
        ));

        print_info(&format!(
            "当前阶段: {}/{}",
            status.current_stage_idx,
            status.stages.len()
        ));

        // 显示错误信息（如果有）
        if let Some(err) = &status.error_message {
            if !err.is_empty() {
                print_warning(&format!("错误信息: {}", err));
            }
        }

        // 显示配置详情
        print_info(&format!(
            "当前操作: {}",
            status.current_action.clone().unwrap_or("无".to_string())
        ));

        // 显示阶段详情
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        table.set_header(vec![
            Cell::new("阶段").add_attribute(Attribute::Bold),
            Cell::new("名称").add_attribute(Attribute::Bold),
            Cell::new("节点数").add_attribute(Attribute::Bold),
            Cell::new("完成/失败").add_attribute(Attribute::Bold),
        ]);

        let mut stage_index = 0;
        for stage in &status.stages {
            table.add_row(vec![
                Cell::new((stage_index + 1).to_string()),
                Cell::new(&stage.stage_name),
                Cell::new(stage.target_agents.len().to_string()),
                Cell::new(format!("{}/{}", stage.completed_count, stage.failed_count)),
            ]);
            stage_index += 1;
        }

        println!("{}", table);

        // 显示失败任务详情
        display_failed_executions(&status.stages);
    }
}

/// 显示失败任务详情
fn display_failed_executions(stages: &[oasis_core::proto::RolloutStageStatusMsg]) {
    let mut has_failures = false;

    let mut stage_index = 0;
    for stage in stages {
        if !stage.failed_executions.is_empty() {
            if !has_failures {
                print_header("失败任务详情");
                has_failures = true;
            }

            print_info(&format!(
                "阶段 {} ({})",
                console::style((stage_index + 1).to_string()).bold(),
                console::style(&stage.stage_name).bold()
            ));

            for execution in &stage.failed_executions {
                let task_id = execution
                    .task_id
                    .as_ref()
                    .map(|id| &id.value[..8]) // 只显示前8位
                    .unwrap_or("unknown");

                let agent_id = execution
                    .agent_id
                    .as_ref()
                    .map(|id| id.value.as_str())
                    .unwrap_or("unknown");

                log_detail("Task", &console::style(task_id).bold().to_string());
                log_detail("Agent", &console::style(agent_id).bold().to_string());

                if !execution.stdout.is_empty() {
                    let out = if let Some(rest) = execution.stdout.strip_prefix("base64:") {
                        base64::engine::general_purpose::STANDARD
                            .decode(rest)
                            .map(|b: Vec<u8>| String::from_utf8_lossy(&b).to_string())
                            .unwrap_or(execution.stdout.clone())
                    } else {
                        execution.stdout.clone()
                    };
                    log_detail("输出", &out);
                }

                if !execution.stderr.is_empty() {
                    let err = if let Some(rest) = execution.stderr.strip_prefix("base64:") {
                        base64::engine::general_purpose::STANDARD
                            .decode(rest)
                            .map(|b: Vec<u8>| String::from_utf8_lossy(&b).to_string())
                            .unwrap_or(execution.stderr.clone())
                    } else {
                        execution.stderr.clone()
                    };
                    print_warning(&format!("错误: {}", err));
                }

                if let Some(exit_code) = execution.exit_code {
                    log_detail("退出码", &exit_code.to_string());
                }
            }
        }
        stage_index += 1;
    }
}

/// 显示发布列表表格
fn display_rollouts_table(rollouts: &[oasis_core::proto::RolloutStatusMsg]) {
    if rollouts.is_empty() {
        print_info("没有找到灰度发布");
        return;
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);

    table.set_header(vec![
        Cell::new("发布ID").add_attribute(Attribute::Bold),
        Cell::new("名称").add_attribute(Attribute::Bold),
        Cell::new("状态").add_attribute(Attribute::Bold),
        Cell::new("进度").add_attribute(Attribute::Bold),
        Cell::new("创建时间").add_attribute(Attribute::Bold),
    ]);

    for rollout in rollouts {
        if let Some(config) = &rollout.config {
            let rollout_id = config
                .rollout_id
                .as_ref()
                .map(|id| id.value.clone())
                .unwrap_or_else(|| "unknown".to_string());

            let progress = format!("{}/{}", rollout.current_stage_idx, rollout.stages.len());
            let created_at = format_timestamp(config.created_at);

            // 为状态添加颜色
            let state_cell = create_colored_state_cell(rollout.state());

            table.add_row(vec![
                Cell::new(rollout_id),
                Cell::new(truncate_string(&config.name, 30)),
                state_cell,
                Cell::new(progress),
                Cell::new(created_at),
            ]);
        }
    }

    println!("{}", table);
}

/// 解析状态字符串
fn parse_rollout_state(state_str: &str) -> Option<RolloutStateEnum> {
    match state_str.to_lowercase().as_str() {
        "created" => Some(RolloutStateEnum::RolloutCreated),
        "running" => Some(RolloutStateEnum::RolloutRunning),
        "completed" => Some(RolloutStateEnum::RolloutCompleted),
        "failed" => Some(RolloutStateEnum::RolloutFailed),
        "rollingback" => Some(RolloutStateEnum::RolloutRollingback),
        "rollbackfailed" => Some(RolloutStateEnum::RolloutRollbackfailed),
        _ => None,
    }
}

/// 状态中文映射 - 支持所有新状态
fn rollout_state_to_cn(state: RolloutStateEnum) -> &'static str {
    match state {
        RolloutStateEnum::RolloutCreated => "已创建",
        RolloutStateEnum::RolloutRunning => "执行中",
        RolloutStateEnum::RolloutCompleted => "已完成",
        RolloutStateEnum::RolloutFailed => "失败",
        RolloutStateEnum::RolloutRollingback => "正在回滚",
        RolloutStateEnum::RolloutRollbackfailed => "回滚失败",
        RolloutStateEnum::RolloutRolledback => "已回滚",
    }
}

/// 为 rollout 状态创建带颜色的 Cell - 支持所有新状态
fn create_colored_state_cell(state: RolloutStateEnum) -> Cell {
    let state_cn = rollout_state_to_cn(state);
    let cell = Cell::new(state_cn);

    match state {
        RolloutStateEnum::RolloutCreated => cell.fg(Color::Yellow),
        RolloutStateEnum::RolloutRunning => cell.fg(Color::Blue),
        RolloutStateEnum::RolloutCompleted => cell.fg(Color::Green).add_attribute(Attribute::Bold),
        RolloutStateEnum::RolloutFailed => cell.fg(Color::Red).add_attribute(Attribute::Bold),
        RolloutStateEnum::RolloutRollingback => cell.fg(Color::Magenta),
        RolloutStateEnum::RolloutRollbackfailed => {
            cell.fg(Color::Red).add_attribute(Attribute::Bold)
        }
        RolloutStateEnum::RolloutRolledback => cell.fg(Color::Green),
    }
}

fn create_colored_status_text(state: RolloutStateEnum) -> console::StyledObject<&'static str> {
    let state_cn = rollout_state_to_cn(state);

    match state {
        RolloutStateEnum::RolloutCreated => console::style(state_cn).yellow(),
        RolloutStateEnum::RolloutRunning => console::style(state_cn).blue(),
        RolloutStateEnum::RolloutCompleted => console::style(state_cn).green().bold(),
        RolloutStateEnum::RolloutFailed => console::style(state_cn).red().bold(),
        RolloutStateEnum::RolloutRollingback => console::style(state_cn).magenta(),
        RolloutStateEnum::RolloutRollbackfailed => console::style(state_cn).red().bold(),
        RolloutStateEnum::RolloutRolledback => console::style(state_cn).green(),
    }
}

/// 截断字符串
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

/// 格式化时间戳
fn format_timestamp(timestamp: i64) -> String {
    crate::time::format_local_ts(timestamp)
}
