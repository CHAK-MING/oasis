use crate::common::target::TargetSelector;
use anyhow::{anyhow, Result};
use clap::Parser;
use console::style;
use oasis_core::proto::{
    oasis_service_client::OasisServiceClient, task_target_msg, ExecuteTaskRequest,
    GetTaskResultRequest, StreamTaskResultsRequest, TaskId, TaskTargetMsg,
};
use std::collections::HashMap;

/// 通过 CEL 选择器在目标 Agent 上执行命令
#[derive(Parser, Debug)]
#[command(
    name = "exec",
    about = "在匹配到的 Agent 上执行命令（支持 CEL 选择器或逗号分隔的 ID 列表）",
    after_help = r#"示例：
  # 在生产环境的前端服务器上执行 uptime
  oasis-cli exec --target 'labels[\"environment\"] == \"prod\" && \"frontend\" in groups' -- /usr/bin/uptime

  # 在所有 Web 服务器上运行 ps aux
  oasis-cli exec --target 'labels[\"role\"] == \"web\"' -- /usr/bin/ps aux

  # 对指定 agent 传入环境变量
  oasis-cli exec --target agent-1 --env DEBUG=true --env LOG_LEVEL=info -- /usr/bin/echo $DEBUG

  # 等待结果（5 秒）
  oasis-cli exec --target true --wait-ms 5000 -- /bin/echo hi

  # 持续流式查看结果
  oasis-cli exec --target true --stream -- /bin/echo hi"#
)]
pub struct ExecArgs {
    /// 目标（CEL 选择器或逗号分隔的 Agent ID）
    #[arg(long, short = 't', help = "目标（CEL 选择器或逗号分隔的 Agent ID）")]
    pub target: String,

    /// 传递给命令的环境变量，格式 KEY=VALUE，可多次指定
    #[arg(
        long,
        value_name = "KEY=VALUE",
        action = clap::ArgAction::Append,
        help = "环境变量（可重复）",
    )]
    pub env: Vec<String>,

    /// 等待结果的毫秒数（0 表示不等待）
    #[arg(long, default_value_t = 0)]
    pub wait_ms: u64,

    /// 流式显示结果（直到 Ctrl+C）
    #[arg(long, default_value_t = false)]
    pub stream: bool,

    /// 要执行的命令与参数，必须在 -- 之后给出
    #[arg(
        last = true,
        required = true,
        help = "要执行的命令与参数（置于 -- 之后）"
    )]
    pub command: Vec<String>,
}

/// 执行 exec 子命令：向匹配到的节点发布任务，并可选择等待或流式显示结果
pub async fn run_exec(
    mut client: OasisServiceClient<tonic::transport::Channel>,
    args: ExecArgs,
) -> Result<()> {
    if args.target.is_empty() {
        return Err(anyhow!("必须提供 --target 参数。"));
    }
    if args.command.is_empty() {
        return Err(anyhow!("必须在 -- 后提供要执行的命令。"));
    }

    println!(
        "{} `{}`",
        style("在目标上执行命令:").bold(),
        style(args.command.join(" ")).cyan()
    );

    let (cmd, command_args) = args
        .command
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("命令不能为空"))?;

    // 使用智能解析器统一处理目标
    let target_selector = TargetSelector::parse(&args.target)?;
    let target_msg = TaskTargetMsg {
        target: Some(task_target_msg::Target::Selector(
            target_selector.expression().to_string(),
        )),
    };

    let mut env_map: HashMap<String, String> = HashMap::new();
    for kv in args.env {
        if let Some((k, v)) = kv.split_once('=') {
            env_map.insert(k.to_string(), v.to_string());
        } else {
            return Err(anyhow!(
                "无效的环境变量格式: '{}'。期望格式为 KEY=VALUE",
                kv
            ));
        }
    }

    let request = tonic::Request::new(ExecuteTaskRequest {
        command: cmd.to_string(),
        args: command_args.to_vec(),
        timeout_seconds: 300,
        env: env_map,
        target: Some(target_msg),
    });

    let response = client.execute_task(request).await?.into_inner();

    let task_id = if let Some(task_id) = response.task_id {
        println!(
            "  {} {} {}",
            style("✔").green(),
            style("任务已提交, ID:").dim(),
            style(&task_id.value).dim()
        );
        task_id.value
    } else {
        return Err(anyhow!("服务端未返回任务 ID。"));
    };

    if args.stream {
        println!("{}", style("正在执行命令...").bold());
        let mut stream = client
            .stream_task_results(StreamTaskResultsRequest {
                task_id: Some(TaskId {
                    value: task_id.clone(),
                }),
            })
            .await?
            .into_inner();

        let mut result_count = 0;
        while let Some(item) = stream.message().await? {
            result_count += 1;
            let status_str = match item.exit_code {
                0 => style("✔ 成功").green().bold(),
                -1 => style("✖ 失败").red().bold(),
                -2 => style("⏹ 已取消").yellow().bold(),
                -3 => style("⏱ 超时").yellow().bold(),
                _ => style("● 运行中").cyan(),
            };
            let agent_id = item
                .agent_id
                .as_ref()
                .map(|id| id.value.clone())
                .unwrap_or_default();
            println!(
                "\n{} {} {}",
                style("›").dim(),
                style(format!("[{}]", agent_id)).bold(),
                status_str
            );
            println!(
                "  {} {} {} {} {}",
                style("时间:").dim(),
                style(item.timestamp).dim(),
                style("耗时:").dim(),
                style(format!("{}ms", item.duration_ms)).dim(),
                style(format!("退出码: {}", item.exit_code)).dim()
            );

            if !item.stdout.is_empty() {
                println!(
                    "  {}:\n{}",
                    style("标准输出").dim(),
                    style(item.stdout).dim()
                );
            }
            if !item.stderr.is_empty() {
                eprintln!(
                    "  {}:\n{}",
                    style("标准错误").red(),
                    style(item.stderr).red()
                );
            }
        }

        if result_count > 0 {
            println!("\n{} {}", style("✔").green(), style("命令执行完成").green());
        }
        return Ok(());
    }

    if args.wait_ms > 0 {
        println!("{}", style("--- 等待任务结果 ---").bold().dim());
        let resp = client
            .get_task_result(GetTaskResultRequest {
                task_id: Some(TaskId {
                    value: task_id.clone(),
                }),
                agent_id: None,
                wait_timeout_ms: args.wait_ms as i64,
            })
            .await?
            .into_inner();
        if !resp.found {
            println!(
                "{}",
                style(format!("等待超时，结果未就绪（任务 ID={}）", task_id)).yellow()
            );
            return Ok(());
        }
        let agent_id = resp.agent_id.as_ref().unwrap().value.clone();
        let status_str = if resp.exit_code == 0 {
            style("成功").green().bold()
        } else {
            style("失败").red().bold()
        };

        println!(
            "[{}] {} (耗时: {}ms | 退出码: {})",
            style(agent_id).cyan(),
            status_str,
            resp.duration_ms,
            resp.exit_code,
        );

        if !resp.stdout.is_empty() {
            println!(
                "  {} {}",
                style("标准输出:").dim(),
                style(&resp.stdout).dim()
            );
        }
        if !resp.stderr.is_empty() {
            println!(
                "  {} {}",
                style("标准错误:").red(),
                style(&resp.stderr).red()
            );
        }
    }

    Ok(())
}
