use crate::common::target::TargetSelector;
use anyhow::{Result, anyhow};
use clap::Parser;
use oasis_core::proto::{
    ExecuteTaskRequest, TaskTargetMsg, oasis_service_client::OasisServiceClient, task_target_msg,
};
use std::collections::HashMap;

/// Execute shell commands on target agents using CEL selectors
///
/// This command allows you to execute shell commands on agents that match the specified CEL selector.
/// Commands are executed securely with proper isolation and can include environment variables.
/// Results are collected and displayed in real-time.
#[derive(Parser, Debug)]
#[command(
    name = "exec",
    about = "Execute shell commands on target agents using CEL selectors",
    after_help = r#"Examples:
  # Execute uptime on production frontend servers
  oasis-cli exec --target 'labels["environment"] == "prod" && "frontend" in groups' -- /usr/bin/uptime
  
  # Run ps aux on all web servers  
  oasis-cli exec --target 'labels["role"] == "web"' -- /usr/bin/ps aux
  
  # Execute with environment variables on specific agent
  oasis-cli exec --target agent-1 --env DEBUG=true --env LOG_LEVEL=info -- /usr/bin/echo $DEBUG
  
  # Execute on all agents (use with caution)
  oasis-cli exec --target true -- /usr/bin/uptime
  
  # Execute on multiple specific agent IDs
  oasis-cli exec --target agent-1,agent-2,agent-3 -- /usr/bin/uptime"#
)]
pub struct ExecArgs {
    /// Target specification (CEL selector or comma-separated agent IDs)
    ///
    /// Smart parsing supports multiple formats:
    /// - Single agent: agent-1 -> agent_id == "agent-1"
    /// - Multiple agents: agent-1,agent-2 -> agent_id in ["agent-1", "agent-2"]
    /// - All agents: true -> true
    /// - CEL expressions: labels["role"] == "web" -> labels["role"] == "web"
    #[arg(
        long,
        short = 't',
        help = "Target specification (CEL selector or agent IDs)"
    )]
    pub target: String,

    /// Environment variables to pass to the command
    ///
    /// Can be specified multiple times. Format: KEY=VALUE
    /// These variables will be available in the command's environment.
    #[arg(
        long,
        value_name = "KEY=VALUE",
        action = clap::ArgAction::Append,
        help = "Environment variables to pass to the command (can be repeated)"
    )]
    pub env: Vec<String>,

    /// Command and arguments to execute
    ///
    /// The command and all its arguments must be specified after --.
    /// The command will be executed in the agent's shell environment.
    #[arg(
        last = true,
        required = true,
        help = "Command and arguments to execute (after --)"
    )]
    pub command: Vec<String>,
}

/// 执行 exec 子命令：向匹配到的节点发布任务
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

    let (cmd, command_args) = args
        .command
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("Command cannot be empty"))?;

    // 使用智能解析器统一处理目标
    let target_selector = TargetSelector::parse(&args.target);
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

    if let Some(task_id) = response.task_id {
        println!("任务提交成功。任务 ID: {}", task_id.value);
    } else {
        return Err(anyhow!("服务端未返回任务 ID。"));
    }

    Ok(())
}
