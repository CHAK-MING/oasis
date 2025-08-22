use anyhow::{anyhow, Result};
use clap::Parser;
use oasis_core::proto::{
    oasis_service_client::OasisServiceClient, task_target_msg, AgentIdList, ExecuteTaskRequest,
    TaskTargetMsg,
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
  oasis-cli exec --selector 'labels["environment"] == "prod" && "frontend" in groups' -- /usr/bin/uptime
  
  # Run ps aux on all web servers
  oasis-cli exec --selector 'labels["role"] == "web"' -- /usr/bin/ps aux
  
  # Execute with environment variables
  oasis-cli exec --selector 'agent_id == "agent-1"' --env DEBUG=true --env LOG_LEVEL=info -- /usr/bin/echo $DEBUG
  
  # Execute on all agents (use with caution)
  oasis-cli exec --selector 'true' -- /usr/bin/uptime
  
  # Execute on specific agent IDs
  oasis-cli exec --targets agent-1 agent-2 agent-3 -- /usr/bin/uptime"#
)]
pub struct ExecArgs {
    /// CEL selector expression to target specific agents
    ///
    /// The selector uses CEL (Common Expression Language) syntax to match agents based on:
    /// - Agent ID: agent_id == "agent-1"
    /// - Labels: labels["role"] == "web"
    /// - Facts: facts.os_name == "Ubuntu"
    /// - Complex expressions: labels["environment"] == "prod" && "frontend" in groups
    #[arg(long, help = "CEL selector expression to target specific agents", conflicts_with = "targets")]
    pub selector: Option<String>,

    /// Agent IDs to target directly
    ///
    /// Specify a list of agent IDs to target directly, bypassing selector resolution.
    /// This is useful when you know exactly which agents to target.
    #[arg(long = "target", help = "A list of agent IDs to target directly", conflicts_with = "selector")]
    pub targets: Vec<String>,

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
    if args.selector.is_none() && args.targets.is_empty() {
        return Err(anyhow!("必须提供 --selector 或 --targets 参数之一。"));
    }
    if args.command.is_empty() {
        return Err(anyhow!("必须在 -- 后提供要执行的命令。"));
    }

    let (cmd, command_args) = args.command.split_first().unwrap();

    let target_msg = if let Some(selector) = args.selector {
        TaskTargetMsg {
            target: Some(task_target_msg::Target::Selector(selector)),
        }
    } else {
        TaskTargetMsg {
            target: Some(task_target_msg::Target::AgentIds(AgentIdList {
                ids: args
                    .targets
                    .into_iter()
                    .map(|id| oasis_core::proto::AgentId { value: id })
                    .collect(),
            })),
        }
    };

    let mut env_map: HashMap<String, String> = HashMap::new();
    for kv in args.env {
        if let Some((k, v)) = kv.split_once('=') {
            env_map.insert(k.to_string(), v.to_string());
        } else {
            return Err(anyhow!("无效的环境变量格式: '{}'。期望格式为 KEY=VALUE", kv));
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
