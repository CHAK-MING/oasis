use anyhow::Result;
use clap::Parser;
use oasis_core::proto::{
    ExecuteTaskRequest, ResolveSelectorRequest, oasis_service_client::OasisServiceClient,
};
use oasis_core::selector::CelSelector;
use oasis_core::target_spec_agent;

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
  oasis-cli exec --selector 'true' -- /usr/bin/uptime"#
)]
pub struct ExecArgs {
    /// CEL selector expression to target specific agents
    ///
    /// The selector uses CEL (Common Expression Language) syntax to match agents based on:
    /// - Agent ID: agent_id == "agent-1"
    /// - Labels: labels["role"] == "web"
    /// - Facts: facts.os_name == "Ubuntu"
    /// - Complex expressions: labels["environment"] == "prod" && "frontend" in groups
    #[arg(long, help = "CEL selector expression to target specific agents")]
    pub selector: String,

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
    let ExecArgs {
        mut command,
        selector,
        env,
    } = args;
    let cmd = command.remove(0);
    let mut targets = Vec::new();

    // CLI 侧：选择器语法验证（本地即时反馈，无需网络调用）
    let _selector =
        CelSelector::new(selector.clone()).map_err(|e| anyhow::anyhow!("选择器语法错误: {}", e))?;

    // 服务端：解析选择器并匹配节点（需要依赖实时节点信息）
    tracing::info!(selector = %selector, "Resolving CEL selector");

    let response = client
        .resolve_selector(ResolveSelectorRequest {
            selector_expression: selector.clone(),
        })
        .await?;
    let result = response.into_inner();

    if !result.error_message.is_empty() {
        anyhow::bail!("节点匹配失败: {}", result.error_message);
    }

    tracing::info!(
        selector = %selector,
        total_nodes = result.total_nodes,
        matched_nodes = result.matched_nodes,
        "CEL selector resolved successfully"
    );

    for agent_id in result.agent_ids {
        targets.push(target_spec_agent(&agent_id));
    }

    // 若选择器无任何匹配节点，则拒绝提交，避免误投到默认队列
    if targets.is_empty() {
        anyhow::bail!("No nodes matched selector: {}", selector);
    }

    // 解析环境变量 KEY=VALUE 列表
    let mut env_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for kv in env {
        if let Some((k, v)) = kv.split_once('=') {
            env_map.insert(k.to_string(), v.to_string());
        } else {
            anyhow::bail!("Invalid env format: '{}'. Expected KEY=VALUE", kv);
        }
    }

    let response = client
        .execute_task(ExecuteTaskRequest {
            targets: targets.clone(),
            command: cmd,
            args: command,
            timeout_seconds: 300,
            env: env_map,
        })
        .await?;
    let task_id = &response.get_ref().task_id;

    println!("Task submitted: {}", task_id);
    tracing::info!(
        task_id = %task_id,
        targets = ?targets,
        "task submitted with routing targets"
    );

    Ok(())
}
