use crate::commands::config::{ConfigArgs, run_config};
use crate::commands::exec::ExecArgs;
use crate::commands::exec::run_exec;
use crate::commands::file::{FileArgs, run_file};
use crate::commands::node::{NodeCommands, run_node};
use crate::commands::rollout::{RolloutCommand, run_rollout};
use crate::commands::task::{TaskCommands, run_task};
use crate::precheck;
use anyhow::Result;
use clap::Parser;
use oasis_core::proto::oasis_service_client::OasisServiceClient;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    long_about = None,
    after_help = r#"Examples:
  # Execute commands on agents
  oasis-cli exec --selector 'agent_id == "agent-1"' -- /usr/bin/uptime
  oasis-cli exec --selector 'labels["environment"] == "prod"' -- /usr/bin/ps aux
  
  # Manage files across agents
  oasis-cli file apply --src ./nginx.conf --selector 'role == "web"' --dest /etc/nginx/nginx.conf
  oasis-cli file clear
  
  # Manage cluster nodes
  oasis-cli node ls --output table
  oasis-cli node ls --selector 'labels["environment"] == "prod"' --verbose
  oasis-cli node facts --selector 'labels["role"] == "db"' --output table
  
  # Distribute configs
  oasis-cli config apply --src ./agent.toml --selector 'labels["role"] == "web"'
  oasis-cli config set agent-1 log.level debug
  oasis-cli config get agent-1 log.level
  oasis-cli config del agent-1 log.level

  # Task results
  oasis-cli task get --id <task_id> --wait-ms 5000
  oasis-cli task watch --id <task_id>

For detailed options:
  oasis-cli exec --help
  oasis-cli file --help
  oasis-cli node --help
  oasis-cli config --help"#
)]
pub struct Cli {
    /// Configuration file path
    #[arg(long, value_name = "CONFIG_FILE")]
    pub config: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(clap::Subcommand, Debug)]
pub enum Commands {
    Exec(ExecArgs),
    File {
        #[command(subcommand)]
        args: FileArgs,
    },
    Node {
        #[command(subcommand)]
        cmd: NodeCommands,
    },
    Rollout {
        #[command(subcommand)]
        cmd: RolloutCommand,
    },
    Config {
        #[command(subcommand)]
        args: ConfigArgs,
    },
    Task {
        #[command(subcommand)]
        cmd: TaskCommands,
    },
}

pub async fn run(cli: Cli) -> Result<()> {
    // 根据是否提供 --config 参数决定配置加载方式
    let cfg = if let Some(config_path) = cli.config {
        crate::config::CliConfig::load_from_path(&config_path).await?
    } else {
        crate::config::CliConfig::load_smart().await?
    };

    let server_url = cfg.cli.server_url;

    let mut client = OasisServiceClient::connect(server_url).await?;

    // 在连接成功后立即进行服务器健康检查
    precheck::precheck_server_health(&mut client).await?;

    match cli.command {
        Commands::Exec(args) => run_exec(client, args).await?,
        Commands::File { args } => run_file(client, args).await?,
        Commands::Node { cmd } => run_node(client, cmd).await?,
        Commands::Rollout { cmd } => run_rollout(cmd, client).await?,
        Commands::Config { args } => run_config(client, args).await?,
        Commands::Task { cmd } => run_task(client, cmd).await?,
    }
    Ok(())
}
