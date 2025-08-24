use crate::commands::config::{ConfigArgs, run_config};
use crate::commands::exec::ExecArgs;
use crate::commands::exec::run_exec;
use crate::commands::file::{FileArgs, run_file};
use crate::commands::node::{NodeCommands, run_node};
use crate::commands::rollout::{RolloutCommand, run_rollout};
use crate::commands::task::{TaskCommands, run_task};
use crate::precheck;
use crate::tls::TlsClientService;
use anyhow::{Context, Result};
use clap::Parser;
use oasis_core::proto::oasis_service_client::OasisServiceClient;
use tonic::transport::{Channel, Endpoint};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    long_about = None,
    after_help = r#"Examples:
  # Execute commands on agents (target supports CEL or comma-separated IDs)
  oasis-cli exec -t 'labels["environment"] == "prod"' -- /usr/bin/ps aux
  oasis-cli exec -t 'agent-1,agent-2' -- /usr/bin/uptime
  oasis-cli exec -t 'true' -- /usr/bin/uptime

  # Manage files across agents
  oasis-cli file apply --src ./nginx.conf --dest /etc/nginx/nginx.conf -t 'labels["role"] == "web"'
  oasis-cli file clear

  # Manage cluster nodes (still support --selector)
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
    /// Configuration file path (handled in main.rs for telemetry)
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
    // Use defaults for CLI config here; main.rs already initialized telemetry.
    let cfg = crate::config::CliConfig::default();

    // Create gRPC client with TLS support
    let mut client = create_grpc_client(&cfg)
        .await
        .context("Failed to create gRPC client")?;

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

/// Create gRPC client with TLS configuration
async fn create_grpc_client(cfg: &crate::config::CliConfig) -> Result<OasisServiceClient<Channel>> {
    let server_url = &cfg.cli.server_url;

    // Parse the endpoint
    let endpoint = Endpoint::from_shared(server_url.clone()).context("Invalid server URL")?;

    // Check if TLS is configured and URL uses HTTPS
    let channel = if server_url.starts_with("https://") {
        if let Some(ref tls_config) = cfg.cli.grpc_tls {
            tracing::info!("Connecting to server with TLS: {}", server_url);

            // Create TLS client service
            let tls_service = TlsClientService::new(tls_config.clone());
            let client_tls_config = tls_service
                .create_client_tls_config()
                .context("Failed to create TLS configuration")?;

            // Connect with TLS
            endpoint
                .tls_config(client_tls_config)
                .context("Failed to configure TLS")?
                .connect()
                .await
                .context("Failed to connect to server with TLS")?
        } else {
            return Err(anyhow::anyhow!(
                "HTTPS URL specified but no TLS configuration found. Please configure [grpc_tls] section."
            ));
        }
    } else if server_url.starts_with("http://") {
        tracing::info!("Connecting to server without TLS: {}", server_url);

        // Connect without TLS
        endpoint
            .connect()
            .await
            .context("Failed to connect to server")?
    } else {
        return Err(anyhow::anyhow!(
            "Invalid server URL scheme. Must start with http:// or https://"
        ));
    };

    Ok(OasisServiceClient::new(channel))
}
