use crate::commands::agent::{run_agent, AgentCommands};
use crate::commands::exec::run_exec;
use crate::commands::exec::ExecArgs;
use crate::commands::file::{run_file, FileArgs};

use crate::commands::rollout::{run_rollout, RolloutCommand};
use crate::commands::storage::{run_storage, StorageCommands};
use crate::commands::system::{run_system, SystemCommands};
use crate::precheck;
use anyhow::{Context, Result};
use clap::Parser;
use console::style;
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

  # Task results
  oasis-cli task get --id <task_id> --wait-ms 5000
  oasis-cli task watch --id <task_id>

For detailed options:
  oasis-cli exec --help
  oasis-cli file --help
  oasis-cli node --help"#
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
    Agent {
        #[command(subcommand)]
        cmd: AgentCommands,
    },
    Exec(ExecArgs),
    File {
        #[command(subcommand)]
        args: FileArgs,
    },

    Rollout {
        #[command(subcommand)]
        cmd: RolloutCommand,
    },
    Storage {
        #[command(subcommand)]
        cmd: StorageCommands,
    },
    System {
        #[command(subcommand)]
        cmd: SystemCommands,
    },
}

pub async fn run(cli: Cli, config: &oasis_core::config::OasisConfig) -> Result<()> {
    match cli.command {
        Commands::System { cmd } => {
            // System commands don't need gRPC client
            run_system(cmd).await?
        }
        Commands::Agent { cmd } => {
            // Agent commands don't need gRPC client
            run_agent(cmd).await?
        }
        Commands::Storage { cmd } => {
            // Storage commands don't need gRPC client
            run_storage(cmd).await?
        }
        _ => {
            // Create gRPC client with configuration for other commands
            let mut client = create_grpc_client(config)
                .await
                .context("Failed to create gRPC client")?;

            // 在连接成功后立即进行服务器健康检查
            precheck::precheck_server_health(&mut client).await?;

            match cli.command {
                Commands::Exec(args) => run_exec(client, args).await?,
                Commands::File { args } => run_file(client, args).await?,

                Commands::Rollout { cmd } => run_rollout(cmd, client).await?,
                Commands::System { .. } | Commands::Agent { .. } | Commands::Storage { .. } => {
                    unreachable!()
                }
            }
        }
    }
    Ok(())
}

/// Create gRPC client with configuration
async fn create_grpc_client(
    config: &oasis_core::config::OasisConfig,
) -> Result<OasisServiceClient<Channel>> {
    // 使用配置中的服务器地址，强制使用 TLS
    // 将 0.0.0.0 替换为 localhost 以匹配证书的 SAN
    let server_addr = config.listen_addr.replace("0.0.0.0", "localhost");
    let server_url = format!("https://{}", server_addr);

    println!("› 正在连接 Oasis 服务器: {}", style(&server_url).cyan());

    // Parse the endpoint
    let endpoint = Endpoint::from_shared(server_url.to_string()).context("无效的服务器地址")?;

    // Load client certificates for mTLS
    let client_cert = tokio::fs::read(&config.tls.grpc_client_cert_path)
        .await
        .context("读取客户端证书失败")?;
    let client_key = tokio::fs::read(&config.tls.grpc_client_key_path)
        .await
        .context("读取客户端密钥失败")?;
    let ca_cert = tokio::fs::read(&config.tls.grpc_ca_path)
        .await
        .context("读取 CA 证书失败")?;

    // Configure TLS
    let endpoint = endpoint
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .identity(tonic::transport::Identity::from_pem(
                    client_cert,
                    client_key,
                ))
                .ca_certificate(tonic::transport::Certificate::from_pem(ca_cert)),
        )
        .context("配置 TLS 失败")?;

    // Connect with TLS
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("无法连接到服务器 {}", server_url))?;

    Ok(OasisServiceClient::new(channel))
}
