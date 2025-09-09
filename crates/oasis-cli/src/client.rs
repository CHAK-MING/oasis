use crate::commands::agent::{AgentArgs, run_agent};
use crate::commands::exec::ExecArgs;
use crate::commands::exec::run_exec;
use crate::commands::file::{FileArgs, run_file};
use crate::commands::rollout::{RolloutArgs, run_rollout};
use crate::commands::system::{SystemArgs, run_system};
use anyhow::{Context, Result};
use clap::Parser;
use console::style;
use oasis_core::proto::oasis_service_client::OasisServiceClient;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    name = "oasis-cli",
    about = "Oasis CLI 大规模集群节点管理命令行工具",
    after_help = r#"示例：
  # 在节点上执行命令
  oasis-cli exec -t 'labels["environment"] == "prod"' -- /usr/bin/ps aux
  oasis-cli exec -t "agent-1,agent-2" -- /usr/bin/uptime
  oasis-cli exec -t "true" -- /usr/bin/uptime

  # 管理文件
  oasis-cli file apply --src ./nginx.conf --dest /etc/nginx/nginx.conf -t 'labels["role"] == "web"'
  oasis-cli file clear

  # 管理 agent 节点
  oasis-cli agent list --target 'labels["environment"] == "prod"' --verbose
  oasis-cli agent list --target 'labels["role"] == "db"' --verbose

For detailed options:
  oasis-cli exec --help
  oasis-cli file --help
  oasis-cli agent --help"#
)]
pub struct Cli {
    /// 配置文件路径
    #[arg(long, value_name = "CONFIG_FILE")]
    pub config: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(clap::Subcommand, Debug)]
pub enum Commands {
    Agent(AgentArgs),
    Exec(ExecArgs),
    File(FileArgs),
    System(SystemArgs),
    Rollout(RolloutArgs),
}

pub async fn run(cli: Cli, config: &oasis_core::config::OasisConfig) -> Result<()> {
    match cli.command {
        Commands::System(args) => {
            // 系统命令不需要 grpc
            run_system(args).await?
        }
        _ => {
            let client = create_grpc_client(config)
                .await
                .context("Failed to create gRPC client")?;

            match cli.command {
                Commands::Exec(args) => run_exec(client, args).await?,
                Commands::File(args) => run_file(client, args).await?,
                Commands::Agent(args) => run_agent(client, args).await?,
                Commands::Rollout(args) => run_rollout(client, args).await?,
                Commands::System(_) => {
                    unreachable!()
                }
            }
        }
    }
    Ok(())
}

async fn create_grpc_client(
    config: &oasis_core::config::OasisConfig,
) -> Result<OasisServiceClient<Channel>> {
    let server_url = config.build_grpc_url()?;

    println!("› 正在连接 Oasis 服务器: {}", style(&server_url).cyan());

    // 解析 endpoint
    let mut endpoint = Endpoint::from_shared(server_url.to_string()).context("无效的服务器地址")?;

    // 只支持 HTTPS
    let mut tls = tonic::transport::ClientTlsConfig::new();

    // 设定 SNI 域名（若 URL 含域名）
    if let Some(host) = extract_host(&server_url) {
        tls = tls.domain_name(host.to_string());
    }

    // CA
    let ca_cert_path = config.tls.grpc_ca_path();
    if ca_cert_path.exists() {
        let ca_cert = tokio::fs::read(&ca_cert_path).await.with_context(|| {
            let abs_path = ca_cert_path
                .canonicalize()
                .unwrap_or_else(|_| ca_cert_path.clone());
            format!(
                "读取 CA 证书失败 - 路径: {} (绝对路径: {})",
                ca_cert_path.display(),
                abs_path.display()
            )
        })?;
        tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(ca_cert));
    } else {
        return Err(anyhow::anyhow!("未找到 CA 证书"));
    }

    // 客户端证书（开启 mTLS）
    let client_cert_path = config.tls.grpc_client_cert_path();
    let client_key_path = config.tls.grpc_client_key_path();
    if client_cert_path.exists() && client_key_path.exists() {
        let client_cert = tokio::fs::read(&client_cert_path).await.with_context(|| {
            let abs_path = client_cert_path
                .canonicalize()
                .unwrap_or_else(|_| client_cert_path.clone());
            format!(
                "读取客户端证书失败 - 路径: {} (绝对路径: {})",
                client_cert_path.display(),
                abs_path.display()
            )
        })?;
        let client_key = tokio::fs::read(&client_key_path).await.with_context(|| {
            let abs_path = client_key_path
                .canonicalize()
                .unwrap_or_else(|_| client_key_path.clone());
            format!(
                "读取客户端密钥失败 - 路径: {} (绝对路径: {})",
                client_key_path.display(),
                abs_path.display()
            )
        })?;
        tls = tls.identity(tonic::transport::Identity::from_pem(
            client_cert,
            client_key,
        ));
    } else {
        return Err(anyhow::anyhow!("未找到客户端证书或密钥"));
    }

    endpoint = endpoint.tls_config(tls).context("配置 TLS 失败")?;

    // 保持连接 & 超时
    endpoint = endpoint
        .connect_timeout(Duration::from_secs(5))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .timeout(Duration::from_secs(30))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(5))
        .keep_alive_while_idle(true);

    // 使用退避重试连接（更长的窗口）来覆盖服务器重启
    let backoff = oasis_core::backoff::network_connect_backoff();
    let channel = oasis_core::backoff::execute_with_backoff(
        || async { endpoint.connect().await.map_err(|e| anyhow::anyhow!(e)) },
        backoff,
    )
    .await
    .with_context(|| format!("无法连接到服务器 {}", server_url))?;

    Ok(OasisServiceClient::new(channel))
}

/// 从 https URL 中提取主机名
fn extract_host(url: &str) -> Option<String> {
    // 匹配 http/https，捕获主机部分：IPv6 用 [::1]，IPv4/域名用普通形式
    // 例: https://[2001:db8::1]:50051/path -> 捕获 "[2001:db8::1]"
    //     https://example.com:50051/path   -> 捕获 "example.com"
    let re = regex::Regex::new(r"(?i)^https?://(\[[^\]]+\]|[^/:]+)").ok()?;
    let caps = re.captures(url)?;
    let host = caps.get(1)?.as_str();
    // 去掉 IPv6 方括号
    Some(host.trim_matches(&['[', ']'][..]).to_string())
}
