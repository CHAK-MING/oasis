use crate::commands::agent::{AgentArgs, run_agent};
use crate::commands::exec::ExecArgs;
use crate::commands::exec::run_exec;
use crate::commands::file::{FileArgs, run_file};
use crate::commands::rollout::{RolloutArgs, run_rollout};
use crate::commands::system::{SystemArgs, run_system};
use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use clap_complete::{Shell, generate};
use console::style;
use oasis_core::backoff::network_connect_backoff;
use backon::Retryable;
use oasis_core::proto::oasis_service_client::OasisServiceClient;
use std::io;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

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

更多用法：
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
    /// 生成 Shell 补全脚本
    Completion {
        /// 目标 Shell 类型
        #[arg(value_enum)]
        shell: Shell,
    },
}

pub async fn run(cli: Cli, config: &oasis_core::config::OasisConfig) -> Result<()> {
    match cli.command {
        Commands::System(args) => run_system(args).await?,
        Commands::Completion { shell } => {
            let mut cmd = Cli::command();
            generate(shell, &mut cmd, "oasis-cli", &mut io::stdout());
        }
        _ => {
            let client = create_grpc_client(config)
                .await
                .context("创建 gRPC 客户端失败")?;

            match cli.command {
                Commands::Exec(args) => run_exec(client, args).await?,
                Commands::File(args) => run_file(client, args).await?,
                Commands::Agent(args) => run_agent(client, args).await?,
                Commands::Rollout(args) => run_rollout(client, args).await?,
                Commands::System(_) | Commands::Completion { .. } => {
                    unreachable!()
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn create_grpc_client(
    config: &oasis_core::config::OasisConfig,
) -> Result<OasisServiceClient<Channel>> {
    let server_url = config.build_grpc_url()?;

    println!("› 正在连接 Oasis 服务器: {}", style(&server_url).cyan());

    // 解析 host 与连接/校验策略
    let original_host = extract_host(&server_url).unwrap_or_default();
    // 如果是 IP，则优先使用本地开发证书常见的 DNS 名称作为 SNI（例如 localhost）
    // 如果是 localhost，则强制走 IPv4 以避免 ::1 未监听导致的超时
    let sni_host = if original_host.parse::<std::net::IpAddr>().is_ok() {
        "localhost".to_string()
    } else {
        original_host.clone()
    };
    let connect_url = if original_host == "localhost" {
        // 将连接地址中的 localhost 替换为 127.0.0.1，确保使用 IPv4 连接
        server_url.replacen("localhost", "127.0.0.1", 1)
    } else {
        server_url.to_string()
    };
    // 构建 endpoint（连接地址可能与校验域名不同）
    let mut endpoint = Endpoint::from_shared(connect_url).context("无效的服务器地址")?;

    // 只支持 HTTPS
    let mut tls = tonic::transport::ClientTlsConfig::new();

    // 设定 SNI 域名（优先使用 DNS 名称）
    if !sni_host.is_empty() {
        tls = tls.domain_name(sni_host);
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
        .connect_timeout(Duration::from_secs(15))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(60))
        .keep_alive_while_idle(true);

    let channel = endpoint.connect().await.context("连接到服务器失败")?;

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

/// 判断 gRPC 错误是否为“瞬态错误”，适合进行自动重试
fn is_transient_status(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded => true,
        Code::Unknown => {
            // 对部分常见传输类错误进行放行（尽量保守）
            let msg = status.message().to_ascii_lowercase();
            msg.contains("connect")
                || msg.contains("connection")
                || msg.contains("transport")
                || msg.contains("protocol error")
                || msg.contains("goaway")
        }
        _ => false,
    }
}

// 统一 gRPC 错误格式
pub(crate) fn format_grpc_error(e: &tonic::Status) -> String {
    use tonic::Code;
    let code = e.code();
    let raw = e.message();
    let lower = raw.to_ascii_lowercase();

    let hint = match code {
        Code::Ok => "成功",
        Code::Unavailable => "服务器不可用或网络中断",
        Code::DeadlineExceeded => "请求超时",
        Code::Unauthenticated => "认证失败（证书或凭据无效）",
        Code::PermissionDenied => "权限不足",
        Code::InvalidArgument => "请求参数无效",
        Code::NotFound => "资源未找到",
        Code::AlreadyExists => "资源已存在",
        Code::ResourceExhausted => "资源耗尽（可能被限流/配额不足）",
        Code::FailedPrecondition => "前置条件不满足",
        Code::Aborted => "操作被中止（可能存在并发冲突）",
        Code::OutOfRange => "超出允许范围",
        Code::Unimplemented => "功能未实现",
        Code::Internal => "服务器内部错误",
        Code::DataLoss => "数据丢失",
        Code::Cancelled => "请求已取消",
        Code::Unknown => {
            if lower.contains("tls") || lower.contains("handshake") || lower.contains("certificate")
            {
                "TLS 握手失败（请检查证书与域名/SNI）"
            } else if lower.contains("dns") || lower.contains("name resolution") {
                "DNS 解析失败（请检查服务器地址）"
            } else if lower.contains("connect") || lower.contains("connection") {
                "无法连接服务器（请检查网络或服务器监听端口）"
            } else if lower.contains("protocol")
                || lower.contains("goaway")
                || lower.contains("http2")
            {
                "HTTP/2 传输错误（网络抖动或服务器重启）"
            } else {
                "未知错误"
            }
        }
    };

    format!("{}: {}", hint, raw)
}

pub async fn call_with_retry<F, Fut, T>(op: F) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Status>>,
{
    let classifier = std::sync::Arc::new(|e: &Status| is_transient_status(e));

    // 使用重试机制处理瞬态错误
    (op).retry(&network_connect_backoff().build()).when(|e| classifier(e)).await
}

#[macro_export]
macro_rules! grpc_retry {
    ($client:expr, $method:ident($req:expr)) => {{
        // 显式限定路径，避免 #[macro_export] 导致的路径变化
        $crate::client::call_with_retry(|| {
            let req = $req;
            let client = $client.clone();
            async move {
                let mut client = client;
                client.$method(req).await
            }
        })
    }};
}
