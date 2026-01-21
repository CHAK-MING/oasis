use oasis_agent::{agent_manager::AgentManager, cert_bootstrap::CertBootstrap, nats_client::NatsClient};
use oasis_core::{
    config::{NatsConfig, TlsConfig},
    config_strategies::AgentConfigStrategy,
    config_strategy::ConfigStrategy,
    core_types::AgentId,
    error::Result,
    shutdown::GracefulShutdown,
    telemetry::init_tracing_with,
};
use std::collections::HashMap;
use tracing::{error, info};

/// 解析环境变量标签
fn parse_env_labels(env_var: &str) -> HashMap<String, String> {
    std::env::var(env_var)
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|s| {
            s.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .or_else(|| Some((s.to_string(), "true".to_string())))
        })
        .collect()
}

fn parse_env_groups(env_var: &str) -> Vec<String> {
    std::env::var(env_var)
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn build_agent_info(
    labels: HashMap<String, String>,
    groups: Vec<String>,
) -> HashMap<String, String> {
    let mut info = labels;

    // 如果有分组，添加特殊的 __groups 键
    if !groups.is_empty() {
        info.insert("__groups".to_string(), groups.join(","));
    }

    info
}

#[tokio::main]
async fn main() -> Result<()> {
    // 从环境变量读取 Agent ID
    let agent_id = std::env::var("OASIS_AGENT_ID")
        .unwrap_or_else(|_| format!("agent-{}", uuid::Uuid::now_v7()));

    // 读取 Agent 标签配置
    let labels = parse_env_labels("OASIS_AGENT_LABELS");

    // 读取 Agent 分组配置
    let groups = parse_env_groups("OASIS_AGENT_GROUPS");

    let info = build_agent_info(labels, groups.clone());

    // 通过统一的 Agent 配置策略加载（仅环境变量）
    let strategy = AgentConfigStrategy::new();
    let cfg = strategy.load_initial_config().await?;

    // 初始化遥测
    init_tracing_with(&oasis_core::telemetry::LogConfig {
        level: cfg.telemetry.log_level.clone(),
        format: cfg.telemetry.log_format.clone(),
        no_ansi: cfg.telemetry.log_no_ansi,
    });

    info!("Starting Oasis Agent...");
    info!("  Agent ID: {}", agent_id);
    info!("  NATS URL: {}", cfg.nats.url);

    // 读取 bootstrap token（用于首次证书请求）
    let bootstrap_token = std::env::var("OASIS_BOOTSTRAP_TOKEN").ok();

    // 证书引导流程：如果证书不存在且有 bootstrap token，则请求证书
    let cert_bootstrap = CertBootstrap::new(
        AgentId::from(agent_id.clone()),
        &cfg.tls.certs_dir,
        cfg.nats.url.clone(),
        bootstrap_token,
    );

    if cert_bootstrap.bootstrap_if_needed().await? {
        info!("Certificate bootstrap completed, proceeding with TLS connection");
    }

    // 连接到 NATS
    let nats_client = NatsClient::connect_with_oasis_config(
        &NatsConfig {
            url: cfg.nats.url.clone(),
        },
        &TlsConfig {
            certs_dir: cfg.tls.certs_dir.clone(),
            require_tls: cfg.tls.require_tls,
        },
    )
    .await?;
    info!("Connected to NATS successfully");

    // 创建全局关闭信号
    let shutdown = GracefulShutdown::new();

    // 创建并启动 Agent 管理器
    let agent_manager = AgentManager::new(
        AgentId::from(agent_id),
        nats_client,
        info,
        shutdown.child_token(),
    );

    // 启动 Agent
    let agent_handle = tokio::spawn({
        let manager = agent_manager.clone();
        async move {
            if let Err(e) = manager.run().await {
                error!("Agent manager failed: {}", e);
            }
        }
    });

    info!("Agent started successfully");

    // 等待全局关闭信号
    shutdown.wait_for_signal().await;
    info!("Shutdown signal received, stopping agent...");

    // 等待 Agent 优雅关闭
    let _ = agent_handle.await;
    info!("Agent shut down gracefully");

    Ok(())
}
