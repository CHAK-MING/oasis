use anyhow::Result;
use clap::Parser;

mod application;
mod bootstrap;
mod domain;
mod infrastructure;
mod interface;

use crate::bootstrap::ServerBootstrapper;
use oasis_core::config::OasisConfig;

#[derive(Parser)]
#[command(name = "oasis-server")]
#[command(about = "Oasis Server - DDD-based cluster management server")]
#[command(version)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Load configuration from the single oasis.toml file.
    let cfg: OasisConfig = OasisConfig::load_config(args.config.as_deref())?;

    // 初始化遥测
    oasis_core::telemetry::init_tracing_with(&oasis_core::telemetry::LogConfig {
        level: cfg.telemetry.log_level.clone(),
        format: "text".to_string(), // Hardcode format
        no_ansi: false,             // Hardcode no_ansi
    });

    if let Some(path) = args.config.as_deref() {
        tracing::info!("Loaded config file: {}", path);
    } else {
        tracing::info!("Config: using default search (current dir)");
    }
    tracing::info!("Effective NATS URL: {}", cfg.nats.url);
    tracing::info!("Effective gRPC listen: {}", cfg.listen_addr);

    // 创建并启动服务器
    let bootstrapper = ServerBootstrapper::new(cfg);
    let running_server = bootstrapper.start().await?;

    // 等待关闭信号
    running_server.wait_for_shutdown().await
}
