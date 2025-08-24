use anyhow::Result;
use clap::Parser;
use figment::{
    Figment,
    providers::{Format, Serialized, Toml},
};

mod application;
mod bootstrap;
mod config;
mod domain;
mod infrastructure;
mod interface;

use crate::bootstrap::ServerBootstrapper;

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

    // 加载配置：默认值 + TOML 文件（不读取环境变量）
    let mut figment = Figment::new().merge(Serialized::defaults(config::ServerConfig::default()));
    if let Some(path) = args.config.as_deref() {
        figment = figment.merge(Toml::file(path));
    }
    let cfg: config::ServerConfig = figment
        .extract()
        .map_err(|e| anyhow::anyhow!("Configuration error: {}", e))?;

    // 初始化遥测
    oasis_core::telemetry::init_tracing_with(&oasis_core::telemetry::LogConfig {
        level: cfg.common.telemetry.log_level.clone(),
        format: cfg.common.telemetry.log_format.clone(),
        no_ansi: cfg.common.telemetry.log_no_ansi,
    });

    // 创建并启动服务器
    let bootstrapper = ServerBootstrapper::new(cfg);
    let running_server = bootstrapper.start().await?;

    // 等待关闭信号
    running_server.wait_for_shutdown().await
}
