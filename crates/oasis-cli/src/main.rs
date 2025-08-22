use anyhow::Result;
use clap::Parser;
use oasis_core::telemetry::{LogConfig, init_tracing_with};
mod client;
mod commands;
mod config;
mod precheck;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = client::Cli::parse();

    // 根据是否提供 --config 参数决定配置加载方式
    let cfg = if let Some(ref config_path) = cli.config {
        config::CliConfig::load_from_path(config_path).await?
    } else {
        config::CliConfig::load_smart().await?
    };

    init_tracing_with(&LogConfig {
        level: cfg.common.telemetry.log_level,
        format: cfg.common.telemetry.log_format,
        no_ansi: cfg.common.telemetry.log_no_ansi,
    });

    client::run(cli).await
}
