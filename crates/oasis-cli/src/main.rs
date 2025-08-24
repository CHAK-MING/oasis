use anyhow::Result;
use clap::Parser;
use figment::{
    Figment,
    providers::{Format, Serialized, Toml},
};
use oasis_core::telemetry::{LogConfig, init_tracing_with};
mod client;
mod commands;
mod common;
mod config;
mod precheck;
mod tls;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = client::Cli::parse();

    // 默认值 + TOML 文件
    let mut figment = Figment::new().merge(Serialized::defaults(config::CliConfig::default()));
    if let Some(ref config_path) = cli.config {
        figment = figment.merge(Toml::file(config_path));
    }
    let cfg: config::CliConfig = figment.extract()?;

    init_tracing_with(&LogConfig {
        level: cfg.common.telemetry.log_level,
        format: cfg.common.telemetry.log_format,
        no_ansi: cfg.common.telemetry.log_no_ansi,
    });

    client::run(cli).await
}
