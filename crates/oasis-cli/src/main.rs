use anyhow::{Context, Result};
use clap::Parser;
use console::style;
use oasis_core::config::OasisConfig;
mod client;
mod commands;
mod common;
mod precheck;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = client::Cli::parse();

    // 强制启用 ANSI 颜色
    if std::env::var("CLICOLOR_FORCE").is_err() {
        std::env::set_var("CLICOLOR_FORCE", "1");
    }

    // 统一从 oasis.toml 加载配置
    let cfg = OasisConfig::load_config(cli.config.as_deref())
        .with_context(|| "无法加载 oasis.toml 配置文件，请检查文件是否存在或路径是否正确。")?;

    if let Err(e) = client::run(cli, &cfg).await {
        eprintln!("{} {:#}", style("错误:").red().bold(), e);
        std::process::exit(1);
    }

    Ok(())
}
