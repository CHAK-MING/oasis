mod certificate;
mod client;
mod commands;
mod time;
mod ui;

use anyhow::Result;
use clap::Parser;
use console::style;

use oasis_core::{config_strategies::CliConfigStrategy, config_strategy::ConfigStrategy};

fn main() -> Result<()> {
    // SAFETY: 在 tokio runtime 启动前设置环境变量（单线程安全）
    if std::env::var("CLICOLOR_FORCE").is_err() {
        #[allow(unsafe_code)]
        unsafe { std::env::set_var("CLICOLOR_FORCE", "1") };
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    let cli = client::Cli::parse();

    // 统一从 oasis.toml 加载配置
    let strategy = CliConfigStrategy::new(cli.config.as_deref().map(std::path::PathBuf::from));

    let config = strategy.load_initial_config().await?;

    if let Err(e) = client::run(cli, &config).await {
        eprintln!("{} {:#}", style("错误:").red().bold(), e);
        std::process::exit(1);
    }

    Ok(())
}
