use anyhow::Result;
use clap::Parser;

mod application;
mod infrastructure;
mod interface;

use crate::infrastructure::bootstrap::Bootstrap;

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // 使用配置策略启动应用
    if let Err(e) = Bootstrap::start(args.config.as_deref()).await {
        tracing::error!("Failed to start OASIS Server: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
