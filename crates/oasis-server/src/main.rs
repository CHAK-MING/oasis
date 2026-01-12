use anyhow::Result;
use clap::Parser;

mod application;
mod infrastructure;
mod interface;

use crate::infrastructure::bootstrap::Bootstrap;

#[derive(Parser)]
#[command(name = "oasis-server")]
#[command(about = "Oasis Server 大规模集群节点管理服务器")]
#[command(version)]
struct Args {
    /// 配置文件路径
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,

    /// 实例锁文件路径（可选）
    #[arg(
        long,
        value_name = "LOCK_FILE",
        default_value = "/tmp/oasis-server.lock"
    )]
    lock_file: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let lock_path = std::path::Path::new(&args.lock_file);
    let lock_dir = lock_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("/tmp"));
    std::fs::create_dir_all(lock_dir)?;

    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;

    if let Err(e) = fs2::FileExt::try_lock_exclusive(&file) {
        eprintln!(
            "oasis-server 已在运行（锁文件: {}）。若确定未运行，请删除锁文件后重试。错误: {}",
            lock_path.display(),
            e
        );
        std::process::exit(1);
    }

    // 使用配置策略启动应用
    if let Err(e) = Bootstrap::start(args.config.as_deref()).await {
        tracing::error!("Failed to start OASIS Server: {}", e);
        // 退出前释放锁（文件关闭会自动释放）
        drop(file);
        std::process::exit(1);
    }

    // 正常退出时释放锁
    drop(file);

    Ok(())
}
