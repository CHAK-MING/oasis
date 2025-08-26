use anyhow::Result;
use clap::Subcommand;
use console::style;

#[derive(Subcommand, Debug)]
#[command(name = "storage", about = "查看或管理存储容量（简化）")]
pub enum StorageCommands {
    /// 查看存储信息
    Info,
    /// 设置容量（预留接口）
    SetCapacity {
        #[arg(long)]
        size_gb: u32,
    },
}

pub async fn run_storage(cmd: StorageCommands) -> Result<()> {
    match cmd {
        StorageCommands::Info => {
            println!("› {}:", style("存储信息").bold());
            println!("  - {}: {}", style("类型").dim(), "NATS JetStream 对象存储");
            println!("  - {}: {}", style("容量").dim(), "受限于 Docker Volume 大小");
        }
        StorageCommands::SetCapacity { size_gb } => {
            println!(
                "› {}: {} GB ({})",
                style("设置存储容量").bold(),
                style(size_gb).cyan(),
                style("当前为占位实现，功能待开发").yellow()
            );
        }
    }
    Ok(())
}
