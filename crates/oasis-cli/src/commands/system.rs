use crate::certificate::CertificateGenerator;
use crate::ui::{print_header, print_status};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, command};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "system",
    about = "管理 Oasis 系统",
    after_help = r#"示例：
  # 初始化 Oasis 系统
  oasis-cli system init

  # 启动 Oasis 服务器
  oasis-cli system start

  # 查看 Oasis 服务器状态
  oasis-cli system status

  # 停止 Oasis 服务器
  oasis-cli system stop

  # 查看 Oasis 服务器日志
  oasis-cli system logs

  # 卸载 Oasis 服务器
  oasis-cli system uninstall
"#
)]
pub struct SystemArgs {
    #[command(subcommand)]
    pub cmd: SystemCmd,
}

#[derive(Subcommand, Debug)]
pub enum SystemCmd {
    /// 初始化 Oasis 系统
    Init(InitArgs),
    /// 安装 Oasis 服务器为 systemd 服务
    Install(InstallArgs),
    /// 启用 Oasis 服务器开机自启
    Enable,
    /// 启动 Oasis 服务器
    Start,
    /// 停止 Oasis 服务器
    Stop,
    /// 重启 Oasis 服务器
    Restart,
    /// 查看 Oasis 服务器状态
    Status,
    /// 查看 Oasis 服务器日志
    Logs(LogsArgs),
    /// 卸载 Oasis 服务器
    Uninstall,
}

#[derive(Parser, Debug)]
pub struct InitArgs {
    /// 输出目录（默认当前目录）
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,

    /// 强制覆盖已存在文件
    #[arg(short, long)]
    force: bool,
}

#[derive(Parser, Debug)]
pub struct InstallArgs {
    #[arg(long)]
    cfg_dir: Option<PathBuf>,
    #[arg(long)]
    server_bin: Option<PathBuf>,
    #[arg(long, default_value = "root")]
    user: String,
    #[arg(long, default_value = "root")]
    group: String,
}

#[derive(Parser, Debug)]
pub struct LogsArgs {
    #[arg(long, short = 'f')]
    follow: bool,
    #[arg(long, short = 'n', default_value_t = 200)]
    lines: u32,
}

pub async fn run_system(args: SystemArgs) -> Result<()> {
    match args.cmd {
        SystemCmd::Init(init) => run_system_init(&init).await,
        SystemCmd::Install(install) => run_system_install(&install).await,
        SystemCmd::Enable => run_system_enable().await,
        SystemCmd::Start => run_system_start().await,
        SystemCmd::Restart => run_system_restart().await,
        SystemCmd::Logs(logs) => run_system_logs(&logs).await,
        SystemCmd::Uninstall => run_system_uninstall().await,
        SystemCmd::Stop => run_system_stop().await,
        SystemCmd::Status => run_system_status().await,
    }
}

async fn run_system_init(args: &InitArgs) -> Result<()> {
    println!("=== 开始初始化 Oasis 系统 ===");

    // 生成证书
    println!("步骤 1: 生成证书...");
    let certs_dir = args.output_dir.join("certs");
    if args.force && certs_dir.exists() {
        std::fs::remove_dir_all(&certs_dir)?;
    }
    CertificateGenerator::generate_base_certificates(&certs_dir).await?;
    println!("✓ 证书生成完成");

    // 写入 docker-compose.yml
    println!("步骤 2: 创建 docker-compose.yml...");
    let docker_compose_path = args.output_dir.join("docker-compose.yml");
    if docker_compose_path.exists() && !args.force {
        anyhow::bail!(
            "{} 已存在。若需覆盖，请使用 --force",
            docker_compose_path.display()
        );
    }
    if docker_compose_path.exists() && args.force {
        let _ = std::fs::remove_file(&docker_compose_path);
    }
    create_docker_compose(&args.output_dir).await?;
    println!("✓ docker-compose.yml 创建完成");

    println!();
    println!("🎉 初始化完成！接下来需要执行的操作:");
    println!("  1. 在项目根目录执行: docker compose up -d");
    println!("  2. 启动服务: oasis-cli system start -d");
    Ok(())
}

async fn run_system_install(args: &InstallArgs) -> Result<()> {
    print_header("安装 Oasis Server 为 systemd 服务");
    let cfg_dir = if let Some(d) = &args.cfg_dir {
        d.clone()
    } else {
        std::env::current_dir()?
    };
    let server_bin = if let Some(b) = &args.server_bin {
        b.clone()
    } else if let Ok(p) = which::which("oasis-server") {
        p
    } else {
        anyhow::bail!("未找到 oasis-server 可执行文件，请通过 --server-bin 指定或加入 PATH")
    };

    // 规范化为绝对路径，避免 systemd 的 bad-setting（ExecStart 必须为绝对路径）
    let cfg_dir_abs = cfg_dir
        .canonicalize()
        .with_context(|| format!("无法解析 cfg_dir 路径: {}", cfg_dir.display()))?;
    let server_bin_abs = server_bin
        .canonicalize()
        .with_context(|| format!("无法解析 server_bin 路径: {}", server_bin.display()))?;

    let unit = format!(
        r#"[Unit]
Description=Oasis Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={wd}
ExecStart={bin} --config oasis.toml
User={user}
Group={group}
Restart=always
RestartSec=3
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
"#,
        wd = cfg_dir_abs.display(),
        bin = server_bin_abs.display(),
        user = args.user,
        group = args.group,
    );

    let unit_path = PathBuf::from("/etc/systemd/system/oasis-server.service");
    std::fs::write(&unit_path, unit)
        .with_context(|| format!("无法写入 {}", unit_path.display()))?;
    run_sysctl(["daemon-reload"]).await?;
    print_status("systemd 单元已安装 (daemon-reload)", true);
    Ok(())
}

async fn run_system_enable() -> Result<()> {
    run_sysctl(["enable", "oasis-server"]).await?;
    print_status("服务已启用开机自启", true);
    Ok(())
}

async fn run_system_start() -> Result<()> {
    run_sysctl(["start", "oasis-server"]).await?;
    print_status("Oasis 服务器启动成功", true);
    Ok(())
}

async fn run_system_stop() -> Result<()> {
    run_sysctl(["stop", "oasis-server"]).await?;
    print_status("Oasis 服务器已停止", true);
    Ok(())
}

async fn run_system_restart() -> Result<()> {
    run_sysctl(["restart", "oasis-server"]).await?;
    print_status("Oasis 服务器已重启", true);
    Ok(())
}

async fn run_system_status() -> Result<()> {
    print_header("oasis-server 状态");
    let status = std::process::Command::new("systemctl")
        .args(["status", "oasis-server", "--no-pager"])
        .status()
        .context("无法执行 systemctl status")?;
    if !status.success() {
        print_status("服务未运行或获取状态失败", false);
    }
    Ok(())
}

async fn run_system_logs(args: &LogsArgs) -> Result<()> {
    let mut cmd = std::process::Command::new("journalctl");
    cmd.args([
        "-u",
        "oasis-server",
        "--no-pager",
        "-n",
        &args.lines.to_string(),
    ]);
    if args.follow {
        cmd.arg("-f");
    }
    let status = cmd.status().context("无法执行 journalctl")?;
    if !status.success() {
        anyhow::bail!("journalctl 返回非零状态码");
    }
    Ok(())
}

async fn run_system_uninstall() -> Result<()> {
    let unit_path = PathBuf::from("/etc/systemd/system/oasis-server.service");
    let _ = std::process::Command::new("systemctl")
        .args(["disable", "oasis-server"])
        .status();
    let _ = std::process::Command::new("systemctl")
        .args(["stop", "oasis-server"])
        .status();
    if unit_path.exists() {
        std::fs::remove_file(&unit_path)
            .with_context(|| format!("无法删除 {}", unit_path.display()))?;
    }
    run_sysctl(["daemon-reload"]).await?;
    print_status("服务已卸载", true);
    Ok(())
}

async fn run_sysctl<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut cmd = std::process::Command::new("systemctl");
    for a in args {
        cmd.arg(a.as_ref());
    }
    let status = cmd.status().context("无法执行 systemctl")?;
    if !status.success() {
        anyhow::bail!("systemctl 返回非零状态码");
    }
    Ok(())
}

async fn create_docker_compose(output_dir: &PathBuf) -> Result<()> {
    let docker_compose_path = output_dir.join("docker-compose.yml");
    let nats_data_dir = output_dir.join("data").join("nats");
    std::fs::create_dir_all(&nats_data_dir)?;

    let content = r#"services:
  oasis-nats:
    image: nats:2.10-alpine
    container_name: oasis-nats
    ports:
      - "4222:4222"
      - "8222:8222"
      - "6222:6222"
    volumes:
      - ./data/nats:/data
      - ./certs:/certs:ro
    command: |
      --store_dir=/data
      --jetstream
      --http_port=8222
      --tls
      --tlscert=/certs/nats-server.pem
      --tlskey=/certs/nats-server-key.pem
      --tlscacert=/certs/nats-ca.pem
      --tlsverify
    restart: unless-stopped
    healthcheck:
      test:
        [
          "CMD",
          "wget",
          "--no-check-certificate",
          "-qO-",
          "http://localhost:8222/healthz",
        ]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s

networks:
  default:
    driver: bridge
"#;

    // 写入 compose 文件
    std::fs::write(&docker_compose_path, content)?;

    Ok(())
}
