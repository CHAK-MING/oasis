use crate::certificate::CertificateGenerator;
use crate::ui::{print_header, print_info, print_next_step, print_status, print_warning};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, command};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

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
    #[arg(long, default_value = "/usr/local/bin")]
    install_bin_dir: PathBuf,
    #[arg(long = "no-copy-bin", action = clap::ArgAction::SetFalse, default_value_t = true)]
    copy_bin: bool,
    #[arg(long)]
    force: bool,
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

fn find_oasis_server_bin(explicit: &Option<PathBuf>) -> Result<PathBuf> {
    if let Some(b) = explicit {
        return Ok(b.clone());
    }

    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let sibling = dir.join("oasis-server");
        if sibling.is_file() {
            return Ok(sibling);
        }
    }

    let cwd = std::env::current_dir().context("无法获取当前工作目录")?;
    let candidates = [
        cwd.join("target/release/oasis-server"),
        cwd.join("target/debug/oasis-server"),
    ];
    for p in &candidates {
        if p.is_file() {
            return Ok(p.to_path_buf());
        }
    }

    if let Ok(p) = which::which("oasis-server") {
        return Ok(p);
    }

    anyhow::bail!(
        "未找到 oasis-server 可执行文件。\n\
请通过 --server-bin 指定，或将 oasis-server 加入 PATH。\n\
已尝试：\n\
  - PATH: oasis-server\n\
  - 与当前 oasis-cli 同目录: oasis-server\n\
  - {}\n\
  - {}\n\
建议：在仓库根目录执行 cargo build -p oasis-server --release，然后使用 --server-bin ./target/release/oasis-server",
        candidates[0].display(),
        candidates[1].display(),
    );
}

async fn run_system_init(args: &InitArgs) -> Result<()> {
    print_header("初始化 Oasis 系统");

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
    print_status("初始化完成", true);
    print_warning("如果 oasis-nats 已在运行，生成的新证书需要重启容器后才能生效");
    print_next_step("在项目根目录执行: docker compose up -d");
    print_next_step("重启 NATS: docker compose down && docker compose up -d");
    print_next_step("安装服务: oasis-cli system install");
    print_next_step("启动服务: oasis-cli system start");
    Ok(())
}

async fn run_system_install(args: &InstallArgs) -> Result<()> {
    print_header("安装 Oasis Server 为 systemd 服务");

    // 确保具备 root 权限（写入 /etc/systemd/system 需要）
    let uid = std::process::Command::new("id")
        .arg("-u")
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<u32>()
                .unwrap_or(1)
        })
        .unwrap_or(1);

    if uid != 0 {
        anyhow::bail!("需要 root 权限，请使用 sudo 执行该命令");
    }

    let cfg_dir = args.cfg_dir.clone().unwrap_or(std::env::current_dir()?);
    let server_bin = find_oasis_server_bin(&args.server_bin)?;
    print_info(&format!("配置目录: {}", cfg_dir.display()));
    print_info(&format!("发现二进制: {}", server_bin.display()));

    // 解析为绝对路径，避免 systemd 的 bad-setting（ExecStart 必须为绝对路径）
    let cfg_dir_abs = cfg_dir.canonicalize().context("无法解析配置目录路径")?;
    let server_bin_abs = server_bin
        .canonicalize()
        .context("无法解析 oasis-server 可执行文件路径")?;

    let install_bin = args.install_bin_dir.join("oasis-server");
    let exec_bin = if args.copy_bin {
        std::fs::create_dir_all(&args.install_bin_dir)
            .with_context(|| format!("无法创建目录 {}", args.install_bin_dir.display()))?;

        if install_bin.exists() && !args.force {
            anyhow::bail!("{} 已存在。若需覆盖，请使用 --force", install_bin.display());
        }

        if server_bin_abs == install_bin {
            anyhow::bail!(
                "发现二进制路径与安装路径相同：{}。\n\
这会导致将文件复制到自己（可能产生 0 字节文件）。\n\
请使用 --server-bin 指定构建产物（例如 ./target/release/oasis-server），或使用 --no-copy-bin。",
                install_bin.display()
            );
        }

        std::fs::copy(&server_bin_abs, &install_bin).with_context(|| {
            format!(
                "无法复制 {} 到 {}",
                server_bin_abs.display(),
                install_bin.display()
            )
        })?;
        let mut perms = std::fs::metadata(&install_bin)
            .with_context(|| format!("无法读取 {} 元数据", install_bin.display()))?
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&install_bin, perms)
            .with_context(|| format!("无法设置 {} 权限", install_bin.display()))?;
        print_info(&format!("已安装二进制: {}", install_bin.display()));
        install_bin
    } else {
        print_warning(
            "未安装到固定目录（使用原始路径作为 ExecStart）。若后续执行 cargo clean，服务可能失效",
        );
        server_bin_abs
    };

    // 生成 systemd unit
    let unit = format!(
        r#"[Unit]
Description=Oasis Server
Documentation=https://github.com/oasis-org/oasis
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={user}
Group={group}

# 环境与目录
WorkingDirectory={wd}
RuntimeDirectory=oasis
RuntimeDirectoryMode=0755
LogsDirectory=oasis
StateDirectory=oasis

# 执行命令
ExecStart={bin} --config oasis.toml --lock-file /run/oasis/oasis-server.lock

# 资源限制与重启策略
LimitNOFILE=1048576
Restart=always
RestartSec=3s
StartLimitInterval=0

# 日志
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"#,
        wd = cfg_dir_abs.display(),
        bin = exec_bin.display(),
        user = args.user,
        group = args.group,
    );

    let unit_path = PathBuf::from("/etc/systemd/system/oasis-server.service");
    std::fs::write(&unit_path, unit).context("写入 systemd unit 文件失败")?;

    // 重新加载并启用
    run_sysctl(["daemon-reload"]).await?;
    run_sysctl(["enable", "oasis-server"]).await?;

    print_status("systemd 服务已安装并启用", true);
    if args.copy_bin {
        print_next_step("后续如需升级二进制，可重新执行: oasis-cli system install --force");
    }
    print_next_step("启动服务: oasis-cli system start");
    print_next_step("查看日志: oasis-cli system logs -f");
    Ok(())
}

async fn run_system_enable() -> Result<()> {
    run_sysctl(["enable", "oasis-server"]).await?;
    print_status("服务已启用开机自启", true);
    Ok(())
}

async fn run_system_start() -> Result<()> {
    run_sysctl(["start", "oasis-server"]).await?;

    // 启动后等待服务变为 active
    let max_wait_secs = 20;
    let mut waited = 0;
    loop {
        let status = std::process::Command::new("systemctl")
            .args(["is-active", "oasis-server"]) // active|activating|failed|inactive
            .output()
            .context("无法执行 systemctl is-active")?;
        let ok = status.status.success();
        let out = String::from_utf8_lossy(&status.stdout).trim().to_string();

        if ok && out == "active" {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let status2 = std::process::Command::new("systemctl")
                .args(["is-active", "oasis-server"])
                .output()
                .context("无法执行 systemctl is-active")?;
            let ok2 = status2.status.success();
            let out2 = String::from_utf8_lossy(&status2.stdout).trim().to_string();
            if ok2 && out2 == "active" {
                print_status("Oasis 服务器启动成功", true);
                break;
            }
        }

        if waited >= max_wait_secs {
            print_status("Oasis 服务器启动失败或超时", false);
            // 打印最近日志帮助定位
            let _ = std::process::Command::new("journalctl")
                .args(["-u", "oasis-server", "--no-pager", "-n", "80"])
                .status();
            anyhow::bail!("服务未在预期时间内进入 active 状态 (状态: {out})");
        }

        // 等待 1 秒后重试
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        waited += 1;
    }

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
    print_header("卸载 Oasis Server");

    // 1. 停止并禁用服务
    let _ = run_system_stop().await;
    let _ = run_sysctl(["disable", "oasis-server"]).await;

    // 2. 删除 systemd unit
    let unit_path = PathBuf::from("/etc/systemd/system/oasis-server.service");
    if unit_path.exists() {
        std::fs::remove_file(&unit_path)?;
        run_sysctl(["daemon-reload"]).await?;
    } else {
        print_warning("未找到 systemd unit 文件，跳过删除");
    }

    print_status("systemd 服务已卸载", true);
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
    let output = cmd.output().context("无法执行 systemctl")?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stdout.is_empty() && stderr.is_empty() {
            anyhow::bail!("systemctl 返回非零状态码");
        }
        anyhow::bail!(
            "systemctl 返回非零状态码\nstdout: {}\nstderr: {}",
            stdout,
            stderr
        );
    }
    Ok(())
}

async fn create_docker_compose(output_dir: &Path) -> Result<()> {
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
