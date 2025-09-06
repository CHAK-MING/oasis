use crate::certificate::CertificateGenerator;
use crate::ui::{print_header, print_info, print_status};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, command};
use console::style;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "system",
    about = "管理 Oasis 系统",
    after_help = r#"示例：
  # 初始化 Oasis 系统
  oasis-cli system init --force

  # 启动 Oasis 服务器（后台）
  oasis-cli system start --daemon --log-file ./oasis-server.log

  # 查看 Oasis 服务器状态
  oasis-cli system status

  # 停止 Oasis 服务器
  oasis-cli system stop
"#
)]
pub struct SystemArgs {
    #[command(subcommand)]
    pub cmd: SystemCmd,
}

#[derive(Subcommand, Debug)]
pub enum SystemCmd {
    /// 初始化 Oasis 系统（生成证书、创建配置与 docker-compose）
    Init(InitArgs),
    /// 启动 Oasis 服务器
    Start(StartArgs),
    /// 停止 Oasis 服务器
    Stop,
    /// 查看 Oasis 服务器状态
    Status,
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
pub struct StartArgs {
    /// 以守护进程方式运行
    #[arg(short, long)]
    daemon: bool,

    /// 将服务端日志重定向到指定文件（仅守护模式生效）
    #[arg(long, value_name = "LOG_FILE", default_value = "oasis-server.log")]
    log_file: String,
}

pub async fn run_system(args: SystemArgs) -> Result<()> {
    match args.cmd {
        SystemCmd::Init(init) => run_system_init(&init).await,
        SystemCmd::Start(start) => run_system_start(&start).await,
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
    create_docker_compose(&args.output_dir).await?;
    println!("✓ docker-compose.yml 创建完成");

    // 记录最近一次 init 的目录
    println!("步骤 3: 记录初始化标记...");
    let marker = std::env::current_dir()?.join(".oasis_last_init");
    std::fs::write(&marker, args.output_dir.to_string_lossy().as_bytes())?;
    println!("✓ 初始化标记记录完成");

    println!();
    println!("🎉 初始化完成！接下来需要执行的操作:");
    println!("  1. 在项目根目录执行: docker compose up -d");
    println!("  2. 启动服务: oasis-cli system start -d");
    Ok(())
}

async fn run_system_start(args: &StartArgs) -> Result<()> {
    print_header("正在启动 Oasis 服务器");
    match run_start(args.daemon, &args.log_file).await {
        Ok(started) => {
            if started {
                print_status("Oasis 服务器启动成功", true);
                if args.daemon {
                    let log_path = if args.log_file.starts_with('/') {
                        PathBuf::from(args.log_file.clone())
                    } else {
                        let cfg_dir = find_config_dir()?;
                        cfg_dir.join(args.log_file.clone())
                    };

                    print_info(&format!("后台日志: {}", log_path.display()));
                } else {
                    print_info("当前以前台模式运行，按 Ctrl+C 可停止");
                }
            }
            Ok(())
        }
        Err(e) => {
            print_status(&format!("Oasis 服务器启动失败: {}", e), false);
            Err(e)
        }
    }
}

async fn run_system_stop() -> Result<()> {
    print_header("正在停止 Oasis 服务器");
    match run_stop().await {
        Ok(_) => {
            print_status("Oasis 服务器已停止", true);
            Ok(())
        }
        Err(e) => {
            print_status(&format!("停止服务器失败: {}", e), false);
            Err(e)
        }
    }
}

async fn run_system_status() -> Result<()> {
    let cfg_dir = find_config_dir()?;
    let state = load_server_state(&cfg_dir);
    let running = state
        .as_ref()
        .map(|s| s.pid)
        .map(pid_alive)
        .unwrap_or(false);
    if !running {
        print_status("Oasis 服务器未运行", false);
    } else {
        let state = state.expect("State should be available");
        let pid = state.pid;
        let uptime = chrono::Utc::now().timestamp() - state.started_at;
        let uptime_str = if uptime < 60 {
            format!("{}秒", uptime)
        } else if uptime < 3600 {
            format!("{}分钟", uptime / 60)
        } else {
            format!("{}小时{}分钟", uptime / 3600, (uptime % 3600) / 60)
        };
        print_status(
            &format!(
                "Oasis 服务器正在运行 (PID: {}, 运行时间: {})",
                pid, uptime_str
            ),
            true,
        );
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ServerState {
    pid: u32,
    cfg_dir: String,
    log_file: String,
    certs_fingerprint: String,
    started_at: i64,
}

fn statefile_path(cfg_dir: &std::path::Path) -> std::path::PathBuf {
    cfg_dir.join("oasis-server.state.json")
}

fn load_server_state(cfg_dir: &std::path::Path) -> Option<ServerState> {
    let path = statefile_path(cfg_dir);
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str::<ServerState>(&s).ok())
}

fn save_server_state(cfg_dir: &std::path::Path, state: &ServerState) -> Result<()> {
    let path = statefile_path(cfg_dir);
    let temp_path = path.with_extension("tmp");

    // 先写入临时文件
    std::fs::write(
        &temp_path,
        serde_json::to_string_pretty(state).unwrap_or_else(|_| "{}".to_string()),
    )?;

    // 原子性地重命名临时文件
    std::fs::rename(&temp_path, &path)?;

    Ok(())
}

fn clear_server_state(cfg_dir: &std::path::Path) -> Result<()> {
    let path = statefile_path(cfg_dir);
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

fn pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|st| st.success())
        .unwrap_or(false)
}

fn compute_certs_fingerprint(
    cfg: &oasis_core::config::OasisConfig,
    cfg_dir: &std::path::Path,
) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let paths = [
        &cfg.tls.nats_ca_path(),
        &cfg.tls.nats_client_cert_path(),
        &cfg.tls.nats_client_key_path(),
        &cfg.tls.grpc_ca_path(),
        &cfg.tls.grpc_server_cert_path(),
        &cfg.tls.grpc_server_key_path(),
        &cfg.tls.grpc_client_cert_path(),
        &cfg.tls.grpc_client_key_path(),
    ];
    let mut hasher = DefaultHasher::new();
    for p in paths.iter() {
        let p = p.as_path();
        let resolved = if p.is_absolute() {
            p.to_path_buf()
        } else {
            cfg_dir.join(p)
        };
        let meta_opt = std::fs::metadata(&resolved).ok();
        let modified = meta_opt
            .as_ref()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let len = meta_opt.as_ref().map(|m| m.len()).unwrap_or(0);
        resolved.to_string_lossy().hash(&mut hasher);
        modified.hash(&mut hasher);
        len.hash(&mut hasher);
    }
    format!("{:x}", hasher.finish())
}

fn find_config_dir() -> Result<PathBuf> {
    // 1. 检查环境变量
    if let Ok(dir) = std::env::var("OASIS_CONFIG_DIR") {
        let path = PathBuf::from(dir);
        if path.join("oasis.toml").exists() {
            return Ok(path);
        }
    }

    // 2. 检查标记文件
    let cwd = std::env::current_dir()?;
    let marker = cwd.join(".oasis_last_init");
    if let Ok(path_str) = std::fs::read_to_string(&marker) {
        let path = PathBuf::from(path_str.trim());
        if path.join("oasis.toml").exists() {
            return Ok(path);
        }
    }

    // 3. 检查当前目录
    if cwd.join("oasis.toml").exists() {
        return Ok(cwd);
    }

    // 4. 如果都没找到，返回当前目录（用于 init）
    Ok(cwd)
}

async fn run_start(daemon: bool, log_file: &str) -> Result<bool> {
    // 检查 NATS 是否运行
    if !check_nats_running().await? {
        anyhow::bail!("未检测到 NATS 运行，请先执行 `docker compose up -d`");
    }

    // 查找服务器进程（通过 statefile），如果已运行则无需启动
    let cfg_dir = find_config_dir()?;
    if let Some(s) = load_server_state(&cfg_dir) {
        if pid_alive(s.pid) {
            println!(
                "  {} {}",
                style("ℹ").yellow(),
                style("服务器已在运行中，无需重复启动").yellow()
            );
            return Ok(false);
        } else {
            // 清理无效 state
            clear_server_state(&cfg_dir)?;
        }
    }

    let server_bin = find_server_binary()?;

    let mut cmd = std::process::Command::new(&server_bin);
    cmd.current_dir(&cfg_dir);
    cmd.arg("--config").arg("oasis.toml");

    if daemon {
        // 将服务端 stdout/stderr 重定向到日志文件
        let log_path = if log_file.starts_with('/') {
            PathBuf::from(log_file)
        } else {
            let rel = log_file.strip_prefix("./").unwrap_or(log_file);
            cfg_dir.join(rel)
        };
        let logfile = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("无法打开日志文件: {}", log_path.display()))?;
        let logfile_err = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("无法打开日志文件: {}", log_path.display()))?;
        // 彻底脱离终端：关闭子进程标准输入
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(logfile);
        cmd.stderr(logfile_err);
        let child = cmd
            .spawn()
            .with_context(|| format!("无法以守护进程模式启动服务器: {}", server_bin.display()))?;
        // 记录 statefile
        let cfg_path = cfg_dir.join("oasis.toml");
        let cfg = oasis_core::config::OasisConfig::load_config(Some(
            cfg_path.to_string_lossy().as_ref(),
        ))?;
        let fingerprint = compute_certs_fingerprint(&cfg, &cfg_dir);
        let state = ServerState {
            pid: child.id(),
            cfg_dir: cfg_dir.to_string_lossy().to_string(),
            log_file: log_path.to_string_lossy().to_string(),
            certs_fingerprint: fingerprint,
            started_at: chrono::Utc::now().timestamp(),
        };
        save_server_state(&cfg_dir, &state)?;
    } else {
        let status = cmd
            .status()
            .with_context(|| format!("无法以前台模式启动服务器: {}", server_bin.display()))?;
        if !status.success() {
            anyhow::bail!("服务器进程异常退出，状态码: {}", status);
        }
    }

    Ok(true)
}

async fn run_stop() -> Result<()> {
    let cfg_dir = find_config_dir()?;
    let state = load_server_state(&cfg_dir);
    let pid = state.as_ref().map(|s| s.pid).filter(|pid| pid_alive(*pid));
    if pid.is_none() {
        println!(
            "  {} {}",
            style("ℹ").yellow(),
            style("未发现服务器进程").yellow()
        );
        return Ok(());
    }

    if let Some(pid) = pid {
        // 优雅停止
        let status = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(&pid.to_string())
            .status()
            .context("发送 SIGTERM 信号失败")?;

        if !status.success() {
            anyhow::bail!("发送 SIGTERM 信号到进程 {} 失败", pid);
        }

        // 等待进程退出（最多 5 秒），否则升级为 SIGKILL
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            // kill -0 检查是否仍存活
            let alive = std::process::Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .status()
                .map(|st| st.success())
                .unwrap_or(false);
            if !alive {
                break;
            }
            if std::time::Instant::now() >= deadline {
                // 强制杀死
                let _ = std::process::Command::new("kill")
                    .arg("-KILL")
                    .arg(pid.to_string())
                    .status();
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        clear_server_state(&cfg_dir)?;
        println!("  {} {}", style("✔").green(), style("服务器已停止").green());
    }
    Ok(())
}

async fn create_docker_compose(output_dir: &PathBuf) -> Result<()> {
    let docker_compose_path = output_dir.join("docker-compose.yml");
    // Ensure data directories exist
    let nats_data_dir = output_dir.join("data").join("nats");
    std::fs::create_dir_all(&nats_data_dir)?;

    // Generate docker-compose with NATS (no direct host ports)
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

    // Write compose file
    std::fs::write(&docker_compose_path, content)?;

    Ok(())
}

async fn check_nats_running() -> Result<bool> {
    // 1. 检查容器是否存在
    let output = std::process::Command::new("docker")
        .args(&[
            "ps",
            "--filter",
            "name=oasis-nats",
            "--format",
            "{{.Names}}",
        ])
        .output()
        .context("Failed to check NATS container")?;

    let output_str = String::from_utf8_lossy(&output.stdout);
    if output_str.trim() != "oasis-nats" {
        return Ok(false);
    }

    // 2. 检查 8222 监控端口是否可达（避免触发 TLS EOF）
    for attempt in 1..=3 {
        match tokio::net::TcpStream::connect("127.0.0.1:8222").await {
            Ok(_) => return Ok(true),
            Err(_) => {
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // 3. 如果端口检查失败，尝试通过 docker exec 检查容器内部状态
    let status_output = std::process::Command::new("docker")
        .args(&["exec", "oasis-nats", "pgrep", "-f", "nats-server"])
        .output();

    Ok(status_output.map(|o| o.status.success()).unwrap_or(false))
}

#[allow(dead_code)]
async fn check_certificates_exist() -> Result<bool> {
    let cfg_dir = find_config_dir()?;
    let cfg_path = cfg_dir.join("oasis.toml");
    let cfg =
        oasis_core::config::OasisConfig::load_config(Some(cfg_path.to_string_lossy().as_ref()))?;
    let paths = [
        cfg.tls.nats_ca_path(),
        cfg.tls.nats_client_cert_path(),
        cfg.tls.nats_client_key_path(),
        cfg.tls.grpc_ca_path(),
        cfg.tls.grpc_server_cert_path(),
        cfg.tls.grpc_server_key_path(),
        cfg.tls.grpc_client_cert_path(),
        cfg.tls.grpc_client_key_path(),
    ];

    for p in paths {
        let resolved = if p.is_absolute() {
            p.clone()
        } else {
            cfg_dir.join(p)
        };
        if !resolved.exists() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn find_server_binary() -> Result<PathBuf> {
    // 1. 检查环境变量
    if let Ok(bin_path) = std::env::var("OASIS_SERVER_BIN") {
        let path = PathBuf::from(bin_path);
        if path.exists() {
            return Ok(path);
        }
    }

    // 2. 检查 PATH 中的 oasis-server
    if let Ok(path) = which::which("oasis-server") {
        return Ok(path);
    }

    // 3. 检查 target 目录
    let cwd = std::env::current_dir()?;
    let candidates = [
        cwd.join("target/release/oasis-server"),
        cwd.join("target/debug/oasis-server"),
    ];

    for candidate in &candidates {
        if candidate.exists() {
            return Ok(candidate.clone());
        }
    }

    anyhow::bail!(
        "未找到 oasis-server 二进制文件。请尝试:\n  1. 在项目根目录执行 `cargo build -p oasis-server`\n  2. 将 `oasis-server` 路径加入到 PATH 环境变量\n  3. 设置 OASIS_SERVER_BIN 环境变量指向正确的二进制文件路径"
    )
}
