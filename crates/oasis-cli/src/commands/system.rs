use anyhow::{Context, Result};
use console::style;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyUsagePurpose, SanType, PKCS_ECDSA_P256_SHA256,
};
use std::net::IpAddr;
use std::path::PathBuf;

fn find_config_dir() -> Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    // 优先读取标记文件
    let marker = cwd.join(".oasis_last_init");
    if let Ok(path) = std::fs::read_to_string(&marker) {
        let p = PathBuf::from(path.trim());
        if p.join("oasis.toml").exists() {
            return Ok(p);
        }
    }
    let mut dirs: Vec<PathBuf> = vec![];
    // 收集包含 oasis.toml 的候选目录（当前目录及一层子目录）
    let mut candidates: Vec<(PathBuf, std::time::SystemTime)> = vec![];
    for dir in std::fs::read_dir(&cwd)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
    {
        dirs.push(dir);
    }
    dirs.push(cwd.clone());
    for d in dirs {
        let cfg = d.join("oasis.toml");
        if cfg.exists() {
            let mtime = std::fs::metadata(&cfg)
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            candidates.push((d, mtime));
        }
    }
    if candidates.is_empty() {
        return Ok(cwd);
    }
    // 根据修改时间倒序选择最近的配置目录
    candidates.sort_by(|a, b| b.1.cmp(&a.1));

    let required = [
        "certs/nats-ca.pem",
        "certs/nats-client.pem",
        "certs/nats-client-key.pem",
        "certs/grpc-ca.pem",
        "certs/grpc-server.pem",
        "certs/grpc-server-key.pem",
        "certs/grpc-client.pem",
        "certs/grpc-client-key.pem",
    ];
    for (dir, _) in &candidates {
        let mut ok = true;
        for rel in required.iter() {
            if !dir.join(rel).exists() {
                ok = false;
                break;
            }
        }
        if ok {
            return Ok(dir.clone());
        }
    }
    Ok(candidates[0].0.clone())
}

#[derive(Debug)]
struct InitArgs {
    output_dir: PathBuf,
    force: bool,
}

#[derive(clap::Subcommand, Debug)]
pub enum SystemCommands {
    /// 初始化 Oasis 系统（生成证书、创建配置与 docker-compose）
    Init {
        /// 输出目录（默认当前目录）
        #[arg(short, long, default_value = ".")]
        output_dir: PathBuf,

        /// 强制覆盖已存在文件
        #[arg(short, long)]
        force: bool,
    },

    /// 启动 Oasis 服务器
    Start {
        /// 以守护进程方式运行
        #[arg(short, long)]
        daemon: bool,

        /// 将服务端日志重定向到指定文件（仅守护模式生效）
        #[arg(long, value_name = "LOG_FILE", default_value = "./oasis-server.log")]
        log_file: String,
    },

    /// 停止 Oasis 服务器
    Stop,

    /// 查看 Oasis 服务器状态
    Status,
}

pub async fn run_system(cmd: SystemCommands) -> Result<()> {
    match cmd {
        SystemCommands::Init { output_dir, force } => {
            run_init(&InitArgs { output_dir, force }).await
        }
        SystemCommands::Start { daemon, log_file } => run_start_with_ui(daemon, log_file).await,
        SystemCommands::Stop => run_stop_with_ui().await,
        SystemCommands::Status => run_status_with_ui().await,
    }
}

async fn run_init(args: &InitArgs) -> Result<()> {
    println!("{}", style("初始化 Oasis 系统...").bold());

    // 生成证书
    let certs_dir = args.output_dir.join("certs");
    if args.force && certs_dir.exists() {
        std::fs::remove_dir_all(&certs_dir).ok();
    }
    CertificateGenerator::generate_all(&certs_dir).await?;

    // 写入 docker-compose.yml
    create_docker_compose(&args.output_dir).await?;
    println!(
        "  {} {}",
        style("✔").green(),
        style("创建 docker-compose.yml").cyan()
    );

    // 写入 oasis.toml
    create_default_config(&args.output_dir).await?;
    println!(
        "  {} {}",
        style("✔").green(),
        style("创建默认配置 oasis.toml").cyan()
    );

    // 记录最近一次 init 的目录
    let marker = std::env::current_dir()?.join(".oasis_last_init");
    std::fs::write(&marker, args.output_dir.to_string_lossy().as_bytes()).ok();

    println!("\n{}", style("初始化完成，接下来:").bold());
    println!(
        "  1. {}",
        style("在项目根目录执行: docker-compose up -d").cyan()
    );
    println!("  2. {}", style("启动服务: oasis-cli system start").cyan());
    Ok(())
}

async fn run_start_with_ui(daemon: bool, log_file: String) -> Result<()> {
    println!("{}", style("正在启动 Oasis 服务器...").bold());
    match run_start(daemon, &log_file).await {
        Ok(started) => {
            if started {
                println!(
                    "  {} {}",
                    style("✔").green(),
                    style("Oasis 服务器启动成功").green()
                );
            }
            Ok(())
        }
        Err(e) => {
            println!(
                "  {} {}: {}",
                style("✖").red(),
                style("Oasis 服务器启动失败").red(),
                e
            );
            Err(e)
        }
    }
}

async fn run_stop_with_ui() -> Result<()> {
    println!("{}", style("正在停止 Oasis 服务器...").bold());
    match run_stop().await {
        Ok(_) => {
            println!(
                "  {} {}",
                style("✔").green(),
                style("Oasis 服务器已停止").green()
            );
            Ok(())
        }
        Err(e) => {
            println!(
                "  {} {}: {}",
                style("✖").red(),
                style("停止服务器失败").red(),
                e
            );
            Err(e)
        }
    }
}

async fn run_status_with_ui() -> Result<()> {
    let server_pids = find_server_processes().await?;
    if server_pids.is_empty() {
        println!(
            "{} {}",
            style("●").red(),
            style("Oasis 服务器未运行。").bold()
        );
    } else {
        println!(
            "{} {} (PID: {})",
            style("●").green(),
            style("Oasis 服务器正在运行").bold(),
            server_pids
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}

async fn run_start(daemon: bool, log_file: &str) -> Result<bool> {
    // 检查 NATS 是否运行
    if !check_nats_running().await? {
        anyhow::bail!("未检测到 NATS 运行，请先执行 `docker-compose up -d`");
    }

    // 检查证书文件是否存在
    if !check_certificates_exist().await? {
        anyhow::bail!("未找到证书文件，请先执行 `oasis-cli system init`");
    }

    // 查找服务器进程，如果已运行则无需启动
    if !find_server_processes().await?.is_empty() {
        println!(
            "  {} {}",
            style("ℹ").yellow(),
            style("服务器已在运行中，无需重复启动").yellow()
        );
        return Ok(false);
    }

    let server_bin = find_server_binary()?;
    let config_dir = find_config_dir()?;

    let mut cmd = std::process::Command::new(&server_bin);
    cmd.current_dir(&config_dir);
    cmd.arg("--config").arg("oasis.toml");

    if daemon {
        // 将服务端 stdout/stderr 重定向到日志文件
        let log_path = std::path::Path::new(log_file);
        let logfile = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .with_context(|| format!("无法打开日志文件: {}", log_file))?;
        let logfile_err = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .with_context(|| format!("无法打开日志文件: {}", log_file))?;
        cmd.stdout(logfile);
        cmd.stderr(logfile_err);
        cmd.spawn()
            .with_context(|| format!("无法以守护进程模式启动服务器: {}", server_bin.display()))?;
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
    let pids = find_server_processes().await?;

    if pids.is_empty() {
        println!(
            "  {} {}",
            style("ℹ").yellow(),
            style("未发现服务器进程").yellow()
        );
        return Ok(());
    }

    for pid in pids {
        // 优雅停止
        let status = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(&pid.to_string())
            .status()
            .context("发送 SIGTERM 信号失败")?;

        if !status.success() {
            anyhow::bail!("发送 SIGTERM 信号到进程 {} 失败", pid);
        }
    }
    Ok(())
}

async fn create_docker_compose(output_dir: &PathBuf) -> Result<()> {
    let docker_compose_path = std::env::current_dir()?.join("docker-compose.yml");
    let data_dir = output_dir.join("data").join("nats");
    std::fs::create_dir_all(&data_dir)?;

    // 获取相对于项目根目录的路径（失败则使用绝对路径）
    let current_dir = std::env::current_dir()?;
    let certs_rel = output_dir
        .strip_prefix(&current_dir)
        .ok()
        .map(|p| p.join("certs"))
        .unwrap_or_else(|| output_dir.join("certs"));
    let data_rel = output_dir
        .strip_prefix(&current_dir)
        .ok()
        .map(|p| p.join("data").join("nats"))
        .unwrap_or_else(|| output_dir.join("data").join("nats"));

    let content = format!(
        r#"services:
  nats:
    image: nats:2.10-alpine
    container_name: oasis-nats
    ports:
      - "4222:4222"
      - "8222:8222"
    command: >
      -js
      -m 8222
      --tls
      --tlscert=/etc/nats/certs/nats-server.pem
      --tlskey=/etc/nats/certs/nats-server-key.pem
      --tlscacert=/etc/nats/certs/nats-ca.pem
      --tlsverify
    volumes:
      - {}:/etc/nats/certs:ro
      - {}:/data
    restart: unless-stopped
"#,
        format!("./{}", certs_rel.display()),
        format!("./{}", data_rel.display())
    );

    std::fs::write(&docker_compose_path, content)?;
    Ok(())
}

async fn create_default_config(output_dir: &PathBuf) -> Result<()> {
    let config_path = output_dir.join("oasis.toml");

    let content = r#"# Oasis 配置文件

# 服务器监听地址
listen_addr = "0.0.0.0:50051"

# 数据目录
data_dir = "./data/oasis"

# 日志配置
[telemetry]
# 日志级别：trace, debug, info, warn, error
log_level = "info"
# 日志格式：text, json
log_format = "text"
# 是否禁用 ANSI 颜色
log_no_ansi = false

# NATS 配置
[nats]
# NATS 服务器地址
url = "tls://127.0.0.1:4222"

# gRPC 配置（对外 URL）
[grpc]
url = "https://127.0.0.1:50051"

# 服务器配置
[server]
# 心跳超时时间（秒）
heartbeat_ttl_sec = 60

# Agent 配置
[agent]
# 心跳间隔时间（秒）
heartbeat_interval_sec = 30
# 事实收集间隔时间（秒）
fact_collection_interval_sec = 300
"#;

    std::fs::write(&config_path, content)?;
    Ok(())
}

async fn check_nats_running() -> Result<bool> {
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
    Ok(output_str.trim() == "oasis-nats")
}

async fn check_certificates_exist() -> Result<bool> {
    let cfg_dir = find_config_dir()?;
    let cfg_path = cfg_dir.join("oasis.toml");
    let cfg =
        oasis_core::config::OasisConfig::load_config(Some(cfg_path.to_string_lossy().as_ref()))?;
    let paths = [
        cfg.nats.ca_path,
        cfg.nats.client_cert_path,
        cfg.nats.client_key_path,
        cfg.tls.grpc_ca_path,
        cfg.tls.grpc_server_cert_path,
        cfg.tls.grpc_server_key_path,
        cfg.tls.grpc_client_cert_path,
        cfg.tls.grpc_client_key_path,
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

async fn find_server_processes() -> Result<Vec<u32>> {
    // 使用精确匹配进程名，避免误匹配包含 "oasis-server" 的其他命令行
    let output = std::process::Command::new("pgrep")
        .arg("-x")
        .arg("oasis-server")
        .output()
        .context("Failed to find server processes")?;

    if !output.status.success() {
        return Ok(vec![]);
    }

    // 解析 PID，并通过 kill -0 校验进程仍然存活
    let mut alive_pids: Vec<u32> = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Ok(pid) = line.trim().parse::<u32>() {
            let status = std::process::Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .status();
            if let Ok(st) = status {
                if st.success() {
                    alive_pids.push(pid);
                }
            }
        }
    }
    Ok(alive_pids)
}

fn find_server_binary() -> Result<PathBuf> {
    let exe_path = std::env::current_exe().unwrap_or_default();
    let is_cli_debug = exe_path.to_string_lossy().contains("/debug/");
    let (first, third) = if is_cli_debug {
        (
            std::env::current_dir()?.join("target/debug/oasis-server"),
            std::env::current_dir()?.join("target/release/oasis-server"),
        )
    } else {
        (
            std::env::current_dir()?.join("target/release/oasis-server"),
            std::env::current_dir()?.join("target/debug/oasis-server"),
        )
    };
    let candidate_bins = [first, std::path::PathBuf::from("oasis-server"), third];

    for c in candidate_bins.iter() {
        if c.components().count() == 1 {
            if let Ok(p) = which::which(c) {
                return Ok(p);
            }
        } else if c.exists() {
            return Ok(c.clone());
        }
    }

    anyhow::bail!(
        "未找到 oasis-server 二进制文件。请尝试:\n  1. 在项目根目录执行 `cargo build -p oasis-server`\n  2. 将 `oasis-server` 路径加入到 PATH 环境变量"
    )
}

/// Certificate generator for CLI
struct CertificateGenerator;

impl CertificateGenerator {
    /// Generate all certificates for mTLS authentication
    async fn generate_all(certs_dir: &std::path::Path) -> Result<()> {
        println!("› {}", style("正在生成 mTLS 证书...").bold());

        // 创建证书目录
        std::fs::create_dir_all(certs_dir).context("Failed to create certificates directory")?;

        // 生成统一 CA（ECDSA P-256）
        let ca = Self::generate_ca("Oasis Root CA")?;
        let ca_pem = ca.serialize_pem()?;
        let ca_key_pem = ca.serialize_private_key_pem();
        // 同一 CA 写入到 nats/grpc 的 CA 文件
        std::fs::write(certs_dir.join("nats-ca.pem"), &ca_pem)?;
        std::fs::write(certs_dir.join("nats-ca-key.pem"), &ca_key_pem)?;
        std::fs::write(certs_dir.join("grpc-ca.pem"), &ca_pem)?;
        std::fs::write(certs_dir.join("grpc-ca-key.pem"), &ca_key_pem)?;

        // 生成 NATS 证书（统一 CA 签发）
        Self::generate_nats_certificates_with_ca(certs_dir, &ca)?;

        // 生成 gRPC 证书（统一 CA 签发）
        Self::generate_grpc_certificates_with_ca(certs_dir, &ca)?;

        println!(
            "  {} mTLS 证书已成功生成到 {}",
            style("✔").green(),
            style(certs_dir.display()).cyan()
        );
        Ok(())
    }

    /// Create a CA certificate with given CN
    fn generate_ca(common_name: &str) -> Result<Certificate> {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, common_name);

        let mut params = CertificateParams::default();
        params.alg = &PKCS_ECDSA_P256_SHA256;
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        Ok(Certificate::from_params(params)?)
    }

    /// Create a leaf certificate signed by given CA
    fn generate_leaf_signed_by_ca(
        ca: &Certificate,
        common_name: &str,
        sans_dns: &[&str],
        sans_ip: &[IpAddr],
        extended_ekus: &[ExtendedKeyUsagePurpose],
        key_usages: Vec<KeyUsagePurpose>,
    ) -> Result<Certificate> {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, common_name);

        let mut params = CertificateParams::default();
        params.alg = &PKCS_ECDSA_P256_SHA256;
        params.distinguished_name = dn;
        params.is_ca = IsCa::NoCa;
        params.key_usages = key_usages;
        params.extended_key_usages = extended_ekus.to_vec();
        for d in sans_dns {
            params
                .subject_alt_names
                .push(SanType::DnsName((*d).to_string()));
        }
        for ip in sans_ip {
            params.subject_alt_names.push(SanType::IpAddress(*ip));
        }

        let cert = Certificate::from_params(params)?;
        // Validate signing path
        let _ = cert.serialize_der_with_signer(ca)?;
        Ok(cert)
    }

    /// Generate NATS certificates using provided CA
    fn generate_nats_certificates_with_ca(
        certs_dir: &std::path::Path,
        ca: &Certificate,
    ) -> Result<()> {
        // 生成 NATS 服务器证书（由统一 CA 签发）
        let server_cert = Self::generate_leaf_signed_by_ca(
            ca,
            "nats-server",
            &["localhost", "127.0.0.1", "0.0.0.0"],
            &["127.0.0.1".parse().unwrap(), "0.0.0.0".parse().unwrap()],
            &[ExtendedKeyUsagePurpose::ServerAuth],
            vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyEncipherment,
            ],
        )?;
        std::fs::write(
            certs_dir.join("nats-server.pem"),
            server_cert.serialize_pem_with_signer(ca)?,
        )?;
        std::fs::write(
            certs_dir.join("nats-server-key.pem"),
            server_cert.serialize_private_key_pem(),
        )?;

        // 生成 NATS 客户端证书（由统一 CA 签发）
        let client_cert = Self::generate_leaf_signed_by_ca(
            ca,
            "oasis-nats-client",
            &["oasis-client"],
            &[],
            &[ExtendedKeyUsagePurpose::ClientAuth],
            vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyEncipherment,
            ],
        )?;
        std::fs::write(
            certs_dir.join("nats-client.pem"),
            client_cert.serialize_pem_with_signer(ca)?,
        )?;
        std::fs::write(
            certs_dir.join("nats-client-key.pem"),
            client_cert.serialize_private_key_pem(),
        )?;

        println!("  {} 生成 NATS 证书", style("✔").green());
        Ok(())
    }

    /// Generate gRPC certificates using provided CA
    fn generate_grpc_certificates_with_ca(
        certs_dir: &std::path::Path,
        ca: &Certificate,
    ) -> Result<()> {
        // 生成 gRPC 服务器证书（由统一 CA 签发）
        let grpc_server = Self::generate_leaf_signed_by_ca(
            ca,
            "oasis-grpc-server",
            &["localhost", "127.0.0.1", "0.0.0.0"],
            &["127.0.0.1".parse().unwrap(), "0.0.0.0".parse().unwrap()],
            &[ExtendedKeyUsagePurpose::ServerAuth],
            vec![KeyUsagePurpose::DigitalSignature],
        )?;
        std::fs::write(
            certs_dir.join("grpc-server.pem"),
            grpc_server.serialize_pem_with_signer(ca)?,
        )?;
        std::fs::write(
            certs_dir.join("grpc-server-key.pem"),
            grpc_server.serialize_private_key_pem(),
        )?;

        // 生成 gRPC 客户端证书（由统一 CA 签发）
        let grpc_client = Self::generate_leaf_signed_by_ca(
            ca,
            "oasis-grpc-client",
            &["oasis-grpc-client"],
            &[],
            &[ExtendedKeyUsagePurpose::ClientAuth],
            vec![KeyUsagePurpose::DigitalSignature],
        )?;
        std::fs::write(
            certs_dir.join("grpc-client.pem"),
            grpc_client.serialize_pem_with_signer(ca)?,
        )?;
        std::fs::write(
            certs_dir.join("grpc-client-key.pem"),
            grpc_client.serialize_private_key_pem(),
        )?;

        println!("  {} 生成 gRPC 证书", style("✔").green());
        Ok(())
    }
}
