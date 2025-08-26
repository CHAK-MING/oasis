use anyhow::{Context, Result};
use clap::Subcommand;
use comfy_table::{presets::UTF8_FULL, Attribute, Cell, CellAlignment, ContentArrangement, Table};
use console::style;
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub enum AgentCommands {
    /// 部署 agent 到远端机器
    Deploy {
        /// 目标主机（SSH 连接串，如 user@host）
        #[arg(short, long)]
        target: String,

        /// SSH 私钥路径
        #[arg(short, long)]
        key: Option<PathBuf>,

        /// Agent 连接的 Server URL
        #[arg(short, long, default_value = "https://127.0.0.1:50051")]
        server_url: String,

        /// Agent 连接的 NATS URL
        #[arg(short, long, default_value = "tls://127.0.0.1:4222")]
        nats_url: String,

        /// 部署文件输出目录
        #[arg(short, long, default_value = "./deploy")]
        output_dir: PathBuf,
    },

    /// 列出已部署的 agents
    List {
        /// 显示详细信息（包括 facts）
        #[arg(short, long)]
        verbose: bool,
    },

    /// 从远端移除 agent
    Remove {
        /// 目标主机（SSH 连接串，如 user@host）
        #[arg(short, long)]
        target: String,

        /// SSH 私钥路径
        #[arg(short, long)]
        key: Option<PathBuf>,
    },
}

pub async fn run_agent(cmd: AgentCommands) -> Result<()> {
    match cmd {
        AgentCommands::Deploy {
            target,
            key,
            server_url,
            nats_url,
            output_dir,
        } => deploy_agent(target, key, server_url, nats_url, output_dir).await,
        AgentCommands::List { verbose } => list_agents(verbose).await,
        AgentCommands::Remove { target, key } => remove_agent(target, key).await,
    }
}

async fn deploy_agent(
    target: String,
    key: Option<PathBuf>,
    server_url: String,
    nats_url: String,
    output_dir: PathBuf,
) -> Result<()> {
    println!(
        "{} {}",
        style("部署 Agent 到:").bold(),
        style(&target).cyan()
    );

    // 创建部署目录
    std::fs::create_dir_all(&output_dir)?;

    let deploy_script = generate_deploy_script(&target, &server_url, &nats_url)?;
    let script_path = output_dir.join("deploy-agent.sh");
    std::fs::write(&script_path, deploy_script)?;
    println!("  {} {}", style("✔").green(), style("生成部署脚本").dim());

    // 生成环境变量文件
    let env_file = generate_env_file(&server_url, &nats_url)?;
    let env_path = output_dir.join("agent.env");
    std::fs::write(&env_path, env_file)?;
    println!(
        "  {} {}",
        style("✔").green(),
        style("生成环境变量文件").dim()
    );

    // 拷贝本地证书到输出目录（如果存在）
    let local_certs = PathBuf::from("./certs");
    if local_certs.exists() && local_certs.is_dir() {
        let out_certs = output_dir.join("certs");
        if out_certs.exists() {
            std::fs::remove_dir_all(&out_certs).ok();
        }
        std::fs::create_dir_all(&out_certs)?;
        for entry in std::fs::read_dir(&local_certs)? {
            let entry = entry?;
            let src = entry.path();
            if src.is_file() {
                let dst = out_certs.join(src.file_name().unwrap());
                std::fs::copy(src, dst)?;
            }
        }
        println!("  {} {}", style("✔").green(), style("复制本地证书").dim());
    }

    // 生成 systemd 服务文件
    let service_file = generate_systemd_service()?;
    let service_path = output_dir.join("oasis-agent.service");
    std::fs::write(&service_path, service_file)?;
    println!(
        "  {} {}",
        style("✔").green(),
        style("生成 systemd 服务文件").dim()
    );

    // 生成安装脚本
    let install_script = generate_install_script(&target, &key)?;
    let install_path = output_dir.join("install.sh");
    std::fs::write(&install_path, install_script)?;
    println!("  {} {}", style("✔").green(), style("生成安装脚本").dim());

    println!(
        "\n{} {}",
        style("部署文件已生成到:").bold(),
        style(output_dir.display()).cyan()
    );
    println!("{}", style("接下来:").bold());
    println!(
        "  1. {}",
        style("复制 `oasis-agent` 二进制文件到输出目录").cyan()
    );
    println!("  2. {}", style("运行 `./install.sh` 完成部署").cyan());

    Ok(())
}

fn generate_deploy_script(target: &str, _server_url: &str, _nats_url: &str) -> Result<String> {
    let script = format!(
        r#"#!/bin/bash
# Oasis Agent 部署脚本
# 目标主机: {target}

set -e

echo "部署 Oasis Agent 到 {target}..."

# 创建必要目录
sudo mkdir -p /opt/oasis/agent
sudo mkdir -p /opt/oasis/certs
sudo mkdir -p /var/log/oasis

# 复制二进制文件
sudo cp oasis-agent /opt/oasis/agent/
sudo chmod +x /opt/oasis/agent/oasis-agent

# 复制环境变量文件
sudo cp agent.env /opt/oasis/agent/

# 复制证书（如果从安装脚本上传）
if [ -d "certs" ]; then
  sudo cp -r certs/* /opt/oasis/certs/
  sudo chmod 600 /opt/oasis/certs/*.pem || true
fi

# 安装 systemd 服务
sudo cp oasis-agent.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable oasis-agent
sudo systemctl start oasis-agent

echo "Oasis Agent 部署完成"
echo "  查看状态: sudo systemctl status oasis-agent"
echo "  查看日志: sudo journalctl -u oasis-agent -f"
"#
    );

    Ok(script)
}

fn generate_env_file(server_url: &str, nats_url: &str) -> Result<String> {
    let env = format!(
        r#"# Oasis Agent 环境变量
# 该文件由工具自动生成，请勿手工修改

# Server 配置
OASIS_SERVER_URL={server_url}

# NATS 配置
OASIS_NATS_URL={nats_url}
OASIS_NATS_CA=/opt/oasis/certs/nats-ca.pem
OASIS_NATS_CLIENT_CERT=/opt/oasis/certs/nats-client.pem
OASIS_NATS_CLIENT_KEY=/opt/oasis/certs/nats-client-key.pem

# gRPC 配置
OASIS_GRPC_CA=/opt/oasis/certs/grpc-ca.pem
OASIS_GRPC_CLIENT_CERT=/opt/oasis/certs/grpc-client.pem
OASIS_GRPC_CLIENT_KEY=/opt/oasis/certs/grpc-client-key.pem

# Agent 配置
OASIS_LOG_LEVEL=info
OASIS_HEARTBEAT_INTERVAL_SEC=30
OASIS_FACT_COLLECTION_INTERVAL_SEC=300

# 性能配置
OASIS_MAX_CONCURRENT_TASKS=10
"#
    );

    Ok(env)
}

fn generate_systemd_service() -> Result<String> {
    let service = r#"[Unit]
Description=Oasis Agent
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/opt/oasis/agent
EnvironmentFile=/opt/oasis/agent/agent.env
ExecStart=/opt/oasis/agent/oasis-agent
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=oasis-agent

# 安全设置
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/log/oasis

[Install]
WantedBy=multi-user.target
"#;

    Ok(service.to_string())
}

fn generate_install_script(target: &str, key: &Option<PathBuf>) -> Result<String> {
    let key_arg = if let Some(key_path) = key {
        format!("-i {}", key_path.display())
    } else {
        String::new()
    };

    let script = format!(
        r#"#!/bin/bash
# Oasis Agent 安装脚本

set -e

echo "正在安装 Oasis Agent..."

# 检查必要文件
if [ ! -f "oasis-agent" ]; then
    echo "oasis-agent 二进制文件未找到"
    exit 1
fi

if [ ! -f "deploy-agent.sh" ]; then
    echo "deploy-agent.sh 脚本未找到"
    exit 1
fi

echo "正在复制文件到 {target}..."
scp {key_arg} oasis-agent deploy-agent.sh agent.env oasis-agent.service {target}:/tmp/
# 复制证书（如果从安装脚本上传）
if [ -d "certs" ]; then
  scp -r {key_arg} certs {target}:/tmp/
fi

echo "正在运行部署脚本..."
ssh {key_arg} {target} "cd /tmp && chmod +x deploy-agent.sh && ./deploy-agent.sh"

echo "安装完成"
echo "  Agent 正在运行 {target}"
echo "  查看状态: ssh {key_arg} {target} 'sudo systemctl status oasis-agent'"
"#
    );

    Ok(script)
}

async fn list_agents(verbose: bool) -> Result<()> {
    println!("{}", style("列出所有已连接的 Agent...").bold());

    // 复用 node 命令的逻辑来获取 Agent 列表
    let config = oasis_core::config::OasisConfig::load_config(None)?;
    let server_addr = config.listen_addr;
    let server_url = format!("https://{}", server_addr);

    let endpoint =
        tonic::transport::Endpoint::from_shared(server_url).context("Invalid server URL")?;

    // 加载客户端证书
    let certs_dir = std::env::current_dir()?.join("certs");
    let ca_cert = tokio::fs::read(certs_dir.join("grpc-ca.pem")).await?;
    let client_cert = tokio::fs::read(certs_dir.join("grpc-client.pem")).await?;
    let client_key = tokio::fs::read(certs_dir.join("grpc-client-key.pem")).await?;

    let tls_config = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca_cert))
        .identity(tonic::transport::Identity::from_pem(
            client_cert,
            client_key,
        ));

    let channel = endpoint
        .tls_config(tls_config)?
        .connect()
        .await
        .context("Failed to connect to server")?;

    let mut client = oasis_core::proto::oasis_service_client::OasisServiceClient::new(channel);

    // 调用服务器 API 获取节点列表
    let response = client
        .list_nodes(oasis_core::proto::ListNodesRequest {
            selector: Some(String::new()),
            verbose: false,
        })
        .await
        .context("Failed to get nodes from server")?;

    let nodes = response.into_inner().nodes;

    if nodes.is_empty() {
        println!("  {}", style("没有已连接的 Agent").yellow());
        return Ok(());
    }

    if verbose {
        // 详细模式：显示 facts 信息
        for node in nodes {
            let agent_id = node
                .agent_id
                .as_ref()
                .map(|id| id.value.clone())
                .unwrap_or_default();
            let is_online = node.is_online;
            let labels = node
                .labels
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(", ");

            println!("\n{} {}", style("Agent ID:").bold(), style(agent_id).cyan());
            println!(
                "  {}: {}",
                style("状态").dim(),
                if is_online {
                    style("在线").green()
                } else {
                    style("离线").red()
                }
            );
            println!(
                "  {}: {}",
                style("标签").dim(),
                if labels.is_empty() {
                    "-".to_string()
                } else {
                    labels
                }
            );

            // 解析并显示 facts（来自 Protobuf 结构）
            if let Some(facts) = node.facts {
                println!("  {}:", style("系统信息").dim());
                println!("    {}: {}", style("操作系统").dim(), facts.os_name);
                println!("    {}: {}", style("版本").dim(), facts.os_version);
                println!("    {}: {}", style("CPU 核心数").dim(), facts.cpu_cores);
                let mem_gb = facts.memory_total_bytes / (1024 * 1024 * 1024);
                println!("    {}: {} GB", style("内存").dim(), mem_gb);
                println!("    {}: {}", style("主机名").dim(), facts.hostname);
                println!("    {}: {}", style("主 IP").dim(), facts.primary_ip);
                println!("    {}: {}", style("Agent 版本").dim(), facts.agent_version);
                let datetime = chrono::DateTime::from_timestamp(facts.collected_at, 0)
                    .unwrap_or_default()
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();
                println!("    {}: {}", style("信息收集时间").dim(), datetime);
                println!("    {}: {}", style("内核版本").dim(), facts.kernel_version);
                println!("    {}: {}", style("CPU 架构").dim(), facts.cpu_arch);
            }
        }
    } else {
        // 简洁模式：表格显示
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec![
            Cell::new("AGENT ID").add_attribute(Attribute::Bold),
            Cell::new("主机名").add_attribute(Attribute::Bold),
            Cell::new("状态").add_attribute(Attribute::Bold),
            Cell::new("标签").add_attribute(Attribute::Bold),
        ]);

        for node in nodes {
            let agent_id = node
                .agent_id
                .as_ref()
                .map(|id| id.value.clone())
                .unwrap_or_default();
            let is_online = node.is_online;
            let labels = node
                .labels
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(", ");

            // 从 facts 中提取主机名
            let hostname = node
                .facts
                .as_ref()
                .map(|f| f.hostname.clone())
                .unwrap_or_else(|| "未知".to_string());

            table.add_row(vec![
                Cell::new(agent_id),
                Cell::new(hostname),
                Cell::new(if is_online {
                    style("在线").green().to_string()
                } else {
                    style("离线").red().to_string()
                }),
                Cell::new(if labels.is_empty() {
                    "-".to_string()
                } else {
                    labels
                }),
            ]);
        }

        // 将“状态”列（索引为 2）设置为居中对齐，以修复中英文混合显示问题
        if let Some(column) = table.column_mut(2) {
            column.set_cell_alignment(CellAlignment::Center);
        }

        println!("{}", table);
    }
    Ok(())
}

async fn remove_agent(target: String, key: Option<PathBuf>) -> Result<()> {
    println!(
        "{} {}",
        style("正在从远端移除 Agent:").bold(),
        style(&target).cyan()
    );

    let key_arg = if let Some(ref key_path) = key {
        format!("-i {}", key_path.display())
    } else {
        String::new()
    };

    // 生成卸载脚本
    let uninstall_script = r#"#!/bin/bash
# Oasis Agent 卸载脚本

set -e

echo "正在停止 Oasis Agent..."

# 停止并禁用服务
sudo systemctl stop oasis-agent || true
sudo systemctl disable oasis-agent || true

# 移除服务文件
sudo rm -f /etc/systemd/system/oasis-agent.service
sudo systemctl daemon-reload

# 移除已安装文件
sudo rm -rf /opt/oasis/agent
sudo rm -rf /opt/oasis/certs
sudo rm -rf /var/log/oasis

echo "Oasis Agent 已成功移除!"
"#;

    // 保存卸载脚本
    let script_name = format!("uninstall-{}.sh", target.replace(['@', ':'], "-"));
    std::fs::write(&script_name, uninstall_script)?;

    println!(
        "  {} {}",
        style("✔").green(),
        style(format!("已生成卸载脚本: {}", script_name)).dim()
    );
    println!("\n{}", style("请手动执行以下命令完成移除:").bold());
    println!(
        "  1. {}",
        style(format!("scp {} {} {}:/tmp/", key_arg, script_name, target)).cyan()
    );
    println!(
        "  2. {}",
        style(format!(
            "ssh {} {} 'chmod +x /tmp/{} && sudo /tmp/{}'",
            key_arg, target, script_name, script_name
        ))
        .cyan()
    );

    Ok(())
}
