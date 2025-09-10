use crate::client::format_grpc_error;
use crate::grpc_retry;
use crate::ui::{
    log_operation, print_header, print_info, print_next_step, print_progress, print_status,
    print_warning,
};
use anyhow::Result;
use clap::{Parser, Subcommand, arg, command};
use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table, presets::UTF8_FULL};
use console::style;
use oasis_core::proto::oasis_service_client::OasisServiceClient;
use oasis_core::proto::{RemoveAgentRequest, SetInfoAgentRequest};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "agent",
    about = "管理 Agent 节点",
    after_help = r#"示例：
  # 部署 Agent
  oasis-cli agent deploy \
    --ssh-target root@localhost 
    --agent-id agent-1234567890 \
    --nats-url tls://127.0.0.1:4222 \
    --output-dir ./agent-deploy \
    --labels "env=test" \
    --labels "role=worker" \
    --groups "test-group" \
    --agent-binary ./oasis-agent

  # 列出 Agent
  oasis-cli agent list
"#
)]
pub struct AgentArgs {
    #[command(subcommand)]
    cmd: AgentCmd,
}

#[derive(Subcommand, Debug)]
pub enum AgentCmd {
    /// 部署 agent 到远端机器
    Deploy(AgentDeployArgs),
    /// 列出 Agent
    List(AgentListArgs),
    /// 从远端移除 agent
    Remove(AgentRemoveArgs),
    /// 设置 Agent 标签或者分组
    Set(AgentSetArgs),
}

#[derive(Parser, Debug)]
pub struct AgentDeployArgs {
    /// 目标主机（SSH 连接串，如 user@host）
    #[arg(short, long)]
    ssh_target: String,

    /// Agent ID
    #[arg(short, long)]
    agent_id: String,

    /// SSH 私钥路径
    #[arg(short, long)]
    key: Option<PathBuf>,

    /// Agent 连接的 NATS URL
    #[arg(short, long, default_value = "tls://127.0.0.1:4222")]
    nats_url: String,

    /// 部署文件输出目录
    #[arg(short, long, default_value = "./deploy")]
    output_dir: PathBuf,

    /// Agent 标签，格式 KEY=VALUE，可多次指定
    #[arg(
            long,
            value_name = "KEY=VALUE",
            action = clap::ArgAction::Append,
            help = "Agent 标签（可重复）",
        )]
    labels: Vec<String>,

    /// Agent 组，可多次指定
    #[arg(
            long,
            value_name = "GROUP",
            action = clap::ArgAction::Append,
            help = "Agent 组（可重复）",
        )]
    groups: Vec<String>,

    /// 自动执行部署（无需手动运行脚本）
    #[arg(long)]
    auto_install: bool,

    /// Agent 二进制文件路径（如果提供，将自动复制）
    #[arg(long)]
    agent_binary: Option<PathBuf>,

    /// 为 Agent 生成独立证书
    #[arg(long)]
    generate_cert: bool,
}

#[derive(Parser, Debug)]
pub struct AgentListArgs {
    /// 显示详细信息（包括 facts）
    #[arg(short, long)]
    verbose: bool,

    #[arg(long, short = 't', help = "目标（选择器语法）")]
    target: String,

    /// 是否只显示在线的 Agent
    #[arg(long)]
    is_online: bool,
}

#[derive(Parser, Debug)]
pub struct AgentRemoveArgs {
    /// 目标主机（SSH 连接串，如 user@host）
    #[arg(short, long)]
    ssh_target: String,

    /// Agent ID
    #[arg(short, long)]
    agent_id: String,

    /// SSH 私钥路径
    #[arg(short, long)]
    key: Option<PathBuf>,
}

#[derive(Parser, Debug)]
pub struct AgentSetArgs {
    /// Agent ID
    #[arg(short, long)]
    agent_id: String,

    /// 标签
    #[arg(short, long)]
    labels: Option<Vec<String>>,

    /// 分组
    #[arg(short, long)]
    groups: Option<Vec<String>>,
}

/// 主入口函数 - 根据子命令分发执行
pub async fn run_agent(
    mut client: OasisServiceClient<tonic::transport::Channel>,
    args: AgentArgs,
) -> Result<()> {
    match args.cmd {
        AgentCmd::Deploy(deploy) => run_agent_deploy(&mut client, deploy).await,
        AgentCmd::List(list) => run_agent_list(&mut client, list).await,
        AgentCmd::Remove(remove) => run_agent_remove(&mut client, remove).await,
        AgentCmd::Set(set) => run_agent_set(&mut client, set).await,
    }
}

async fn run_agent_deploy(
    _client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: AgentDeployArgs,
) -> Result<()> {
    if args.ssh_target.is_empty() {
        return Err(anyhow::anyhow!("必须提供 --ssh-target 参数"));
    }

    print_header(&format!("部署 Agent 到 {}", style(&args.ssh_target).cyan()));

    let re_id = regex::Regex::new(r"^[A-Za-z0-9]{1,64}$").unwrap();
    if !re_id.is_match(&args.agent_id) {
        return Err(anyhow::anyhow!("Agent ID 非法（仅允许 1-64 位字母或数字）"));
    }

    // 创建部署目录
    let deploy_dir = args.output_dir.join(&args.agent_id);
    std::fs::create_dir_all(&deploy_dir)?;
    print_status("创建部署目录", true);

    // 如果需要，生成独立证书
    if args.generate_cert {
        print_info("生成 Agent 独立证书...");
        generate_agent_certificates(&args.agent_id, &deploy_dir.join("certs")).await?;
        print_status("生成独立证书", true);
    } else {
        // 复制默认证书
        copy_default_certificates(&deploy_dir.join("certs"))?;
        print_status("复制默认证书", true);
    }

    // 复制 Agent 二进制文件（如果提供）
    if let Some(binary_path) = &args.agent_binary {
        std::fs::copy(binary_path, deploy_dir.join("oasis-agent"))?;
        print_status("复制 Agent 二进制文件", true);
    }

    // 生成环境变量文件
    let env_file =
        generate_agent_env_file(&args.nats_url, &args.labels, &args.groups, &args.agent_id)?;
    std::fs::write(deploy_dir.join("agent.env"), env_file)?;
    print_status("生成环境变量文件", true);

    // 生成 systemd 服务文件
    let service_file = generate_systemd_service()?;
    std::fs::write(deploy_dir.join("oasis-agent.service"), service_file)?;
    print_status("生成 systemd 服务文件", true);

    // 生成安装脚本
    let install_script = generate_install_script(&args.ssh_target, &args.agent_id)?;
    let install_script_path = deploy_dir.join("install.sh");
    std::fs::write(&install_script_path, install_script)?;
    std::fs::set_permissions(
        &install_script_path,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )?;
    print_status("生成安装脚本", true);

    // 如果启用自动安装
    if args.auto_install {
        print_header("自动部署 Agent");

        // 执行自动部署
        run_agent_auto_deploy(&args.ssh_target, &args.key, &deploy_dir, &args.agent_id).await?;

        // 验证部署
        verify_agent_deployment(&args.ssh_target, &args.key).await?;

        print_status("Agent 部署完成并已验证", true);
    } else {
        print_next_step("手动部署步骤:");
        println!("  1. cd {}", deploy_dir.display());
        println!("  2. ./install.sh");
    }

    Ok(())
}

async fn run_agent_list(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: AgentListArgs,
) -> Result<()> {
    if args.target.is_empty() {
        return Err(anyhow::anyhow!("必须提供 --target 参数。"));
    }

    print_header("列出所有已连接的 Agent");

    // 获取节点列表
    let base_req = oasis_core::proto::ListAgentsRequest {
        target: Some(oasis_core::proto::SelectorExpression {
            expression: args.target.clone(),
        }),
        verbose: args.verbose,
        is_online: args.is_online,
    };
    let response = grpc_retry!(client, list_agents(base_req.clone())).await?;

    let agents = response.into_inner().agents;

    if agents.is_empty() {
        print_warning("没有已连接的 Agent");
        return Ok(());
    }

    if args.verbose {
        // 详细模式：显示 agent 信息（分离 groups、系统信息、普通标签）
        for agent in agents {
            let agent_id = agent
                .agent_id
                .as_ref()
                .map(|id| id.value.clone())
                .unwrap_or_default();

            // 状态处理
            let is_online = match agent.status {
                0 => true,  // ONLINE
                1 => false, // OFFLINE
                2 => true,  // BUSY
                _ => false,
            };

            // 解析 labels：分离 groups、系统信息、普通标签
            let mut groups: Vec<String> = Vec::new();
            let mut system_info_kv: Vec<(String, String)> = Vec::new();
            let mut user_labels_kv: Vec<(String, String)> = Vec::new();

            for (k, v) in agent.info.iter() {
                let key = k.as_str();
                if key == "__groups" {
                    groups.extend(
                        v.split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty()),
                    );
                } else if key.starts_with("__system_") {
                    let clean_key = key.strip_prefix("__system_").unwrap_or(key);
                    system_info_kv.push((clean_key.to_string(), v.clone()));
                } else {
                    // 用户标签：过滤掉所有内部标签
                    if !key.starts_with("__") {
                        user_labels_kv.push((k.clone(), v.clone()));
                    }
                }
            }

            print_header(&format!("Agent ID: {}", agent_id));

            let status_msg = if is_online { "在线" } else { "离线" };
            print_info(&format!("状态: {}", status_msg));

            if !user_labels_kv.is_empty() {
                let labels_str = user_labels_kv
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ");
                print_info(&format!("标签: {}", labels_str));
            }

            if !groups.is_empty() {
                print_info(&format!("分组: {}", groups.join(", ")));
            }

            // 系统信息 + 公共元数据
            let datetime = crate::time::format_local_ts(agent.last_heartbeat);

            // 组装系统信息项，按常见顺序展示
            let mut sys_items: Vec<(String, String)> = Vec::new();
            let push_if = |items: &mut Vec<(String, String)>,
                           key: &str,
                           label: &str,
                           map: &Vec<(String, String)>| {
                if let Some((_, v)) = map.iter().find(|(k, _)| k == key) {
                    items.push((label.to_string(), v.clone()));
                }
            };
            push_if(&mut sys_items, "hostname", "主机名", &system_info_kv);
            push_if(&mut sys_items, "primary_ip", "主IP", &system_info_kv);
            push_if(&mut sys_items, "cpu_arch", "架构", &system_info_kv);
            push_if(&mut sys_items, "cpu_cores", "CPU核数", &system_info_kv);
            push_if(&mut sys_items, "cpu_count", "CPU核数", &system_info_kv);
            push_if(
                &mut sys_items,
                "memory_total_gb",
                "内存(GB)",
                &system_info_kv,
            );
            push_if(
                &mut sys_items,
                "memory_total",
                "内存(字节)",
                &system_info_kv,
            );
            push_if(&mut sys_items, "os_name", "OS", &system_info_kv);
            push_if(&mut sys_items, "os_version", "OS版本", &system_info_kv);
            push_if(&mut sys_items, "kernel_version", "内核", &system_info_kv);
            push_if(&mut sys_items, "boot_id", "BootID", &system_info_kv);

            // 附加通用元信息
            sys_items.push(("Agent 版本".to_string(), agent.version.clone()));
            sys_items.push(("能力".to_string(), agent.capabilities.join(", ")));
            sys_items.push(("最后心跳".to_string(), datetime));

            // 转为引用切片以适配 log_operation 接口
            let sys_slice: Vec<(String, String)> = sys_items;
            let sys_ref: Vec<(&str, &str)> = sys_slice
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            log_operation("系统信息", Some(&sys_ref));
        }
    } else {
        // 简洁模式：表格显示
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec![
            Cell::new("AGENT ID").add_attribute(Attribute::Bold),
            Cell::new("状态").add_attribute(Attribute::Bold),
            Cell::new("版本").add_attribute(Attribute::Bold),
            Cell::new("分组").add_attribute(Attribute::Bold),
            Cell::new("标签").add_attribute(Attribute::Bold),
        ]);

        for agent in agents {
            let agent_id = agent
                .agent_id
                .as_ref()
                .map(|id| id.value.clone())
                .unwrap_or_default();

            // 状态处理
            let status_cell = match agent.status {
                0 => Cell::new("在线").fg(Color::Green),  // AGENT_ONLINE
                1 => Cell::new("离线").fg(Color::Red),    // AGENT_OFFLINE
                2 => Cell::new("繁忙").fg(Color::Yellow), // AGENT_BUSY
                _ => Cell::new("未知").fg(Color::Grey),
            };
            // 解析 labels：分离 groups 与普通标签
            let mut groups: Vec<String> = Vec::new();
            let mut user_labels_kv: Vec<(String, String)> = Vec::new();
            for (k, v) in agent.info.into_iter() {
                let key = k.as_str();
                if key == "__groups" {
                    groups.extend(
                        v.split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty()),
                    );
                } else {
                    user_labels_kv.push((k, v));
                }
            }

            let groups_str = if groups.is_empty() {
                "-".to_string()
            } else {
                groups.join(", ")
            };
            let mut labels = if user_labels_kv.is_empty() {
                "-".to_string()
            } else {
                user_labels_kv
                    .into_iter()
                    .filter(|(k, _)| !k.starts_with("__system_"))
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            // 限制标签长度，避免表格换行过多
            if labels.len() > 60 {
                labels.truncate(57);
                labels.push_str("...");
            }

            table.add_row(vec![
                Cell::new(agent_id),
                status_cell,
                Cell::new(agent.version),
                Cell::new(groups_str),
                Cell::new(labels),
            ]);
        }

        println!("{}", table);
    }
    Ok(())
}

async fn run_agent_remove(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: AgentRemoveArgs,
) -> Result<()> {
    print_header(&format!(
        "正在从远端移除 Agent: {}",
        style(&args.ssh_target).cyan()
    ));

    let key_arg = if let Some(ref key_path) = args.key {
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
    let script_name = format!("uninstall-{}.sh", args.ssh_target.replace(['@', ':'], "-"));
    std::fs::write(&script_name, uninstall_script)?;

    print_status(&format!("已生成卸载脚本: {}", script_name), true);
    println!();
    print_next_step("请手动执行以下命令完成移除:");
    println!(
        "  1. scp {} {} {}:/tmp/",
        key_arg, script_name, args.ssh_target
    );
    println!(
        "  2. ssh {} {} 'chmod +x /tmp/{} && sudo /tmp/{}'",
        key_arg, args.ssh_target, script_name, script_name
    );

    match grpc_retry!(
        client,
        remove_agent(RemoveAgentRequest {
            agent_id: Some(oasis_core::proto::AgentId {
                value: args.agent_id.clone(),
            }),
        })
    )
    .await
    .map_err(|e| anyhow::anyhow!("从服务器注销 Agent 失败: {}", format_grpc_error(&e)))
    {
        Ok(response) => {
            let resp = response.into_inner();
            if resp.success {
                print_status("Agent 已从服务器注销", true);
            } else {
                print_warning(&format!("注销结果: {}", resp.message));
            }
        }
        Err(e) => {
            print_warning(&format!("从服务器注销 Agent 失败: {}", e));
            print_warning("将继续进行远程卸载，但 Agent 信息可能仍存在于服务器");
        }
    }

    Ok(())
}

async fn run_agent_set(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: AgentSetArgs,
) -> Result<()> {
    print_header(&format!(
        "设置 Agent {} 标签或者分组",
        style(&args.agent_id).cyan()
    ));

    // agent_id 参数有提供
    if args.agent_id.is_empty() {
        return Err(anyhow::anyhow!("Agent ID 不能为空。"));
    }

    let mut info = HashMap::new();
    // labels 是每一个参数输入的是 k=v 的形式，和 hashmap 的方式一样
    // group 是每一个参数输入的是 xxx,xxx,xxx 的形式，需要设置成 key为__groups，value为 xxx,xxx,xxx
    // labels: KEY=VALUE 格式，使用正则校验，避免 unwrap 崩溃
    let re_label = regex::Regex::new(r"^[^=\s]+=[^\s]*$").unwrap();
    for label in args.labels.unwrap_or_default() {
        if !re_label.is_match(&label) {
            return Err(anyhow::anyhow!(format!(
                "非法标签格式: {} (应为 KEY=VALUE)",
                label
            )));
        }
        if let Some((k, v)) = label.split_once('=') {
            info.insert(k.to_string(), v.to_string());
        }
    }
    for group in args.groups.unwrap_or_default() {
        info.insert("__groups".to_string(), group);
    }

    // 调用 agent_service 的 set_info_agent 方法
    match grpc_retry!(
        client,
        set_info_agent(SetInfoAgentRequest {
            agent_id: Some(oasis_core::proto::AgentId {
                value: args.agent_id.clone()
            }),
            info: info.clone(),
        })
    )
    .await
    .map_err(|e| anyhow::anyhow!("设置失败: {}", format_grpc_error(&e)))
    {
        Ok(_) => {
            print_status("设置 Agent 标签或者分组成功", true);
        }
        Err(e) => {
            print_warning(&format!("设置 Agent 标签或者分组失败: {}", e));
        }
    }

    Ok(())
}

/// 自动执行 Agent 部署
async fn run_agent_auto_deploy(
    target: &str,
    key: &Option<PathBuf>,
    deploy_dir: &PathBuf,
    agent_id: &str,
) -> Result<()> {
    let ssh_key_args = if let Some(key_path) = key {
        vec![
            "-i",
            key_path.to_str().expect("Key path should be valid UTF-8"),
        ]
    } else {
        vec![]
    };

    // 1. 创建远程临时目录
    print_progress(1, 5, "创建远程目录");
    let mut cmd = tokio::process::Command::new("ssh");
    cmd.args(&ssh_key_args)
        .arg(target)
        .arg(format!("mkdir -p /tmp/oasis-deploy-{}", agent_id));

    let output = cmd.output().await?;
    if !output.status.success() {
        anyhow::bail!(
            "创建远程目录失败: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // 2. 使用 rsync 或 scp 复制文件
    print_progress(2, 5, "上传部署文件");

    // 尝试使用 rsync（更快更可靠）
    let mut rsync_cmd = tokio::process::Command::new("rsync");
    rsync_cmd.arg("-avz").arg("--progress");

    if let Some(key_path) = key {
        rsync_cmd.arg(format!("-e ssh -i {}", key_path.display()));
    }

    rsync_cmd
        .arg(format!("{}/", deploy_dir.display()))
        .arg(format!("{}:/tmp/oasis-deploy-{}/", target, agent_id));

    let rsync_result = rsync_cmd.output().await;

    if rsync_result.is_err()
        || !rsync_result
            .expect("rsync_result should be Ok")
            .status
            .success()
    {
        // 如果 rsync 失败，回退到 scp
        print_info("使用 scp 上传文件...");
        let mut scp_cmd = tokio::process::Command::new("scp");
        scp_cmd
            .args(&ssh_key_args)
            .arg("-r")
            .arg(format!("{}/*", deploy_dir.display()))
            .arg(format!("{}:/tmp/oasis-deploy-{}/", target, agent_id));

        let output = scp_cmd.output().await?;
        if !output.status.success() {
            anyhow::bail!("上传文件失败: {}", String::from_utf8_lossy(&output.stderr));
        }
    }

    // 3. 执行安装脚本
    print_progress(3, 5, "执行安装脚本");
    let mut cmd = tokio::process::Command::new("ssh");
    cmd.args(&ssh_key_args).arg(target).arg(format!(
        "cd /tmp/oasis-deploy-{} && sudo bash install.sh",
        agent_id
    ));

    let output = cmd.output().await?;
    if !output.status.success() {
        anyhow::bail!(
            "执行安装脚本失败: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // 4. 启动服务
    print_progress(4, 5, "启动 Agent 服务");
    let mut cmd = tokio::process::Command::new("ssh");
    cmd.args(&ssh_key_args)
        .arg(target)
        .arg("sudo systemctl start oasis-agent");

    let output = cmd.output().await?;
    if !output.status.success() {
        anyhow::bail!("启动服务失败: {}", String::from_utf8_lossy(&output.stderr));
    }

    // 5. 清理临时文件
    print_progress(5, 5, "清理临时文件");
    let mut cmd = tokio::process::Command::new("ssh");
    cmd.args(&ssh_key_args)
        .arg(target)
        .arg(format!("rm -rf /tmp/oasis-deploy-{}", agent_id));

    let _ = cmd.output().await; // 忽略清理错误

    Ok(())
}

/// 验证 Agent 部署是否成功
async fn verify_agent_deployment(target: &str, key: &Option<PathBuf>) -> Result<()> {
    print_info("验证 Agent 部署...");

    let ssh_key_args = if let Some(key_path) = key {
        vec![
            "-i",
            key_path.to_str().expect("Key path should be valid UTF-8"),
        ]
    } else {
        vec![]
    };

    // 检查服务状态
    let mut cmd = tokio::process::Command::new("ssh");
    cmd.args(&ssh_key_args)
        .arg(target)
        .arg("sudo systemctl is-active oasis-agent");

    let output = cmd.output().await?;
    let status = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if status == "active" {
        print_status("Agent 服务运行正常", true);

        // 获取最近的日志
        let mut cmd = tokio::process::Command::new("ssh");
        cmd.args(&ssh_key_args)
            .arg(target)
            .arg("sudo journalctl -u oasis-agent -n 10 --no-pager");

        let output = cmd.output().await?;
        if output.status.success() {
            print_info("最近日志:");
            println!("{}", String::from_utf8_lossy(&output.stdout));
        }

        Ok(())
    } else {
        print_warning(&format!("Agent 服务状态异常: {}", status));
        Err(anyhow::anyhow!("Agent 服务未正常运行"))
    }
}

/// 生成 Agent 独立证书
async fn generate_agent_certificates(agent_id: &str, output_dir: &PathBuf) -> Result<()> {
    use crate::certificate::CertificateGenerator;

    // 加载 CA 证书
    let ca = CertificateGenerator::load_ca(&PathBuf::from("./certs"))?;

    // 为 Agent 生成证书
    CertificateGenerator::generate_agent_certificate(agent_id, &ca, output_dir).await?;

    Ok(())
}

/// 复制默认证书
fn copy_default_certificates(output_dir: &PathBuf) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let cert_files = ["nats-ca.pem", "nats-client.pem", "nats-client-key.pem"];

    for file in &cert_files {
        let src = PathBuf::from("./certs").join(file);
        let dst = output_dir.join(file);
        if src.exists() {
            std::fs::copy(&src, &dst)?;
        }
    }

    Ok(())
}

/// 生成改进的安装脚本
fn generate_install_script(target: &str, agent_id: &str) -> Result<String> {
    let script = format!(
        r#"#!/bin/bash
# Oasis Agent 自动安装脚本
# 目标: {target}
# Agent ID: {agent_id}

set -e

echo "==========================================="
echo "  Oasis Agent 自动安装"
echo "  Agent ID: {agent_id}"
echo "==========================================="

# 检查是否以 root 权限运行
if [[ $EUID -ne 0 ]]; then
   echo "此脚本需要 root 权限运行"
   echo "请使用: sudo $0"
   exit 1
fi

echo "► 创建必要目录..."
mkdir -p /opt/oasis/agent
mkdir -p /opt/oasis/certs
mkdir -p /var/log/oasis

echo "► 安装 Agent 文件..."
if [ -f "oasis-agent" ]; then
    cp oasis-agent /opt/oasis/agent/
    chmod +x /opt/oasis/agent/oasis-agent
    echo "  ✔ Agent 二进制文件已安装"
else
    echo "  ⚠ Agent 二进制文件未找到，请手动复制"
fi

echo "► 安装配置文件..."
cp agent.env /opt/oasis/agent/
echo "  ✔ 环境变量文件已安装"

echo "► 安装证书..."
if [ -d "certs" ]; then
    cp -r certs/* /opt/oasis/certs/
    chmod 600 /opt/oasis/certs/*.pem 2>/dev/null || true
    echo "  ✔ 证书已安装"
else
    echo "  ⚠ 证书目录未找到"
fi

echo "► 安装 systemd 服务..."
cp oasis-agent.service /etc/systemd/system/
systemctl daemon-reload
echo "  ✔ systemd 服务已安装"

echo "► 启动 Agent 服务..."
systemctl enable oasis-agent
systemctl restart oasis-agent

# 等待服务启动
sleep 2

# 检查服务状态
if systemctl is-active --quiet oasis-agent; then
    echo "  ✔ Agent 服务已成功启动"
    echo ""
    echo "==========================================="
    echo "  安装完成！"
    echo "  Agent ID: {agent_id}"
    echo "  状态: 运行中"
    echo "==========================================="
    echo ""
    echo "有用的命令:"
    echo "  查看状态: systemctl status oasis-agent"
    echo "  查看日志: journalctl -u oasis-agent -f"
    echo "  重启服务: systemctl restart oasis-agent"
else
    echo "  ✗ Agent 服务启动失败"
    echo "  请检查日志: journalctl -u oasis-agent -n 50"
    exit 1
fi
"#
    );

    Ok(script)
}

/// 生成环境变量文件（包含 Agent ID）
fn generate_agent_env_file(
    nats_url: &str,
    labels: &[String],
    groups: &[String],
    agent_id: &str,
) -> Result<String> {
    let labels_str = if labels.is_empty() {
        String::new()
    } else {
        format!("\nOASIS_AGENT_LABELS={}", labels.join(","))
    };

    let groups_str = if groups.is_empty() {
        String::new()
    } else {
        format!("\nOASIS_AGENT_GROUPS={}", groups.join(","))
    };

    let env = format!(
        r#"# Oasis Agent 环境变量
# Agent ID: {agent_id}
# 生成时间: {}

# Agent 身份
OASIS_AGENT_ID={agent_id}

# NATS 配置
OASIS__NATS__URL={nats_url}

# 证书路径（部署时固定）
OASIS__TLS__CERTS_DIR=/opt/oasis/certs

# 日志配置
OASIS__TELEMETRY__LOG_LEVEL=info
OASIS__TELEMETRY__LOG_FORMAT=text
OASIS__TELEMETRY__LOG_NO_ANSI=false{labels_str}{groups_str}
"#,
        format!(
            "{} {}",
            crate::time::now_local_string(),
            chrono::Local::now().format("%Z")
        )
    );

    Ok(env)
}

/// 生成 systemd 服务文件
fn generate_systemd_service() -> Result<String> {
    let service = r#"[Unit]
Description=Oasis Agent
After=network.target
Wants=network.target

[Service]
Type=simple
ExecStart=/opt/oasis/agent/oasis-agent
EnvironmentFile=/opt/oasis/agent/agent.env
Restart=always
TimeoutStartSec=10
RestartSec=1
User=root
WorkingDirectory=/opt/oasis/agent
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"#;
    Ok(service.to_string())
}
