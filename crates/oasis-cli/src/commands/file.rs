use anyhow::Result;
use clap::Parser;
use console::style;
use oasis_core::proto::{
    oasis_service_client::OasisServiceClient, task_target_msg, ApplyFileRequest, TaskTargetMsg,
};

/// 分发文件到目标 Agent 并管理对象存储
#[derive(Parser, Debug)]
#[command(
    name = "file",
    about = "分发文件到 Agent 并管理对象存储",
    after_help = r#"示例：
  # 向 Web 服务器分发 nginx 配置
  oasis-cli file apply --src ./nginx.conf --dest /etc/nginx/nginx.conf --target 'labels["role"] == "web"'

  # 原子替换并设置权限/属主
  oasis-cli file apply --src ./app.conf --dest /etc/myapp/config.conf --target 'labels["environment"] == "prod"' --atomic --owner root:root --mode 0644

  # 指定多个 agent ID
  oasis-cli file apply --src ./config.conf --dest /etc/config.conf --target agent-1,agent-2,agent-3

  # 清空对象存储
  oasis-cli file clear"#
)]
pub enum FileArgs {
    /// 分发本地文件到匹配的 Agent
    Apply {
        /// 本地文件路径
        #[arg(long, value_name = "FILE_PATH", help = "本地文件路径")]
        src: String,
        /// 目标（选择器语法）
        #[arg(long, short = 't', value_name = "TARGET", help = "目标（选择器语法）")]
        target: String,
        /// 目标机器上的目标路径
        #[arg(long, value_name = "DEST_PATH", help = "目标路径（远端）")]
        dest: String,
        /// 期望的 SHA256 校验（可选）
        #[arg(long, value_name = "SHA256", help = "可选：文件校验 SHA256（hex）")]
        sha256: Option<String>,
        /// 目标文件属主（user:group）
        #[arg(
            long,
            value_name = "USER:GROUP",
            help = "属主，格式 user:group，如 'nginx:nginx'"
        )]
        owner: Option<String>,
        /// 目标文件权限（八进制）
        #[arg(long, value_name = "MODE", help = "权限（八进制），如 '0644'")]
        mode: Option<String>,
        /// 使用原子替换
        #[arg(long, help = "启用原子替换，防止中途损坏")]
        atomic: bool,
    },
    /// 清空对象存储
    Clear {},
}

/// 执行 `file` 子命令
pub async fn run_file(
    mut client: OasisServiceClient<tonic::transport::Channel>,
    args: FileArgs,
) -> Result<()> {
    match args {
        FileArgs::Apply {
            src,
            target,
            dest,
            sha256,
            owner,
            mode,
            atomic,
        } => {
            if target.is_empty() {
                return Err(anyhow::anyhow!("必须提供 --target 参数。"));
            }

            println!(
                "{} `{}` -> `{}`",
                style("分发文件:").bold(),
                style(&src).cyan(),
                style(&dest).cyan()
            );

            let target_msg = TaskTargetMsg {
                target: Some(task_target_msg::Target::Selector(target.clone())),
            };

            // 依据源文件名推断对象存储名称
            let object_name = std::path::Path::new(&src)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("object.bin")
                .to_string();

            let data = tokio::fs::read(&src).await?;
            println!("  {} {}", style("✔").green(), style("读取本地文件").dim());

            let response = client
                .apply_file(ApplyFileRequest {
                    object_name,
                    file_data: data,
                    destination_path: dest,
                    expected_sha256: sha256.unwrap_or_default(),
                    owner: owner.unwrap_or_default(),
                    mode: mode.unwrap_or_default(),
                    atomic,
                    target: Some(target_msg),
                })
                .await?;
            let resp = response.into_inner();

            if resp.success {
                println!(
                    "  {} {} {}",
                    style("✔").green(),
                    style("文件已成功下发到").dim(),
                    style(format!("{} 个节点", resp.applied_agents.len()))
                        .dim()
                        .bold()
                );
                if !resp.failed_agents.is_empty() {
                    let ids = resp
                        .failed_agents
                        .into_iter()
                        .map(|id| id.value)
                        .collect::<Vec<_>>()
                        .join(", ");
                    println!(
                        "  {} {}: {}",
                        style("✖").red(),
                        style("失败节点").red(),
                        style(ids).red()
                    );
                }
            } else {
                anyhow::bail!("文件分发失败: {}", resp.message);
            }
        }
        FileArgs::Clear {} => {
            println!("{}", style("正在清空对象存储...").bold());

            let resp = client
                .clear_files(oasis_core::proto::ClearFilesRequest {})
                .await?
                .into_inner();

            println!(
                "{} {} {}",
                style("✔").green(),
                style("操作完成，已删除对象数量:").dim(),
                style(resp.deleted_count).bold()
            );
        }
    }
    Ok(())
}
