use crate::ui::{confirm_action, print_header, print_info, print_progress_bar, print_status};
use anyhow::Result;
use clap::{Parser, Subcommand};
use console::style;
use oasis_core::{
    core_types::SelectorExpression,
    proto::{
        CommitFileMsg, EmptyMsg, FileApplyConfigMsg, FileApplyRequestMsg, FileChunkMsg,
        FileSpecMsg, oasis_service_client::OasisServiceClient,
    },
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
  oasis-cli file apply --src ./app.conf --dest /etc/myapp/config.conf --target 'labels["environment"] == "prod"' --owner root:root --mode 0644

  # 指定多个 agent ID
  oasis-cli file apply --src ./config.conf --dest /etc/config.conf --target 'agent-1,agent-2,agent-3'

  # 清空文件仓库（对象存储）——危险操作，会提示确认
  oasis-cli file clear"#
)]
pub struct FileArgs {
    #[command(subcommand)]
    pub cmd: FileCmd,
}

#[derive(Subcommand, Debug)]
pub enum FileCmd {
    Apply(ApplyArgs),
    Clear,
}

#[derive(Parser, Debug)]
pub struct ApplyArgs {
    /// 本地文件路径
    #[arg(long, value_name = "FILE_PATH", help = "本地文件路径")]
    src: String,
    /// 目标（选择器语法）
    #[arg(long, short = 't', help = "目标（选择器语法）")]
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
}

#[derive(Parser, Debug)]
pub struct ClearArgs {}

/// 执行 `file` 子命令
pub async fn run_file(
    mut client: OasisServiceClient<tonic::transport::Channel>,
    args: FileArgs,
) -> Result<()> {
    match args.cmd {
        FileCmd::Apply(apply) => run_file_apply(&mut client, apply).await,
        FileCmd::Clear => run_file_clear(&mut client).await,
    }
}

async fn run_file_apply(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: ApplyArgs,
) -> Result<()> {
    if args.target.is_empty() {
        return Err(anyhow::anyhow!("必须提供 --target 参数。"));
    }

    print_header(&format!(
        "分发文件: {} -> {}",
        style(&args.src).cyan(),
        style(&args.dest).cyan()
    ));

    // 验证源文件存在
    if !std::path::Path::new(&args.src).exists() {
        return Err(anyhow::anyhow!("源文件不存在: {}", args.src));
    }

    // 获取文件信息
    let file_metadata = tokio::fs::metadata(&args.src).await?;
    let file_size = file_metadata.len();

    if file_size == 0 {
        return Err(anyhow::anyhow!("源文件为空: {}", args.src));
    }

    // 显示文件信息
    print_info(&format!("文件大小: {}", human_readable_size(file_size)));

    // 依据源文件名推断对象存储名称
    let name = std::path::Path::new(&args.src)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("object.bin")
        .to_string();

    print_header("开始上传文件到对象存储");

    let begin = client
        .begin_file_upload(FileSpecMsg {
            // 用FileSpecMsg而不是BeginFileUploadRequest
            name: name.clone(),
            size: file_size,
            checksum: args.sha256.clone().unwrap_or_default(),
            content_type: "application/octet-stream".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        })
        .await?
        .into_inner();
    let upload_id = begin.upload_id;

    // 打开文件并准备上传
    let mut file = tokio::fs::File::open(&args.src).await?;
    let mut offset: u64 = begin.received_bytes;
    if offset > 0 {
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        print_info(&format!(
            "从偏移量 {} 继续上传",
            human_readable_size(offset)
        ));
    }

    // 分片上传：遵循服务端建议的分片大小
    let chunk_size = (begin.chunk_size as usize).clamp(64 * 1024, 4 * 1024 * 1024);
    let mut buf = vec![0u8; chunk_size];
    let mut chunk_count = 0;
    let start_time = std::time::Instant::now();

    loop {
        let n = tokio::io::AsyncReadExt::read(&mut file, &mut buf).await?;
        if n == 0 {
            break;
        }

        // 严格限制单次发送不超过声明的总大小
        let remaining = (file_size - offset).min(n as u64) as usize;
        if remaining == 0 {
            break;
        }
        let chunk_data = buf[..remaining].to_vec();
        let chunk_resp = client
            .upload_file_chunk(FileChunkMsg {
                upload_id: upload_id.clone(),
                offset,
                data: chunk_data,
            })
            .await?
            .into_inner();

        offset = chunk_resp.received_bytes;
        chunk_count += 1;

        // 计算上传速度和剩余时间
        let elapsed = start_time.elapsed();
        let speed = if elapsed.as_secs() > 0 {
            offset / elapsed.as_secs()
        } else {
            0
        };

        let remaining_bytes = file_size.saturating_sub(offset);
        let eta = if speed > 0 {
            remaining_bytes / speed
        } else {
            0
        };

        // 显示上传进度（每10个分片或最后显示一次）
        if chunk_count % 10 == 0 || offset >= file_size {
            print_progress_bar(
                offset as usize,
                file_size as usize,
                &format!("上传中 ({} MB/s, 剩余 {}s)", speed / (1024 * 1024), eta),
            );
        }
    }

    print_info("提交文件上传...");
    let commit_result = client
        .commit_file_upload(CommitFileMsg {
            upload_id,
            verify_checksum: true,
        })
        .await?
        .into_inner();

    if commit_result.success {
        print_status("上传完成", true);
    } else {
        return Err(anyhow::anyhow!("文件上传失败: {}", commit_result.message));
    }

    print_header("分发文件到目标节点");
    print_info(&format!("目标: {}", args.target));
    print_info(&format!("路径: {}", args.dest));

    let apply_result = client
        .apply_file(FileApplyRequestMsg {
            config: Some(FileApplyConfigMsg {
                name,
                destination_path: args.dest,
                owner: args.owner.unwrap_or_default(),
                mode: args.mode.unwrap_or_default(),
                target: Some(SelectorExpression::new(args.target).into()),
            }),
        })
        .await?
        .into_inner();

    if apply_result.success {
        print_status("文件分发成功", true);
    } else {
        print_status("文件分发失败", false);
        return Err(anyhow::anyhow!("文件分发失败: {}", apply_result.message));
    }

    Ok(())
}

async fn run_file_clear(client: &mut OasisServiceClient<tonic::transport::Channel>) -> Result<()> {
    print_header("清空文件仓库");

    if !confirm_action("确定要清空所有文件吗？此操作不可恢复。", true) {
        print_info("操作已取消");
        return Ok(());
    }

    let clear_result = client.clear_files(EmptyMsg {}).await?.into_inner();

    if clear_result.success {
        print_status(clear_result.message.as_str(), true);
    } else {
        return Err(anyhow::anyhow!("清空失败: {}", clear_result.message));
    }

    Ok(())
}

fn human_readable_size(bytes: u64) -> String {
    use bytesize::ByteSize;
    ByteSize(bytes).to_string()
}
