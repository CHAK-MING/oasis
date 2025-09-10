use crate::client::format_grpc_error;
use crate::grpc_retry;
use crate::ui::{confirm_action, print_header, print_info, print_progress_bar, print_status};
use anyhow::Result;
use clap::{Parser, Subcommand};
use console::style;
use oasis_core::{
    core_types::SelectorExpression,
    proto::{
        CommitFileMsg, EmptyMsg, FileApplyRequestMsg, FileChunkMsg, FileConfigMsg, FileSpecMsg,
        GetFileHistoryRequest, RollbackFileRequest, oasis_service_client::OasisServiceClient,
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

  # 查看文件的历史版本
  oasis-cli file history --source-path ./nginx.conf

  # 回滚文件到指定版本
  oasis-cli file rollback --source-path ./nginx.conf --revision 1 --dest /etc/nginx/nginx.conf --target 'labels["role"] == "web"'

  # 清空文件仓库（对象存储）——危险操作，会提示确认
  oasis-cli file clear"#
)]
pub struct FileArgs {
    #[command(subcommand)]
    pub cmd: FileCmd,
}

#[derive(Subcommand, Debug)]
pub enum FileCmd {
    /// 下发任务
    Apply(ApplyArgs),
    /// 查看指定文件信息
    History(HistoryArgs),
    /// 回滚文件
    Rollback(RollbackArgs),
    /// 清空文件仓库
    Clear,
}

#[derive(Parser, Debug)]
pub struct ApplyArgs {
    /// 本地文件路径
    #[arg(long, value_name = "FILE_PATH", help = "本地文件路径")]
    src: String,
    /// 目标（选择器语法）
    #[arg(long, short = 't', help = "目标（选择器语法）", default_value = "all")]
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
pub struct HistoryArgs {
    /// 源文件路径
    #[arg(long, value_name = "SOURCE_PATH", help = "指定源文件路径")]
    source_path: String,
}

#[derive(Parser, Debug)]
pub struct RollbackArgs {
    /// 源文件路径（用于生成对象key）
    #[arg(long, value_name = "SOURCE_PATH", help = "要回滚的源文件路径")]
    source_path: String,
    /// 版本号（revision）
    #[arg(long, value_name = "REVISION", help = "要回滚到的版本号")]
    revision: u64,
    /// 目标路径（远端部署路径）
    #[arg(long, value_name = "DEST_PATH", help = "目标路径（远端）")]
    dest: String,
    /// 目标（选择器语法）
    #[arg(long, short = 't', help = "目标（选择器语法）", default_value = "all")]
    target: String,
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
        FileCmd::History(history) => run_file_history(&mut client, history).await,
        FileCmd::Rollback(rollback) => run_file_rollback(&mut client, rollback).await,
        FileCmd::Clear => run_file_clear(&mut client).await,
    }
}

async fn run_file_apply(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: ApplyArgs,
) -> Result<()> {
    print_header(&format!(
        "分发文件: {} -> {}",
        style(&args.src).cyan(),
        style(&args.dest).cyan()
    ));

    // 验证源文件存在
    let path = std::path::Path::new(&args.src);
    if !path.exists() {
        return Err(anyhow::anyhow!("源文件不存在: {}", args.src));
    }

    let abs_path = path.canonicalize().unwrap();

    // 获取文件信息
    let file_metadata = tokio::fs::metadata(&args.src).await?;
    let file_size = file_metadata.len();

    if file_size == 0 {
        return Err(anyhow::anyhow!("源文件为空: {}", args.src));
    }

    // 显示文件信息
    print_info(&format!("文件大小: {}", human_readable_size(file_size)));

    print_header("开始上传文件到对象存储");

    let begin = client
        .begin_file_upload(FileSpecMsg {
            source_path: abs_path.to_string_lossy().to_string(),
            size: file_size,
            checksum: args.sha256.clone().unwrap_or_default(),
            content_type: "application/octet-stream".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("开始上传失败: {}", format_grpc_error(&e)))?
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
            .await
            .map_err(|e| anyhow::anyhow!("上传分片失败: {}", format_grpc_error(&e)))?
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
        .await
        .map_err(|e| anyhow::anyhow!("提交上传失败: {}", format_grpc_error(&e)))?
        .into_inner();

    if commit_result.success {
        print_status("上传完成", true);
    } else {
        return Err(anyhow::anyhow!("文件上传失败: {}", commit_result.message));
    }

    print_header("分发文件到目标节点");
    print_info(&format!("目标: {}", args.target));
    print_info(&format!("路径: {}", args.dest));

    let base_req = FileApplyRequestMsg {
        config: Some(FileConfigMsg {
            source_path: abs_path.to_string_lossy().to_string(),
            destination_path: args.dest,
            revision: commit_result.revision,
            owner: args.owner.unwrap_or_default(),
            mode: args.mode.unwrap_or_default(),
            target: Some(SelectorExpression::new(args.target).into()),
        }),
    };
    let apply_result = grpc_retry!(client, apply_file(base_req.clone()))
        .await
        .map_err(|e| anyhow::anyhow!("分发文件失败: {}", format_grpc_error(&e)))?
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

    let base_req = EmptyMsg {};
    let clear_result = grpc_retry!(client, clear_files(base_req.clone()))
        .await?
        .into_inner();

    if clear_result.success {
        print_status(clear_result.message.as_str(), true);
    } else {
        return Err(anyhow::anyhow!("清空失败: {}", clear_result.message));
    }

    Ok(())
}

async fn run_file_history(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: HistoryArgs,
) -> Result<()> {
    print_header("文件历史信息");
    print_info(&format!("文件路径: {}", style(&args.source_path).cyan()));

    let source_path = std::path::Path::new(&args.source_path)
        .canonicalize()
        .unwrap();

    let base_req = GetFileHistoryRequest {
        source_path: source_path.to_string_lossy().to_string(),
    };
    let response = grpc_retry!(client, get_file_history(base_req.clone()))
        .await?
        .into_inner();

    if response.file_history.is_none() {
        print_info("没有找到文件历史信息");
        return Ok(());
    }

    let file_history = response.file_history.unwrap();

    print_info(&format!("文件名: {}", style(&file_history.name).green()));
    print_info(&format!(
        "当前版本: {}",
        style(file_history.current_version).yellow()
    ));
    print_info(&format!(
        "总版本数: {}",
        style(file_history.versions.len()).blue()
    ));

    if file_history.versions.is_empty() {
        print_info("没有版本历史");
        return Ok(());
    }

    // 使用表格显示版本历史
    use comfy_table::{Cell, Table, presets::UTF8_FULL};

    let mut table = Table::new();
    table.load_preset(UTF8_FULL).set_header(vec![
        Cell::new("版本").add_attribute(comfy_table::Attribute::Bold),
        Cell::new("状态").add_attribute(comfy_table::Attribute::Bold),
        Cell::new("大小").add_attribute(comfy_table::Attribute::Bold),
        Cell::new("创建时间").add_attribute(comfy_table::Attribute::Bold),
    ]);

    for version in &file_history.versions {
        let status_text = if version.is_current {
            "当前"
        } else {
            "历史"
        };

        let created_at = crate::time::format_local_ts(version.created_at);

        let mut status_cell = Cell::new(status_text);
        if version.is_current {
            status_cell = status_cell.fg(comfy_table::Color::Green);
        } else {
            status_cell = status_cell.fg(comfy_table::Color::DarkGrey);
        }

        table.add_row(vec![
            Cell::new(version.revision.to_string()),
            status_cell,
            Cell::new(human_readable_size(version.size)),
            Cell::new(created_at),
        ]);
    }

    println!("\n{}", table);

    Ok(())
}

async fn run_file_rollback(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    args: RollbackArgs,
) -> Result<()> {
    print_header(&format!(
        "回滚文件: {} -> {} (版本 {})",
        style(&args.source_path).cyan(),
        style(&args.dest).cyan(),
        style(args.revision).yellow()
    ));

    print_info(&format!("目标: {}", args.target));
    print_info(&format!("版本: {}", args.revision));

    let abs_path = std::path::Path::new(&args.source_path)
        .canonicalize()
        .unwrap();

    let request = RollbackFileRequest {
        config: Some(FileConfigMsg {
            source_path: abs_path.to_string_lossy().to_string(),
            destination_path: args.dest,
            revision: args.revision,
            owner: args.owner.unwrap_or_default(),
            mode: args.mode.unwrap_or_default(),
            target: Some(SelectorExpression::new(args.target).into()),
        }),
    };

    let base_req = request.clone();
    let response = grpc_retry!(client, rollback_file(base_req.clone()))
        .await
        .map_err(|e| anyhow::anyhow!("文件回滚失败: {}", format_grpc_error(&e)))?
        .into_inner();

    if response.success {
        print_status("文件回滚成功", true);
        print_info(&response.message);
    } else {
        print_status("文件回滚失败", false);
        return Err(anyhow::anyhow!("文件回滚失败: {}", response.message));
    }

    Ok(())
}

fn human_readable_size(bytes: u64) -> String {
    use bytesize::ByteSize;
    ByteSize(bytes).to_string()
}
