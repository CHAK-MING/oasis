use anyhow::Result;
use clap::Parser;
use oasis_core::proto::{ApplyFileRequest, oasis_service_client::OasisServiceClient};
use oasis_core::selector::CelSelector;

/// File management commands for distributed file operations
///
/// These commands allow you to distribute files to agents and manage the object store.
/// Files can be applied atomically with proper permissions and ownership settings.
#[derive(Parser, Debug)]
#[command(
    name = "file",
    about = "Distribute files to agents and manage object store",
    after_help = r#"Examples:
  # Apply nginx configuration to web servers
  oasis-cli file apply --src ./nginx.conf --dest /etc/nginx/nginx.conf --selector 'labels["role"] == "web"'
  
  # Apply configuration with atomic replacement and permissions
  oasis-cli file apply --src ./app.conf --dest /etc/myapp/config.conf --selector 'labels["environment"] == "prod"' --atomic --owner root:root --mode 0644
  
  # Clear all files from object store
  oasis-cli file clear"#
)]
pub enum FileArgs {
    /// Distribute a local file to target agents
    ///
    /// Uploads a local file to the object store and applies it to agents that match the CEL selector.
    /// Supports atomic replacement, file permissions, and ownership settings.
    Apply {
        /// Path to the local file to distribute
        #[arg(
            long,
            value_name = "FILE_PATH",
            help = "Path to the local file to distribute"
        )]
        src: String,
        /// CEL selector expression to target specific agents
        ///
        /// Examples:
        ///   'labels["role"] == "web"'
        ///   'labels["environment"] == "production"'
        ///   'agent_id == "agent-1"'
        #[arg(
            long,
            value_name = "CEL_EXPRESSION",
            help = "CEL selector expression to target specific agents"
        )]
        selector: String,
        /// Destination path on the target agents
        #[arg(
            long,
            value_name = "DEST_PATH",
            help = "Destination path on the target agents"
        )]
        dest: String,
        /// Expected SHA256 checksum for file validation
        ///
        /// If provided, the file will be validated against this checksum
        /// before being applied to ensure integrity.
        #[arg(
            long,
            value_name = "SHA256",
            help = "Expected SHA256 checksum (hex format) for file validation"
        )]
        sha256: Option<String>,
        /// File ownership (user:group format)
        ///
        /// Examples: 'nginx:nginx', 'root:root', 'www-data:www-data'
        #[arg(
            long,
            value_name = "USER:GROUP",
            help = "File ownership in user:group format (e.g., 'nginx:nginx')"
        )]
        owner: Option<String>,
        /// File permissions in octal format
        ///
        /// Examples: '0644', '0755', '0600'
        #[arg(
            long,
            value_name = "MODE",
            help = "File permissions in octal format (e.g., '0644')"
        )]
        mode: Option<String>,
        /// Use atomic file replacement
        ///
        /// When enabled, the file is written to a temporary location first,
        /// then atomically moved to the final destination to prevent corruption.
        #[arg(long, help = "Use atomic file replacement for safe updates")]
        atomic: bool,
    },
    /// Clear all files from the object store
    ///
    /// Removes all files from the distributed object store.
    /// This operation cannot be undone and will affect all agents.
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
            selector,
            dest,
            sha256,
            owner,
            mode,
            atomic,
        } => {
            // 本地校验选择器语法（尽早失败）
            let _selector = CelSelector::new(selector.clone())
                .map_err(|e| anyhow::anyhow!("Invalid selector: {}", e))?;

            // 依据源文件名推断对象存储名称
            let object_name = std::path::Path::new(&src)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("object.bin")
                .to_string();

            let data = tokio::fs::read(&src).await?;
            let response = client
                .apply_file(ApplyFileRequest {
                    object_name,
                    file_data: data,
                    destination_path: dest,
                    target_selector: selector,
                    expected_sha256: sha256.unwrap_or_default(),
                    owner: owner.unwrap_or_default(),
                    mode: mode.unwrap_or_default(),
                    atomic,
                })
                .await?;
            let resp = response.into_inner();

            if resp.success {
                println!(
                    "File applied successfully to {} nodes",
                    resp.applied_nodes.len()
                );
                if !resp.failed_nodes.is_empty() {
                    println!("Failed nodes: {:?}", resp.failed_nodes);
                }
            } else {
                anyhow::bail!("File apply failed: {}", resp.message);
            }
        }
        FileArgs::Clear {} => {
            let resp = client
                .clear_files(oasis_core::proto::ClearFilesRequest {})
                .await?
                .into_inner();
            println!("cleared objects: {}", resp.deleted_count);
        }
    }
    Ok(())
}
