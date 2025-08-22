use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tokio::{fs, io::AsyncWriteExt};

pub struct FileApplyHandler;

#[derive(Debug, Clone)]
pub struct FileApplyRequest {
    pub path: String,
    pub content_b64: String,
    pub permissions: u32,
    pub owner: Option<String>,
    pub group: Option<String>,
}

impl FileApplyHandler {
    pub fn new() -> Self {
        Self
    }

    pub async fn apply_with_roots(
        &self,
        task: &FileApplyRequest,
        allowed_roots: &[PathBuf],
    ) -> Result<()> {
        if allowed_roots.is_empty() {
            bail!("file-apply denied: no allowed roots configured");
        }

        // 1) 解析并校验目标路径，防止路径穿越
        let target_path = PathBuf::from(&task.path);
        let target_abs = canonicalize_under_roots(&target_path, allowed_roots)
            .with_context(|| format!("path not allowed: {}", task.path))?;

        // 2) 解码内容
        let content = general_purpose::STANDARD
            .decode(&task.content_b64)
            .context("decode base64 content")?;

        // 3) 原子写入：写到同目录临时文件，再 rename
        let parent = target_abs
            .parent()
            .ok_or_else(|| anyhow::anyhow!("invalid target path"))?;
        fs::create_dir_all(parent).await.ok();
        let temp_path = parent.join(format!(
            ".{}.tmp",
            target_abs
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("file")
        ));

        {
            let mut f = fs::File::create(&temp_path)
                .await
                .context("create temp file")?;
            f.write_all(&content).await.context("write temp file")?;
            f.flush().await.ok();
        }

        // 设置权限
        let perms = std::fs::Permissions::from_mode(task.permissions);
        fs::set_permissions(&temp_path, perms.clone())
            .await
            .context("set permissions on temp file")?;

        // 原子替换
        fs::rename(&temp_path, &target_abs)
            .await
            .context("atomic rename temp -> target")?;

        // 可选：设置所有者/组（如提供），放在阻塞线程避免阻塞 runtime
        if task.owner.is_some() || task.group.is_some() {
            let owner = task.owner.clone();
            let group = task.group.clone();
            let path_for_chown = target_abs.clone();
            tokio::task::spawn_blocking(move || set_owner_group(&path_for_chown, owner, group))
                .await
                .map_err(|e| anyhow::anyhow!("chown join error: {}", e))??;
        }

        Ok(())
    }
}

fn try_canon(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

fn canonicalize_under_roots(path: &Path, allowed_roots: &[PathBuf]) -> Result<PathBuf> {
    // 如果是相对路径，则挂到第一个根目录下
    let candidate = if path.is_absolute() {
        try_canon(path)
    } else {
        let base = allowed_roots
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("no roots"))?;
        try_canon(&base.join(path))
    };

    let candidate_canon = try_canon(&candidate);
    for root in allowed_roots {
        let root_canon = try_canon(root);
        if candidate_canon.starts_with(&root_canon) {
            return Ok(candidate_canon);
        }
    }
    bail!("target path is outside of allowed roots")
}

#[cfg(target_family = "unix")]
fn set_owner_group(path: &Path, owner: Option<String>, group: Option<String>) -> Result<()> {
    use nix::unistd::{chown, Group, User};
    let uid = match owner {
        Some(name) => User::from_name(&name)
            .map_err(|e| anyhow::anyhow!("lookup user: {}", e))?
            .map(|u| u.uid),
        None => None,
    };
    let gid = match group {
        Some(name) => Group::from_name(&name)
            .map_err(|e| anyhow::anyhow!("lookup group: {}", e))?
            .map(|g| g.gid),
        None => None,
    };
    chown(path, uid, gid).map_err(|e| anyhow::anyhow!("chown: {}", e))?;
    Ok(())
}
