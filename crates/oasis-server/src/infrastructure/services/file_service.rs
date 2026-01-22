//! File Service - 直接管理文件数据
//! 使用 oasis-core 的统一类型，提供简洁的文件管理接口

use async_nats::jetstream::{Context, object_store::ObjectStore};
use oasis_core::core_types::AgentId;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::infrastructure::file_lock::FileLockManager;
use backon::Retryable;
use futures::StreamExt;
use oasis_core::backoff::{fast_backoff, network_publish_backoff};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use oasis_core::{FILES_SUBJECT_PREFIX, file_types::*};

pub struct FileService {
    jetstream: Arc<Context>,
    lock_manager: FileLockManager,
}

impl FileService {
    pub async fn new(jetstream: Arc<Context>) -> Result<Self> {
        Self::ensure_object_store(&jetstream).await?;
        let lock_manager = FileLockManager::new(jetstream.clone()).await?;

        Ok(Self {
            jetstream,
            lock_manager,
        })
    }

    /// 确保对象存储已创建
    async fn ensure_object_store(js: &Context) -> Result<()> {
        match js
            .get_object_store(oasis_core::constants::JS_OBJ_ARTIFACTS)
            .await
        {
            Ok(_) => {
                debug!(
                    "Object store {} already exists",
                    oasis_core::constants::JS_OBJ_ARTIFACTS
                );
                Ok(())
            }
            Err(_) => {
                error!(
                    "Failed to get object store: {}",
                    oasis_core::constants::JS_OBJ_ARTIFACTS
                );
                Err(CoreError::Internal {
                    message: format!(
                        "Failed to get object store: {}",
                        oasis_core::constants::JS_OBJ_ARTIFACTS
                    ),
                    severity: ErrorSeverity::Error,
                })
            }
        }
    }

    /// 上传文件到对象存储 - 返回统一类型的结果
    async fn upload_internal(
        &self,
        source_path: &String,
        data: Vec<u8>,
    ) -> Result<FileOperationResult> {
        debug!("Uploading file: {} ({} bytes)", source_path, data.len());

        let lock = self.lock_manager.acquire_lock(source_path).await?;

        let result = async {
            let mut path_hasher = Sha256::new();
            path_hasher.update(source_path.as_bytes());
            let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
            let filename = std::path::Path::new(source_path)
                .file_name()
                .ok_or_else(|| CoreError::file_error(source_path, "no filename in path"))?
                .to_string_lossy();
            let revision = chrono::Utc::now().timestamp() as u64;
            let object_key = format!("{}/{}.v{}", path_hash, filename, revision);

            let mut hasher = Sha256::new();
            hasher.update(&data);
            let checksum = format!("{:x}", hasher.finalize());

            let store = self.get_object_store().await?;

            let put_res = (|| async {
                store
                    .put(object_key.as_str(), &mut data.as_slice())
                    .await
                    .map_err(|e| CoreError::Internal {
                        message: format!("Failed to upload file: {}", e),
                        severity: ErrorSeverity::Error,
                    })
            })
            .retry(&fast_backoff().build())
            .await;

            match put_res {
                Ok(info) => {
                    info!(
                        "Successfully uploaded file: {} -> {} (nuid: {}, {} bytes, SHA256: {})",
                        source_path, object_key, info.nuid, info.size, checksum
                    );

                    Ok(FileOperationResult::success(
                        format!(
                            "File uploaded successfully: {} -> {} (nuid: {}, {} bytes, SHA256: {})",
                            source_path, object_key, info.nuid, info.size, checksum
                        ),
                        revision,
                    ))
                }
                Err(e) => {
                    error!("Failed to upload file {}: {}", source_path, e);
                    Err(e)
                }
            }
        }
        .await;

        lock.release().await.ok();
        result
    }

    /// 应用到对应的 agents
    async fn apply_internal(
        &self,
        config: &oasis_core::proto::FileConfigMsg,
        agent_ids: Vec<AgentId>,
    ) -> Result<FileOperationResult> {
        info!(
            "Applying file {} to {} agents: {:?}",
            config.source_path,
            agent_ids.len(),
            agent_ids
        );

        // 并行向每个 Agent 发送文件应用任务
        let concurrency_limit = 32usize;
        let futs = agent_ids.into_iter().map(|aid| async move {
            let res = self.send_file_apply_task(config, &aid).await;
            (aid, res)
        });

        use futures::stream;
        let mut applied_agents = Vec::new();
        let mut failed_agents = Vec::new();

        stream::iter(futs)
            .buffer_unordered(concurrency_limit)
            .for_each(|(agent_id, res)| {
                if res.is_ok() {
                    applied_agents.push(agent_id.clone());
                    info!("Sent file apply task to agent: {}", agent_id);
                } else if let Err(e) = res {
                    failed_agents.push(agent_id.clone());
                    error!(
                        "Failed to send file apply task to agent {}: {}",
                        agent_id, e
                    );
                }
                std::future::ready(())
            })
            .await;

        let message = if failed_agents.is_empty() {
            format!(
                "File {} sent to {} agents successfully",
                config.source_path,
                applied_agents.len()
            )
        } else {
            format!(
                "File {} sent to {}/{} agents (failed: {})",
                config.source_path,
                applied_agents.len(),
                applied_agents.len() + failed_agents.len(),
                failed_agents.len()
            )
        };

        let success = failed_agents.is_empty();

        // 成功后更新当前版本指针
        if success {
            if let Err(e) = self
                .set_active_revision(&config.source_path, config.revision)
                .await
            {
                warn!("Failed to update active revision pointer: {}", e);
            }
        }

        Ok(FileOperationResult {
            success,
            message,
            revision: config.revision,
        })
    }

    async fn send_file_apply_task(
        &self,
        config: &oasis_core::proto::FileConfigMsg,
        agent_id: &AgentId,
    ) -> Result<()> {
        // 序列化配置为 protobuf
        use prost::Message;
        let data = config.encode_to_vec();

        // 构建 subject：files.{agent_id}
        let subject = format!("{}.{}", FILES_SUBJECT_PREFIX, agent_id);

        // 设置去重头部
        let mut headers = async_nats::HeaderMap::new();
        let dedupe_key = format!(
            "file-{}@{}@{}",
            config.source_path, config.revision, agent_id
        );
        headers.insert("Nats-Msg-Id", dedupe_key);

        // 发布并等待 ACK
        (|| async {
            let ack = self
                .jetstream
                .publish_with_headers(subject.clone(), headers.clone(), data.clone().into())
                .await
                .map_err(|e| CoreError::Internal {
                    message: format!("Failed to publish file task: {}", e),
                    severity: ErrorSeverity::Error,
                })?;
            ack.await.map_err(|e| CoreError::Internal {
                message: format!("Failed to confirm file task publish: {}", e),
                severity: ErrorSeverity::Error,
            })
        })
        .retry(&network_publish_backoff().build())
        .await?;

        debug!("Published file apply task to subject: {}", subject);
        Ok(())
    }

    /// 清空所有文件
    async fn clear_all_internal(&self) -> Result<usize> {
        debug!("Clearing all files from object store");

        let store = self.get_object_store().await?;

        // 列出所有对象
        let mut count = 0usize;
        let mut objects = Vec::new();

        // 收集所有对象名称
        let mut list = store.list().await.map_err(|e| CoreError::Internal {
            message: format!("Failed to list objects: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        use futures::StreamExt;
        while let Some(result) = list.next().await {
            match result {
                Ok(info) => {
                    objects.push(info.name.clone());
                }
                Err(e) => {
                    warn!("Error listing object: {}", e);
                }
            }
        }

        // 删除所有对象
        for name in objects {
            match store.delete(&name).await {
                Ok(_) => {
                    debug!("Deleted object: {}", name);
                    count += 1;
                }
                Err(e) => {
                    warn!("Failed to delete object {}: {}", name, e);
                }
            }
        }

        info!("Cleared {} files from object store", count);
        Ok(count)
    }

    /// 获取对象存储实例
    async fn get_object_store(&self) -> Result<ObjectStore> {
        self.jetstream
            .get_object_store(oasis_core::constants::JS_OBJ_ARTIFACTS)
            .await
            .map_err(|e| CoreError::Internal {
                message: format!("Failed to get object store: {}", e),
                severity: ErrorSeverity::Error,
            })
    }

    /// 更新指定源文件的当前版本指针（写入 `{path_hash}/{filename}.current` 对象，内容为 revision）
    async fn set_active_revision(&self, source_path: &str, revision: u64) -> Result<()> {
        let mut path_hasher = Sha256::new();
        path_hasher.update(source_path.as_bytes());
        let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
        let filename = std::path::Path::new(source_path)
            .file_name()
            .ok_or_else(|| CoreError::file_error(source_path, "no filename in path"))?
            .to_string_lossy();
        let pointer_key = format!("{}/{}.current", path_hash, filename);

        let content = revision.to_string().into_bytes();
        let store = self.get_object_store().await?;
        store
            .put(pointer_key.as_str(), &mut content.as_slice())
            .await
            .map_err(|e| CoreError::Internal {
                message: format!("Failed to update active revision pointer: {}", e),
                severity: ErrorSeverity::Error,
            })?;
        Ok(())
    }

    /// 读取指定源文件的当前版本指针（从 `{path_hash}/{filename}.current` 读取 revision）
    async fn get_active_revision(&self, source_path: &str) -> Result<Option<u64>> {
        let mut path_hasher = Sha256::new();
        path_hasher.update(source_path.as_bytes());
        let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
        let filename = std::path::Path::new(source_path)
            .file_name()
            .ok_or_else(|| CoreError::file_error(source_path, "no filename in path"))?
            .to_string_lossy();
        let pointer_key = format!("{}/{}.current", path_hash, filename);

        let store = self.get_object_store().await?;
        match store.get(pointer_key.as_str()).await {
            Ok(mut obj) => {
                let mut buf = Vec::new();
                use tokio::io::AsyncReadExt;
                obj.read_to_end(&mut buf)
                    .await
                    .map_err(|e| CoreError::Internal {
                        message: format!("Failed to read active revision pointer: {}", e),
                        severity: ErrorSeverity::Error,
                    })?;
                let s = String::from_utf8_lossy(&buf);
                let rev = s.trim().parse::<u64>().ok();
                Ok(rev)
            }
            Err(_) => Ok(None),
        }
    }
}

impl FileService {
    /// 上传文件到Object Store
    pub async fn upload(&self, source_path: &String, data: Vec<u8>) -> Result<FileOperationResult> {
        self.upload_internal(source_path, data).await
    }

    /// 下发文件到对应的 agents
    pub async fn apply(
        &self,
        config: &oasis_core::proto::FileConfigMsg,
        agent_ids: Vec<AgentId>,
    ) -> Result<FileOperationResult> {
        self.apply_internal(config, agent_ids).await
    }

    /// 清空所有文件
    pub async fn clear_all(&self) -> Result<usize> {
        self.clear_all_internal().await
    }

    /// 获取特定文件的历史版本
    pub async fn get_file_history(&self, source_path: &str) -> Result<Option<FileHistory>> {
        debug!("Getting file history for: {}", source_path);

        // 生成路径hash和文件名
        let mut path_hasher = Sha256::new();
        path_hasher.update(source_path.as_bytes());
        let path_hash = &format!("{:x}", path_hasher.finalize())[..8];
        let filename = std::path::Path::new(source_path)
            .file_name()
            .ok_or_else(|| CoreError::file_error(source_path, "no filename in path"))?
            .to_string_lossy();

        let store = self.get_object_store().await?;

        // 使用 list() 获取所有对象，然后过滤出我们关心的文件
        let mut list = store.list().await.map_err(|e| CoreError::Internal {
            message: format!("Failed to list objects: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        let mut versions = Vec::new();
        let mut current_version = None;

        // 构建匹配模式：{path_hash}/{filename}.v*
        let pattern_prefix = format!("{}/{}.v", path_hash, filename);

        // 收集所有版本信息
        while let Some(result) = list.next().await {
            match result {
                Ok(object_info) => {
                    // 只处理匹配我们文件模式的对象
                    if object_info.name.starts_with(&pattern_prefix) {
                        // 从对象名称中提取时间戳
                        let timestamp_str = object_info
                            .name
                            .strip_prefix(&pattern_prefix)
                            .unwrap_or("0");
                        let timestamp = timestamp_str.parse::<i64>().unwrap_or(0);

                        let version = FileVersion {
                            name: filename.to_string(),
                            revision: timestamp as u64, // 使用时间戳作为版本号
                            size: object_info.size as u64,
                            checksum: object_info.digest.unwrap_or_default(),
                            created_at: object_info
                                .modified
                                .map(|t| t.unix_timestamp())
                                .unwrap_or(timestamp),
                            is_current: false, // 稍后确定当前版本
                        };

                        versions.push(version);
                    }
                }
                Err(e) => {
                    warn!("Error getting object info: {}", e);
                }
            }
        }

        if versions.is_empty() {
            return Ok(None);
        }

        // 按时间戳排序（最新的在前）
        versions.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        // 读取当前指针；若不存在则默认最新为当前
        let active_revision = self.get_active_revision(source_path).await?;
        if let Some(active) = active_revision {
            for v in &mut versions {
                v.is_current = v.revision == active;
            }
            current_version = Some(active);
        } else if let Some(latest_version) = versions.first_mut() {
            latest_version.is_current = true;
            current_version = Some(latest_version.revision);
        }

        let file_history = FileHistory {
            name: filename.to_string(),
            versions,
            current_version: current_version.unwrap_or(0),
        };

        Ok(Some(file_history))
    }

    /// 回滚文件到指定版本
    pub async fn rollback_file(
        &self,
        config: &oasis_core::proto::FileConfigMsg,
        agent_ids: Vec<AgentId>,
    ) -> Result<FileOperationResult> {
        debug!(
            "Rolling back file {} to revision {}",
            config.source_path, config.revision
        );

        // 下发到对应的 Agent
        self.apply_internal(config, agent_ids).await?;

        // 更新当前版本指针为回滚到的版本
        if let Err(e) = self
            .set_active_revision(&config.source_path, config.revision)
            .await
        {
            warn!(
                "Failed to update active revision pointer after rollback: {}",
                e
            );
        }

        info!(
            "Successfully rolled back file {} to revision {}",
            config.source_path, config.revision
        );
        Ok(FileOperationResult::success(
            format!(
                "File {} rolled back to revision {} and redeployed to target: {}",
                config.source_path,
                config.revision,
                config
                    .target
                    .as_ref()
                    .map(|t| t.expression.as_str())
                    .unwrap_or("")
            ),
            config.revision,
        ))
    }

    pub async fn gc_old_versions(
        &self,
        source_path: Option<String>,
        keep_versions: u32,
        keep_days: u32,
    ) -> Result<usize> {
        debug!(
            "GC: source_path={:?}, keep_versions={}, keep_days={}",
            source_path, keep_versions, keep_days
        );

        let store = self.get_object_store().await?;
        let mut list = store.list().await.map_err(|e| CoreError::Internal {
            message: format!("Failed to list objects: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        let now_ts = chrono::Utc::now().timestamp();
        let mut deleted = 0;

        use std::collections::HashMap;
        
        let mut file_groups: HashMap<
            (String, String),
            Vec<async_nats::jetstream::object_store::ObjectInfo>,
        > = HashMap::new();
        let mut pointer_objects: HashMap<(String, String), async_nats::jetstream::object_store::ObjectInfo> = HashMap::new();

        while let Some(result) = list.next().await {
            if let Ok(info) = result {
                let parts: Vec<&str> = info.name.split('/').collect();
                if parts.len() >= 2 {
                    let path_hash = parts[0].to_string();
                    let filename_part = parts[1];
                    
                    if filename_part.ends_with(".current") {
                        let filename = filename_part.strip_suffix(".current").unwrap_or(filename_part).to_string();
                        pointer_objects.insert((path_hash, filename), info);
                    } else if let Some(base_name) = filename_part.split(".v").next() {
                        let filename = base_name.to_string();
                        file_groups
                            .entry((path_hash, filename))
                            .or_default()
                            .push(info);
                    }
                }
            }
        }

        for ((path_hash, filename), mut versions) in file_groups {
            if let Some(ref sp) = source_path {
                let mut path_hasher = Sha256::new();
                path_hasher.update(sp.as_bytes());
                let target_hash = &format!("{:x}", path_hasher.finalize())[..8];
                if path_hash != target_hash {
                    continue;
                }
            }

            versions.sort_by_key(|v| std::cmp::Reverse(v.modified));

            let pointer_key = (path_hash.clone(), filename.clone());
            let mut active_revision: Option<u64> = None;
            let mut needs_pointer_repair = false;

            if let Some(pointer_info) = pointer_objects.get(&pointer_key) {
                match store.get(&pointer_info.name).await {
                    Ok(mut obj) => {
                        let mut buf = Vec::new();
                        use tokio::io::AsyncReadExt;
                        if let Ok(_) = obj.read_to_end(&mut buf).await {
                            let s = String::from_utf8_lossy(&buf);
                            active_revision = s.trim().parse::<u64>().ok();
                        }
                    }
                    Err(_) => {
                        warn!("GC: Failed to read pointer {}", pointer_info.name);
                    }
                }
            }

            let protected_revision: u64;
            if let Some(active) = active_revision {
                let exists = versions.iter().any(|v| {
                    v.name.rsplit(".v").next()
                        .and_then(|r| r.parse::<u64>().ok())
                        .map(|r| r == active)
                        .unwrap_or(false)
                });
                
                if exists {
                    protected_revision = active;
                } else {
                    warn!("GC: Pointer for {}/{} points to non-existent revision {}", path_hash, filename, active);
                    needs_pointer_repair = true;
                    protected_revision = versions.first()
                        .and_then(|v| v.name.rsplit(".v").next().and_then(|r| r.parse::<u64>().ok()))
                        .unwrap_or(0);
                }
            } else {
                if !versions.is_empty() {
                    needs_pointer_repair = true;
                    protected_revision = versions.first()
                        .and_then(|v| v.name.rsplit(".v").next().and_then(|r| r.parse::<u64>().ok()))
                        .unwrap_or(0);
                } else {
                    continue;
                }
            }

            // Build revisions_sorted for the helper
            let revisions_sorted: Vec<(u64, i64)> = versions.iter()
                .filter_map(|v| {
                    let rev = v.name.rsplit(".v").next().and_then(|r| r.parse::<u64>().ok())?;
                    let modified_ts = v.modified.as_ref().map(|t| t.unix_timestamp()).unwrap_or(0);
                    Some((rev, modified_ts))
                })
                .collect();

            let keep_set = compute_gc_keep_set(
                revisions_sorted,
                protected_revision,
                keep_versions,
                keep_days,
                now_ts,
            );

            for version in &versions {
                if let Some(rev) = version.name.rsplit(".v").next().and_then(|r| r.parse::<u64>().ok()) {
                    if !keep_set.contains(&rev) {
                        match store.delete(&version.name).await {
                            Ok(_) => {
                                debug!("GC: deleted {}", version.name);
                                deleted += 1;
                            }
                            Err(e) => {
                                warn!("GC: failed to delete {}: {}", version.name, e);
                            }
                        }
                    }
                }
            }

            if needs_pointer_repair {
                let pointer_key_str = format!("{}/{}.current", path_hash, filename);
                let content = protected_revision.to_string().into_bytes();
                match store.put(pointer_key_str.as_str(), &mut content.as_slice()).await {
                    Ok(_) => {
                        info!("GC: repaired pointer {} -> {}", pointer_key_str, protected_revision);
                    }
                    Err(e) => {
                        warn!("GC: failed to repair pointer {}: {}", pointer_key_str, e);
                    }
                }
            }
        }

        info!("GC: deleted {} old file versions", deleted);
        Ok(deleted)
    }
}

/// Computes which revisions to keep during garbage collection.
///
/// # Arguments
/// * `revisions_sorted` - List of (revision, modified_timestamp) tuples, sorted newest-first
/// * `active_revision` - The currently active revision (must always be kept)
/// * `keep_versions` - Number of newest revisions to keep (minimum 1)
/// * `keep_days` - Number of days to keep revisions (0 = disabled)
/// * `now_ts` - Current timestamp for calculating age threshold
///
/// # Returns
/// A HashSet of revision numbers that should be kept (not deleted)
fn compute_gc_keep_set(
    revisions_sorted: Vec<(u64, i64)>,
    active_revision: u64,
    keep_versions: u32,
    keep_days: u32,
    now_ts: i64,
) -> std::collections::HashSet<u64> {
    use std::collections::HashSet;
    let mut keep_set: HashSet<u64> = HashSet::new();
    
    // Always keep the active revision (protects against GC-after-rollback bug)
    keep_set.insert(active_revision);

    // Keep the newest revisions up to keep_versions count
    let min_keep = std::cmp::max(1, keep_versions) as usize;
    let mut kept_count = if keep_set.contains(&active_revision) { 1 } else { 0 };

    for (rev, _modified_ts) in &revisions_sorted {
        if kept_count >= min_keep {
            break;
        }
        if keep_set.insert(*rev) {
            kept_count += 1;
        }
    }

    // Additionally keep any revisions within keep_days time window
    if keep_days > 0 {
        let cutoff_ts = now_ts - (keep_days as i64 * 86400);
        for (rev, modified_ts) in &revisions_sorted {
            if *modified_ts >= cutoff_ts {
                keep_set.insert(*rev);
            }
        }
    }

    keep_set
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_keep_set_active_old_revision_keep_versions_1() {
        // Scenario: v1 is active (old), v2 is newest, keep_versions=1, keep_days=0
        // Expected: keep only v1 (active must be preserved even though not newest)
        let revisions = vec![
            (2u64, 2000i64), // v2 newest
            (1u64, 1000i64), // v1 older
        ];
        let active_revision = 1u64;
        let keep_versions = 1u32;
        let keep_days = 0u32;
        let now_ts = 3000i64;

        let keep_set = compute_gc_keep_set(revisions, active_revision, keep_versions, keep_days, now_ts);

        // Must keep v1 (active), even though it's not the newest
        assert!(keep_set.contains(&1), "Active revision v1 must be kept");
        assert_eq!(keep_set.len(), 1, "Should keep exactly 1 revision when keep_versions=1");
    }

    #[test]
    fn test_gc_keep_set_active_old_revision_keep_versions_2() {
        // Scenario: v1 is active (old), v2 is newest, keep_versions=2, keep_days=0
        // Expected: keep both v1 and v2
        let revisions = vec![
            (2u64, 2000i64), // v2 newest
            (1u64, 1000i64), // v1 older
        ];
        let active_revision = 1u64;
        let keep_versions = 2u32;
        let keep_days = 0u32;
        let now_ts = 3000i64;

        let keep_set = compute_gc_keep_set(revisions, active_revision, keep_versions, keep_days, now_ts);

        assert!(keep_set.contains(&1), "Active revision v1 must be kept");
        assert!(keep_set.contains(&2), "Newest revision v2 should be kept");
        assert_eq!(keep_set.len(), 2, "Should keep both revisions when keep_versions=2");
    }

    #[test]
    fn test_gc_keep_set_keep_days_retains_recent() {
        // Scenario: v1 is active (old, outside window), v2 is new (within window)
        // keep_versions=1, keep_days=1 (86400 seconds)
        // Expected: keep both v1 (active) and v2 (within time window)
        let now_ts = 100000i64;
        let revisions = vec![
            (2u64, now_ts - 1000), // v2 very recent (within 1 day)
            (1u64, now_ts - 90000), // v1 old (outside 1 day window)
        ];
        let active_revision = 1u64;
        let keep_versions = 1u32;
        let keep_days = 1u32; // 1 day = 86400 seconds
        
        let keep_set = compute_gc_keep_set(revisions, active_revision, keep_versions, keep_days, now_ts);

        assert!(keep_set.contains(&1), "Active revision v1 must be kept even if outside time window");
        assert!(keep_set.contains(&2), "Recent revision v2 within time window must be kept");
        assert_eq!(keep_set.len(), 2, "Should keep both active and recent revisions");
    }

    #[test]
    fn test_gc_keep_set_multiple_revisions_active_newest() {
        // Scenario: v3 is active (also newest), v2, v1 are older
        // keep_versions=2, keep_days=0
        // Expected: keep v3 (active/newest) and v2
        let revisions = vec![
            (3u64, 3000i64), // v3 newest and active
            (2u64, 2000i64), // v2
            (1u64, 1000i64), // v1
        ];
        let active_revision = 3u64;
        let keep_versions = 2u32;
        let keep_days = 0u32;
        let now_ts = 4000i64;

        let keep_set = compute_gc_keep_set(revisions, active_revision, keep_versions, keep_days, now_ts);

        assert!(keep_set.contains(&3), "Active revision v3 must be kept");
        assert!(keep_set.contains(&2), "Second newest revision v2 should be kept");
        assert!(!keep_set.contains(&1), "Oldest revision v1 should not be kept");
        assert_eq!(keep_set.len(), 2);
    }

    #[test]
    fn test_gc_keep_set_empty_revisions() {
        // Edge case: no revisions
        let revisions = vec![];
        let active_revision = 1u64;
        let keep_versions = 1u32;
        let keep_days = 0u32;
        let now_ts = 1000i64;

        let keep_set = compute_gc_keep_set(revisions, active_revision, keep_versions, keep_days, now_ts);

        assert!(keep_set.contains(&1), "Active revision must always be in keep set");
        assert_eq!(keep_set.len(), 1);
    }

    #[test]
    fn test_gc_keep_set_keep_versions_zero_defaults_to_one() {
        // keep_versions=0 should behave as keep_versions=1 (max(1, keep_versions))
        let revisions = vec![
            (2u64, 2000i64),
            (1u64, 1000i64),
        ];
        let active_revision = 1u64;
        let keep_versions = 0u32;
        let keep_days = 0u32;
        let now_ts = 3000i64;

        let keep_set = compute_gc_keep_set(revisions, active_revision, keep_versions, keep_days, now_ts);

        assert!(keep_set.contains(&1), "Active revision must be kept");
        assert_eq!(keep_set.len(), 1, "Should keep at least 1 revision even when keep_versions=0");
    }
}
