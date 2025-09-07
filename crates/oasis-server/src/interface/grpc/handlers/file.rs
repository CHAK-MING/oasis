//! 文件上传处理器 - 适配统一文件类型系统

use tonic::{Request, Response, Status};
use tracing::{debug, error, instrument, warn};

use oasis_core::error::Result;

use crate::interface::grpc::server::OasisServer;

// Session管理
use dashmap::DashMap;
use lazy_static::lazy_static;
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

/// 文件上传会话
#[derive(Debug, Clone)]
struct UploadSession {
    source_path: String,
    size: u64,
    buffer: Vec<u8>,
    checksum: String,
    created_at: Instant,
}

/// 上传会话管理器
struct SessionManager {
    sessions: DashMap<String, UploadSession>,
    max_sessions: usize,
    max_memory_mb: usize,
    session_ttl: Duration,
}

impl SessionManager {
    fn new() -> Self {
        let manager = Self {
            sessions: DashMap::new(),
            max_sessions: 100,                     // 最大并发会话数
            max_memory_mb: 1024,                   // 最大内存使用 1GB
            session_ttl: Duration::from_secs(600), // 10分钟 TTL
        };

        // 启动清理任务
        let sessions = manager.sessions.clone();
        let session_ttl = manager.session_ttl;
        let max_sessions = manager.max_sessions;
        let max_memory_mb = manager.max_memory_mb;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;

                // 带错误恢复的清理逻辑
                if let Err(e) =
                    cleanup_sessions(&sessions, session_ttl, max_sessions, max_memory_mb).await
                {
                    error!("Session cleanup failed, but continuing: {}", e);
                }
            }
        });

        manager
    }
}

/// 安全地清理会话，带错误恢复机制
async fn cleanup_sessions(
    sessions: &DashMap<String, UploadSession>,
    session_ttl: Duration,
    max_sessions: usize,
    max_memory_mb: usize,
) -> Result<()> {
    let now = Instant::now();
    let mut expired = Vec::new();

    // 收集过期的会话ID
    for entry in sessions.iter() {
        if now.duration_since(entry.created_at) > session_ttl {
            expired.push(entry.key().clone());
        }
    }

    // 批量删除过期会话
    for id in expired {
        if let Some((_, _session)) = sessions.remove(&id) {
            warn!("Cleaned up expired upload session: {}", id);
        }
    }

    // 检查内存使用
    let memory_bytes: usize = sessions
        .iter()
        .map(|entry| entry.value().buffer.capacity())
        .sum();
    let memory_mb = memory_bytes / (1024 * 1024);

    // 如果超过限制，清理最老的会话
    if sessions.len() > max_sessions || memory_mb > max_memory_mb {
        let mut sessions_vec: Vec<_> = sessions
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().created_at))
            .collect();
        sessions_vec.sort_by_key(|(_, created_at)| *created_at);

        let to_remove = if sessions.len() > max_sessions {
            sessions.len() - max_sessions
        } else {
            (memory_mb - max_memory_mb) / 10 // 每次清理10MB
        };

        for (id, _) in sessions_vec.iter().take(to_remove) {
            warn!("Cleaning up session due to resource limits: {}", id);
            sessions.remove(id);
        }
    }

    Ok(())
}

impl FileHandlers {
    async fn create(
        upload_id: String,
        source_path: String,
        size: u64,
        checksum: String,
    ) -> std::result::Result<u64, String> {
        // 检查是否已存在
        if let Some(session) = SESSIONS.sessions.get(&upload_id) {
            return Ok(session.buffer.len() as u64);
        }

        // 检查资源限制
        if SESSIONS.sessions.len() >= SESSIONS.max_sessions {
            return Err(format!(
                "Too many active sessions (max: {})",
                SESSIONS.max_sessions
            ));
        }

        // 检查内存使用
        let current_memory_bytes: usize = SESSIONS
            .sessions
            .iter()
            .map(|entry| entry.value().buffer.capacity())
            .sum();
        let current_memory_mb = current_memory_bytes / (1024 * 1024);
        let new_session_memory_mb = (size / (1024 * 1024)) as usize;

        if current_memory_mb + new_session_memory_mb > SESSIONS.max_memory_mb {
            return Err(format!(
                "Insufficient memory (current: {}MB, requested: {}MB, max: {}MB)",
                current_memory_mb, new_session_memory_mb, SESSIONS.max_memory_mb
            ));
        }

        SESSIONS.sessions.insert(
            upload_id,
            UploadSession {
                source_path,
                size,
                buffer: Vec::with_capacity(size.min(100 * 1024 * 1024) as usize),
                checksum,
                created_at: Instant::now(),
            },
        );
        Ok(0)
    }

    async fn write_chunk(
        upload_id: &str,
        offset: u64,
        data: &[u8],
    ) -> std::result::Result<u64, Status> {
        let mut session = SESSIONS
            .sessions
            .get_mut(upload_id)
            .ok_or_else(|| Status::not_found("upload session not found"))?;

        // 检查偏移量
        if offset != session.buffer.len() as u64 {
            return Err(Status::invalid_argument("invalid offset"));
        }

        // 检查大小限制
        if session.buffer.len() + data.len() > session.size as usize {
            return Err(Status::invalid_argument("upload exceeds total size"));
        }

        session.buffer.extend_from_slice(data);
        Ok(session.buffer.len() as u64)
    }

    async fn take(upload_id: &str) -> Option<UploadSession> {
        SESSIONS
            .sessions
            .remove(upload_id)
            .map(|(_, session)| session)
    }

    async fn get_stats() -> (usize, usize, f64) {
        let total = SESSIONS.sessions.len();
        let memory_bytes: usize = SESSIONS
            .sessions
            .iter()
            .map(|entry| entry.value().buffer.capacity())
            .sum();
        let memory_mb = memory_bytes as f64 / (1024.0 * 1024.0);
        (total, 0, memory_mb) // expired count is not tracked here
    }
}

lazy_static! {
    static ref SESSIONS: SessionManager = SessionManager::new();
}

/// 文件上传相关的 gRPC 处理器
pub struct FileHandlers;

impl FileHandlers {
    /// 开始文件上传
    #[instrument(skip_all)]
    pub async fn begin_file_upload(
        _srv: &OasisServer,
        request: Request<oasis_core::proto::FileSpecMsg>,
    ) -> std::result::Result<Response<oasis_core::proto::FileUploadSession>, Status> {
        let req = request.into_inner();

        // 验证参数
        if req.source_path.is_empty() {
            return Err(Status::invalid_argument("source_path cannot be empty"));
        }

        // 使用新的验证模块
        if let Err(e) = oasis_core::constants::validation::validate_file_path(&req.source_path) {
            return Err(Status::invalid_argument(format!(
                "Invalid file path: {}",
                e
            )));
        }

        if req.size == 0 {
            return Err(Status::invalid_argument("size must be greater than 0"));
        }

        // TODO: 支持大文件
        if req.size > 100 * 1024 * 1024 {
            // 100MB 限制
            return Err(Status::invalid_argument("file too large (max 100MB)"));
        }

        // 生成上传 ID
        let upload_id = uuid::Uuid::new_v4().to_string();

        // 创建会话
        let received = FileHandlers::create(
            upload_id.clone(),
            req.source_path.clone(),
            req.size,
            req.checksum,
        )
        .await
        .map_err(|e| {
            Status::resource_exhausted(format!("Failed to create upload session: {}", e))
        })?;

        // 记录会话统计信息
        let (total, expired, memory_mb) = FileHandlers::get_stats().await;
        debug!(
            upload_id = %upload_id,
            total_sessions = total,
            expired_sessions = expired,
            memory_mb = memory_mb,
            "Upload session created"
        );

        Ok(Response::new(oasis_core::proto::FileUploadSession {
            upload_id,
            received_bytes: received,
            chunk_size: 1024 * 1024, // 1MB chunks
            resume: false,
        }))
    }

    /// 上传文件分片
    #[instrument(skip_all)]
    pub async fn upload_file_chunk(
        _srv: &OasisServer,
        request: Request<oasis_core::proto::FileChunkMsg>,
    ) -> std::result::Result<Response<oasis_core::proto::FileChunkResponse>, Status> {
        let req = request.into_inner();

        // 验证数据块大小
        if req.data.is_empty() {
            return Err(Status::invalid_argument("data cannot be empty"));
        }
        if req.data.len() > 10 * 1024 * 1024 {
            // 10MB 限制
            return Err(Status::invalid_argument("chunk size too large (max 10MB)"));
        }

        let recv = FileHandlers::write_chunk(&req.upload_id, req.offset, &req.data).await?;

        Ok(Response::new(oasis_core::proto::FileChunkResponse {
            received_bytes: recv,
            complete: false, // Will be set to true in commit
        }))
    }

    /// 提交文件上传
    #[instrument(skip_all)]
    pub async fn commit_file_upload(
        srv: &OasisServer,
        request: Request<oasis_core::proto::CommitFileMsg>,
    ) -> std::result::Result<Response<oasis_core::proto::FileOperationResult>, Status> {
        let req = request.into_inner();

        // 获取上传会话
        let Some(session) = FileHandlers::take(&req.upload_id).await else {
            return Err(Status::not_found("upload session not found"));
        };

        // 验证文件大小
        if session.buffer.is_empty() {
            return Err(Status::invalid_argument("uploaded file is empty"));
        }

        if session.buffer.len() as u64 != session.size {
            return Err(Status::invalid_argument(format!(
                "incomplete upload: expected {} bytes, got {}",
                session.size,
                session.buffer.len()
            )));
        }

        // 计算 SHA256
        let mut hasher = Sha256::new();
        hasher.update(&session.buffer);
        let checksum = format!("{:x}", hasher.finalize());

        // 验证 SHA256
        if !session.checksum.is_empty() && session.checksum != checksum {
            return Err(Status::invalid_argument(format!(
                "checksum mismatch: expected {}, got {}",
                session.checksum, checksum
            )));
        }

        let upload_result = srv
            .context()
            .file_service
            .upload(&session.source_path, session.buffer)
            .await
            .map_err(|e| Status::internal(format!("Failed to store file: {}", e)))?;

        debug!(
            "File uploaded successfully: {} (size: {}, sha256: {})",
            session.source_path, session.size, checksum
        );

        Ok(Response::new(oasis_core::proto::FileOperationResult {
            success: upload_result.success,
            message: upload_result.message,
            revision: upload_result.revision,
        }))
    }

    /// 应用文件 - 使用正确的请求和响应类型
    #[instrument(skip_all)]
    pub async fn apply_file(
        srv: &OasisServer,
        request: Request<oasis_core::proto::FileApplyRequestMsg>,
    ) -> std::result::Result<Response<oasis_core::proto::FileOperationResult>, Status> {
        // 修复类型
        let req = request.into_inner();

        // 验证参数
        if req.config.as_ref().unwrap().source_path.is_empty() {
            return Err(Status::invalid_argument("name cannot be empty"));
        }

        // 解析目标 agents
        let config = req
            .config
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("config is required for file apply"))?;

        let selector_expr = config
            .target
            .as_ref()
            .map(|t| t.expression.as_str())
            .unwrap_or("");
        let result = srv
            .context()
            .agent_service
            .query(selector_expr)
            .await
            .map_err(|e| Status::internal(format!("Failed to resolve selector: {}", e)))?;

        let agent_ids = result.to_online_agents();
        if agent_ids.is_empty() {
            return Err(Status::failed_precondition("No agents match the target"));
        }

        // 下载文件数据
        let _ = srv
            .context()
            .file_service
            .apply(config, agent_ids)
            .await
            .map_err(|e| Status::internal(format!("Failed to download file: {}", e)))?;

        // 返回完整的FileOperationResult
        Ok(Response::new(oasis_core::proto::FileOperationResult {
            success: true,
            message: format!("File apply task created successfully"),
            revision: 0,
        }))
    }

    /// 清理文件 - 使用正确的请求类型
    #[instrument(skip_all)]
    pub async fn clear_files(
        srv: &OasisServer,
        _request: Request<oasis_core::proto::EmptyMsg>,
    ) -> std::result::Result<Response<oasis_core::proto::FileOperationResult>, Status> {
        // 清理所有文件
        let deleted_count = srv
            .context()
            .file_service
            .clear_all()
            .await
            .map_err(|e| Status::internal(format!("Failed to clear files: {}", e)))?;

        Ok(Response::new(oasis_core::proto::FileOperationResult {
            success: true,
            message: format!("成功清理 {} 个文件", deleted_count),
            revision: 0,
        }))
    }

    /// 获取文件历史版本
    #[instrument(skip_all)]
    pub async fn get_file_history(
        srv: &OasisServer,
        request: Request<oasis_core::proto::GetFileHistoryRequest>,
    ) -> std::result::Result<Response<oasis_core::proto::GetFileHistoryResponse>, Status> {
        let req = request.into_inner();
        debug!("Get file history request: source_path={}", req.source_path);

        // 验证参数
        if req.source_path.is_empty() {
            return Err(Status::invalid_argument("source_path cannot be empty"));
        }

        // 获取文件历史
        let file_history = srv
            .context()
            .file_service
            .get_file_history(&req.source_path)
            .await
            .map_err(|e| Status::internal(format!("Failed to get file history: {}", e)))?;

        let response = oasis_core::proto::GetFileHistoryResponse {
            file_history: file_history.map(|h| h.into()),
        };

        Ok(Response::new(response))
    }

    /// 回滚文件到指定版本
    #[instrument(skip_all)]
    pub async fn rollback_file(
        srv: &OasisServer,
        request: Request<oasis_core::proto::RollbackFileRequest>,
    ) -> std::result::Result<Response<oasis_core::proto::FileOperationResult>, Status> {
        let req = request.into_inner();
        
        let config = req.config.as_ref().ok_or_else(|| Status::invalid_argument("config is required for rollback file"))?;

        // 解析选择器
        let selector_expr = &config.target.as_ref().map(|t| t.expression.as_str()).unwrap_or("");
        let result = srv
            .context()
            .agent_service
            .query(selector_expr)
            .await
            .map_err(|e| Status::internal(format!("Failed to resolve selector: {}", e)))?;

        let agent_ids = result.to_online_agents();
        if agent_ids.is_empty() {
            return Err(Status::failed_precondition("No agents match the target"));
        }

        // 执行回滚
        let result = srv
            .context()
            .file_service
            .rollback_file(config, agent_ids)
            .await
            .map_err(|e| Status::internal(format!("Failed to rollback file: {}", e)))?;

        Ok(Response::new(oasis_core::proto::FileOperationResult {
            success: result.success,
            message: result.message,
            revision: result.revision,
        }))
    }
}
