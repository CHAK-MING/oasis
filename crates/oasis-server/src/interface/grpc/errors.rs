use oasis_core::error::CoreError;
use tonic::Status;

/// 将 CoreError 映射为 tonic::Status
pub fn map_core_error(error: CoreError) -> Status {
    match error {
        CoreError::InvalidTask {
            reason,
            severity: _,
        } => Status::invalid_argument(format!("无效请求: {}", reason)),
        CoreError::TaskExecutionFailed {
            task_id,
            reason,
            severity: _,
            ..
        } => Status::internal(format!("任务执行失败 {}: {}", task_id, reason)),
        CoreError::TaskTimeout {
            task_id,
            severity: _,
        } => Status::deadline_exceeded(format!("任务超时: {}", task_id)),
        CoreError::Agent {
            agent_id,
            message,
            severity: _,
        } => {
            if message.contains("not found") {
                Status::not_found(format!("Agent 未找到: {}", agent_id))
            } else if message.contains("offline") {
                Status::unavailable(format!("Agent 离线: {}", agent_id))
            } else {
                Status::internal(format!("Agent 错误 {}: {}", agent_id, message))
            }
        }
        CoreError::Batch {
            batch_id,
            message,
            severity: _,
        } => Status::internal(format!("批次错误 {}: {}", batch_id, message)),
        CoreError::File {
            path,
            message,
            severity: _,
        } => {
            if message.contains("not found") {
                Status::not_found(format!("文件未找到: {}", path))
            } else if message.contains("access denied") {
                Status::permission_denied(format!("文件访问被拒绝: {}", path))
            } else {
                Status::internal(format!("文件错误 {}: {}", path, message))
            }
        }
        CoreError::Config {
            message,
            severity: _,
        } => Status::invalid_argument(format!("配置错误: {}", message)),
        CoreError::Nats {
            message,
            severity: _,
        } => Status::internal(format!("NATS 错误: {}", message)),
        CoreError::Network {
            message,
            severity: _,
        } => Status::unavailable(format!("网络错误: {}", message)),
        CoreError::Connection {
            endpoint,
            severity: _,
        } => Status::unavailable(format!("连接失败: {}", endpoint)),
        CoreError::Internal {
            message,
            severity: _,
        } => Status::internal(format!("服务器内部错误: {}", message)),
        CoreError::ServiceUnavailable {
            service,
            severity: _,
        } => Status::unavailable(format!("服务不可用: {}", service)),
        CoreError::PermissionDenied {
            operation,
            severity: _,
        } => Status::permission_denied(format!("权限不足: {}", operation)),
        CoreError::Serialization {
            message,
            severity: _,
        } => Status::internal(format!("序列化错误: {}", message)),
        CoreError::NotFound {
            entity_type,
            entity_id,
            severity: _,
        } => Status::not_found(format!("{} 未找到: {}", entity_type, entity_id)),
        CoreError::VersionConflict {
            entity_type,
            entity_id,
            expected_version,
            actual_version,
            severity: _,
        } => Status::aborted(format!(
            "版本冲突: {} {} (期望 {}, 实际 {})",
            entity_type, entity_id, expected_version, actual_version
        )),
    }
}
