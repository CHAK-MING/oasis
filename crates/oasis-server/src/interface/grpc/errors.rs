use oasis_core::error::CoreError;
use tonic::Status;

/// 将 CoreError 映射为 tonic::Status
pub fn map_core_error(error: CoreError) -> Status {
    match error {
        CoreError::InvalidTask {
            reason,
            severity: _,
        } => Status::invalid_argument(format!("Invalid task: {}", reason)),
        CoreError::TaskExecutionFailed {
            task_id,
            reason,
            severity: _,
            ..
        } => Status::internal(format!("Task execution failed {}: {}", task_id, reason)),
        CoreError::TaskTimeout {
            task_id,
            severity: _,
        } => Status::deadline_exceeded(format!("Task timeout: {}", task_id)),
        CoreError::Agent {
            agent_id,
            message,
            severity: _,
        } => {
            if message.contains("not found") {
                Status::not_found(format!("Agent not found: {}", agent_id))
            } else if message.contains("offline") {
                Status::unavailable(format!("Agent offline: {}", agent_id))
            } else {
                Status::internal(format!("Agent error {}: {}", agent_id, message))
            }
        }
        CoreError::Batch {
            batch_id,
            message,
            severity: _,
        } => Status::internal(format!("Batch error {}: {}", batch_id, message)),
        CoreError::File {
            path,
            message,
            severity: _,
        } => {
            if message.contains("not found") {
                Status::not_found(format!("File not found: {}", path))
            } else if message.contains("access denied") {
                Status::permission_denied(format!("File access denied: {}", path))
            } else {
                Status::internal(format!("File error {}: {}", path, message))
            }
        }
        CoreError::Config {
            message,
            severity: _,
        } => Status::invalid_argument(format!("Configuration error: {}", message)),
        CoreError::Nats {
            message,
            severity: _,
        } => Status::internal(format!("NATS error: {}", message)),
        CoreError::Network {
            message,
            severity: _,
        } => Status::unavailable(format!("Network error: {}", message)),
        CoreError::Connection {
            endpoint,
            severity: _,
        } => Status::unavailable(format!("Connection failed: {}", endpoint)),
        CoreError::Internal {
            message,
            severity: _,
        } => Status::internal(format!("Internal error: {}", message)),
        CoreError::ServiceUnavailable {
            service,
            severity: _,
        } => Status::unavailable(format!("Service unavailable: {}", service)),
        CoreError::PermissionDenied {
            operation,
            severity: _,
        } => Status::permission_denied(format!("Permission denied: {}", operation)),
        CoreError::Serialization {
            message,
            severity: _,
        } => Status::internal(format!("Serialization error: {}", message)),
        CoreError::NotFound {
            entity_type,
            entity_id,
            severity: _,
        } => Status::not_found(format!("{} not found: {}", entity_type, entity_id)),
        CoreError::VersionConflict {
            entity_type,
            entity_id,
            expected_version,
            actual_version,
            severity: _,
        } => Status::aborted(format!(
            "Version conflict for {} {} (expected {}, actual {})",
            entity_type, entity_id, expected_version, actual_version
        )),
    }
}
