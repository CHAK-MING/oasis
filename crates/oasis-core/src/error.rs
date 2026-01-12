//! 统一错误处理系统 - 增强版

use crate::core_types::{AgentId, BatchId, TaskId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 错误严重程度分类
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ErrorSeverity {
    /// 警告 - 不影响功能
    Warning,
    /// 错误 - 影响当前操作
    #[default]
    Error,
    /// 严重 - 影响系统稳定性
    Critical,
}

/// 核心错误类型 - 统一的错误处理
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum CoreError {
    // === 序列化错误 ===
    #[error("Serialization failed: {message}")]
    Serialization {
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === 任务相关错误 ===
    #[error("Invalid task: {reason}")]
    InvalidTask {
        reason: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    #[error("Task execution failed: {task_id} - {reason} (retry {retry_count})")]
    TaskExecutionFailed {
        task_id: TaskId,
        reason: String,
        retry_count: u32,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    #[error("Task timeout: {task_id}")]
    TaskTimeout {
        task_id: TaskId,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === Agent 相关错误 ===
    #[error("Agent error: {agent_id} - {message}")]
    Agent {
        agent_id: AgentId,
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    #[error("Batch error: {batch_id} - {message}")]
    Batch {
        batch_id: BatchId,
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === 网络和连接错误 ===
    #[error("Network error: {message}")]
    Network {
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    #[error("Connection failed: {endpoint}")]
    Connection {
        endpoint: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === NATS 相关错误 ===
    #[error("NATS error: {message}")]
    Nats {
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === 数据相关错误 ===
    #[error("Entity not found: {entity_type} with id {entity_id}")]
    NotFound {
        entity_type: String,
        entity_id: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    #[error("Version conflict for {entity_type} with id {entity_id}")]
    VersionConflict {
        entity_type: String,
        entity_id: String,
        expected_version: u64,
        actual_version: u64,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === 文件和存储错误 ===
    #[error("File error: {path} - {message}")]
    File {
        path: String,
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    #[error("IO error: {message}")]
    Io {
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === 验证错误 ===
    #[error("Validation error: {message}")]
    Validation {
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === 配置错误 ===
    #[error("Config error: {message}")]
    Config {
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === 权限和安全错误 ===
    #[error("Permission denied: {operation}")]
    PermissionDenied {
        operation: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    // === 系统错误 ===
    #[error("Internal error: {message}")]
    Internal {
        message: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },

    #[error("Service unavailable: {service}")]
    ServiceUnavailable {
        service: String,
        #[serde(skip)]
        severity: ErrorSeverity,
    },
}

impl CoreError {
    /// 判断错误是否可重试
    pub fn is_retriable(&self) -> bool {
        match self {
            CoreError::Network { .. }
            | CoreError::Connection { .. }
            | CoreError::Nats { .. }
            | CoreError::ServiceUnavailable { .. } => true,
            CoreError::TaskExecutionFailed { retry_count, .. } => *retry_count < 3,
            _ => false,
        }
    }

    /// 获取错误严重程度
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            CoreError::Serialization { severity, .. }
            | CoreError::InvalidTask { severity, .. }
            | CoreError::TaskExecutionFailed { severity, .. }
            | CoreError::TaskTimeout { severity, .. }
            | CoreError::Agent { severity, .. }
            | CoreError::Batch { severity, .. }
            | CoreError::Network { severity, .. }
            | CoreError::Connection { severity, .. }
            | CoreError::Nats { severity, .. }
            | CoreError::NotFound { severity, .. }
            | CoreError::VersionConflict { severity, .. }
            | CoreError::File { severity, .. }
            | CoreError::Io { severity, .. }
            | CoreError::Validation { severity, .. }
            | CoreError::Config { severity, .. }
            | CoreError::PermissionDenied { severity, .. }
            | CoreError::Internal { severity, .. }
            | CoreError::ServiceUnavailable { severity, .. } => *severity,
        }
    }

    /// 从通用错误创建分类错误
    pub fn from_anyhow(error: anyhow::Error, task_id: Option<TaskId>) -> Self {
        match task_id {
            Some(tid) => CoreError::TaskExecutionFailed {
                task_id: tid,
                reason: error.to_string(),
                retry_count: 0,
                severity: ErrorSeverity::Error,
            },
            None => CoreError::Internal {
                message: error.to_string(),
                severity: ErrorSeverity::Critical,
            },
        }
    }

    /// 创建带严重程度的错误
    pub fn agent_error(
        agent_id: impl Into<AgentId>,
        message: impl Into<String>,
        severity: ErrorSeverity,
    ) -> Self {
        CoreError::Agent {
            agent_id: agent_id.into(),
            message: message.into(),
            severity,
        }
    }

    /// 快速创建常见错误的便利方法
    pub fn agent_not_found(agent_id: impl Into<AgentId>) -> Self {
        CoreError::Agent {
            agent_id: agent_id.into(),
            message: "Agent not found".to_string(),
            severity: ErrorSeverity::Error,
        }
    }

    pub fn agent_offline(agent_id: impl Into<AgentId>) -> Self {
        CoreError::Agent {
            agent_id: agent_id.into(),
            message: "Agent offline".to_string(),
            severity: ErrorSeverity::Warning,
        }
    }

    pub fn invalid_task(reason: impl Into<String>) -> Self {
        CoreError::InvalidTask {
            reason: reason.into(),
            severity: ErrorSeverity::Error,
        }
    }

    /// 创建内部错误的便利方法
    pub fn internal_error(message: impl Into<String>) -> Self {
        CoreError::Internal {
            message: message.into(),
            severity: ErrorSeverity::Critical,
        }
    }

    /// 创建配置错误的便利方法
    pub fn config_error_with_severity(message: impl Into<String>, severity: ErrorSeverity) -> Self {
        CoreError::Config {
            message: message.into(),
            severity,
        }
    }

    pub fn config_error(message: impl Into<String>) -> Self {
        CoreError::Config {
            message: message.into(),
            severity: ErrorSeverity::Error,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        CoreError::Internal {
            message: message.into(),
            severity: ErrorSeverity::Critical,
        }
    }

    pub fn file_error(path: impl Into<String>, message: impl Into<String>) -> Self {
        CoreError::File {
            path: path.into(),
            message: message.into(),
            severity: ErrorSeverity::Error,
        }
    }

    pub fn connection_error(endpoint: impl Into<String>) -> Self {
        CoreError::Connection {
            endpoint: endpoint.into(),
            severity: ErrorSeverity::Error,
        }
    }

    pub fn batch_not_found(batch_id: impl Into<BatchId>) -> Self {
        CoreError::Batch {
            batch_id: batch_id.into(),
            message: "Batch not found".to_string(),
            severity: ErrorSeverity::Error,
        }
    }
}

/// Core 操作的 Result 类型别名
pub type Result<T> = std::result::Result<T, CoreError>;

impl From<std::io::Error> for CoreError {
    fn from(err: std::io::Error) -> Self {
        let message = err.to_string();
        let severity = match err.kind() {
            std::io::ErrorKind::NotFound => ErrorSeverity::Warning,
            std::io::ErrorKind::PermissionDenied => ErrorSeverity::Error,
            std::io::ErrorKind::TimedOut => ErrorSeverity::Warning,
            std::io::ErrorKind::ConnectionRefused => ErrorSeverity::Error,
            _ => ErrorSeverity::Error,
        };

        match err.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
                CoreError::File {
                    path: "unknown".to_string(),
                    message,
                    severity,
                }
            }
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::ConnectionRefused => {
                CoreError::Connection {
                    endpoint: "unknown".to_string(),
                    severity,
                }
            }
            _ => CoreError::Internal { message, severity },
        }
    }
}

impl CoreError {
    /// 从 IO 错误创建文件错误，保留文件路径上下文
    pub fn from_io_with_path(err: std::io::Error, path: impl Into<String>) -> Self {
        let message = err.to_string();
        let severity = match err.kind() {
            std::io::ErrorKind::NotFound => ErrorSeverity::Warning,
            std::io::ErrorKind::PermissionDenied => ErrorSeverity::Error,
            _ => ErrorSeverity::Error,
        };
        match err.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
                CoreError::File {
                    path: path.into(),
                    message,
                    severity,
                }
            }
            _ => CoreError::Internal { message, severity },
        }
    }

    /// 从 IO 错误创建连接错误，保留端点上下文
    pub fn from_io_with_endpoint(err: std::io::Error, endpoint: impl Into<String>) -> Self {
        let message = err.to_string();
        let severity = match err.kind() {
            std::io::ErrorKind::TimedOut => ErrorSeverity::Warning,
            std::io::ErrorKind::ConnectionRefused => ErrorSeverity::Error,
            _ => ErrorSeverity::Error,
        };
        match err.kind() {
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::ConnectionRefused => {
                CoreError::Connection {
                    endpoint: endpoint.into(),
                    severity,
                }
            }
            _ => CoreError::Internal { message, severity },
        }
    }
}

impl From<async_nats::Error> for CoreError {
    fn from(err: async_nats::Error) -> Self {
        CoreError::Nats {
            message: err.to_string(),
            severity: ErrorSeverity::Error,
        }
    }
}

impl<T: Clone + std::fmt::Debug + std::fmt::Display + PartialEq> From<async_nats::error::Error<T>>
    for CoreError
{
    fn from(err: async_nats::error::Error<T>) -> Self {
        CoreError::Nats {
            message: err.to_string(),
            severity: ErrorSeverity::Error,
        }
    }
}

impl From<tonic::transport::Error> for CoreError {
    fn from(err: tonic::transport::Error) -> Self {
        CoreError::ServiceUnavailable {
            service: format!("grpc: {}", err),
            severity: ErrorSeverity::Error,
        }
    }
}

impl From<tonic::Status> for CoreError {
    fn from(status: tonic::Status) -> Self {
        use tonic::Code;
        match status.code() {
            Code::DeadlineExceeded => CoreError::Connection {
                endpoint: "grpc".to_string(),
                severity: ErrorSeverity::Warning,
            },
            Code::Unavailable => CoreError::ServiceUnavailable {
                service: "grpc".to_string(),
                severity: ErrorSeverity::Error,
            },
            Code::PermissionDenied | Code::Unauthenticated => CoreError::PermissionDenied {
                operation: format!("grpc: {}", status.message()),
                severity: ErrorSeverity::Error,
            },
            _ => CoreError::Internal {
                message: format!("gRPC({}): {}", status.code() as i32, status.message()),
                severity: ErrorSeverity::Critical,
            },
        }
    }
}

impl From<anyhow::Error> for CoreError {
    fn from(err: anyhow::Error) -> Self {
        CoreError::Internal {
            message: err.to_string(),
            severity: ErrorSeverity::Critical,
        }
    }
}
