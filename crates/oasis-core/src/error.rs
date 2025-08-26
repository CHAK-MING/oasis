//! 统一错误处理系统

use crate::type_defs::{AgentId, TaskId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 核心错误类型 - 统一的错误处理
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum CoreError {
    // === 序列化错误 ===
    #[error("Serialization failed: {message}")]
    Serialization { message: String },

    // === 任务相关错误 ===
    #[error("Invalid task: {reason}")]
    InvalidTask { reason: String },

    #[error("Task execution failed: {task_id} - {reason}")]
    TaskExecutionFailed {
        task_id: TaskId,
        reason: String,
        retry_count: u32,
    },

    #[error("Task timeout: {task_id}")]
    TaskTimeout { task_id: TaskId },

    // === Agent 相关错误 ===
    #[error("Agent error: {agent_id} - {message}")]
    Agent { agent_id: AgentId, message: String },

    // === 网络和连接错误 ===
    #[error("Network error: {message}")]
    Network { message: String },

    #[error("Connection failed: {endpoint}")]
    Connection { endpoint: String },

    // === NATS 相关错误 ===
    #[error("NATS error: {message}")]
    Nats { message: String },

    // === 文件和存储错误 ===
    #[error("File error: {path} - {message}")]
    File { path: String, message: String },

    // === 配置错误 ===
    #[error("Config error: {message}")]
    Config { message: String },

    // === 权限和安全错误 ===
    #[error("Permission denied: {operation}")]
    PermissionDenied { operation: String },

    // === 系统错误 ===
    #[error("Internal error: {message}")]
    Internal { message: String },

    #[error("Service unavailable: {service}")]
    ServiceUnavailable { service: String },
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

    /// 从通用错误创建分类错误
    pub fn from_anyhow(error: anyhow::Error, task_id: Option<TaskId>) -> Self {
        match task_id {
            Some(tid) => CoreError::TaskExecutionFailed {
                task_id: tid,
                reason: error.to_string(),
                retry_count: 0,
            },
            None => CoreError::Internal {
                message: error.to_string(),
            },
        }
    }

    /// 创建 Agent 未找到错误
    pub fn agent_not_found(agent_id: impl Into<AgentId>) -> Self {
        CoreError::Agent {
            agent_id: agent_id.into(),
            message: "Agent not found".to_string(),
        }
    }

    /// 创建 Agent 离线错误
    pub fn agent_offline(agent_id: impl Into<AgentId>) -> Self {
        CoreError::Agent {
            agent_id: agent_id.into(),
            message: "Agent offline".to_string(),
        }
    }

    /// 创建任务无效错误
    pub fn invalid_task(reason: impl Into<String>) -> Self {
        CoreError::InvalidTask {
            reason: reason.into(),
        }
    }

    /// 创建配置错误
    pub fn config_error(message: impl Into<String>) -> Self {
        CoreError::Config {
            message: message.into(),
        }
    }

    /// 创建内部错误
    pub fn internal(message: impl Into<String>) -> Self {
        CoreError::Internal {
            message: message.into(),
        }
    }

    /// 创建文件错误（带上下文）
    pub fn file_error(path: impl Into<String>, message: impl Into<String>) -> Self {
        CoreError::File {
            path: path.into(),
            message: message.into(),
        }
    }

    /// 创建连接错误（带上下文）
    pub fn connection_error(endpoint: impl Into<String>) -> Self {
        CoreError::Connection {
            endpoint: endpoint.into(),
        }
    }
}

/// Core 操作的 Result 类型别名
pub type Result<T> = std::result::Result<T, CoreError>;



impl From<std::io::Error> for CoreError {
    fn from(err: std::io::Error) -> Self {
        let message = err.to_string();
        match err.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
                CoreError::File {
                    path: "unknown".to_string(),
                    message,
                }
            }
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::ConnectionRefused => {
                CoreError::Connection {
                    endpoint: "unknown".to_string(),
                }
            }
            _ => CoreError::Internal { message },
        }
    }
}

impl CoreError {
    /// 从 IO 错误创建文件错误，保留文件路径上下文
    pub fn from_io_with_path(err: std::io::Error, path: impl Into<String>) -> Self {
        let message = err.to_string();
        match err.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
                CoreError::File {
                    path: path.into(),
                    message,
                }
            }
            _ => CoreError::Internal { message },
        }
    }

    /// 从 IO 错误创建连接错误，保留端点上下文
    pub fn from_io_with_endpoint(err: std::io::Error, endpoint: impl Into<String>) -> Self {
        let message = err.to_string();
        match err.kind() {
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::ConnectionRefused => {
                CoreError::Connection {
                    endpoint: endpoint.into(),
                }
            }
            _ => CoreError::Internal { message },
        }
    }
}

impl From<async_nats::Error> for CoreError {
    fn from(err: async_nats::Error) -> Self {
        CoreError::Nats {
            message: err.to_string(),
        }
    }
}

impl From<tonic::transport::Error> for CoreError {
    fn from(err: tonic::transport::Error) -> Self {
        CoreError::ServiceUnavailable {
            service: format!("grpc: {}", err),
        }
    }
}

impl From<tonic::Status> for CoreError {
    fn from(status: tonic::Status) -> Self {
        use tonic::Code;
        match status.code() {
            Code::DeadlineExceeded => CoreError::Connection {
                endpoint: "grpc".to_string(),
            },
            Code::Unavailable => CoreError::ServiceUnavailable {
                service: "grpc".to_string(),
            },
            Code::PermissionDenied | Code::Unauthenticated => CoreError::PermissionDenied {
                operation: format!("grpc: {}", status.message()),
            },
            _ => CoreError::Internal {
                message: format!("gRPC({}): {}", status.code() as i32, status.message()),
            },
        }
    }
}

impl From<anyhow::Error> for CoreError {
    fn from(err: anyhow::Error) -> Self {
        CoreError::Internal {
            message: err.to_string(),
        }
    }
}
