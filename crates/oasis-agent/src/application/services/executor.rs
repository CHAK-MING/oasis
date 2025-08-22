use async_trait::async_trait;
use oasis_core::error::{CoreError, Result};
use serde::{Deserialize, Serialize};
use std::process::Stdio;
use tokio::process::Command;

/// 命令执行结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub duration_ms: u64,
}

/// 命令执行器接口
#[async_trait]
pub trait Executor: Send + Sync {
    /// 执行命令
    async fn execute(&self, command: &str, args: &[String]) -> Result<ExecutionOutput>;
}

/// 原生命令执行器
pub struct NativeExecutor;

#[async_trait]
impl Executor for NativeExecutor {
    async fn execute(&self, command: &str, args: &[String]) -> Result<ExecutionOutput> {
        let start = std::time::Instant::now();
        
        let output = Command::new(command)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| CoreError::TaskExecutionFailed {
                task_id: oasis_core::types::TaskId::new("unknown"),
                reason: format!("Command execution failed: {}", e),
                retry_count: 0,
            })?;

        let duration = start.elapsed();
        
        Ok(ExecutionOutput {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code().unwrap_or(-1),
            duration_ms: duration.as_millis() as u64,
        })
    }
}

/// 带策略控制的命令执行器
pub struct PolicyExecutor {
    allowed_commands: Vec<String>,
    denied_commands: Vec<String>,
    inner: Box<dyn Executor>,
}

impl PolicyExecutor {
    pub fn new(
        allowed_commands: Vec<String>,
        denied_commands: Vec<String>,
        inner: Box<dyn Executor>,
    ) -> Self {
        Self {
            allowed_commands,
            denied_commands,
            inner,
        }
    }

    fn is_command_allowed(&self, command: &str) -> bool {
        // 检查是否在拒绝列表中
        if self.denied_commands.iter().any(|c| c == command) {
            return false;
        }

        // 如果允许列表为空，则允许所有未被拒绝的命令
        if self.allowed_commands.is_empty() {
            return true;
        }

        // 检查是否在允许列表中
        self.allowed_commands.iter().any(|c| c == command)
    }
}

#[async_trait]
impl Executor for PolicyExecutor {
    async fn execute(&self, command: &str, args: &[String]) -> Result<ExecutionOutput> {
        if !self.is_command_allowed(command) {
            return Err(CoreError::PermissionDenied {
                operation: format!("Command '{}' is not allowed by policy", command),
            });
        }

        self.inner.execute(command, args).await
    }
}
