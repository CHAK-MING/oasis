use anyhow::{Result, anyhow};
use async_trait::async_trait;
use regex::Regex;
use std::time::Duration;

use crate::infrastructure::system::executor::CommandExecutor;

/// 命令执行的输出结果
#[derive(Debug, Clone)]
pub struct ExecutionOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// 命令执行的输入参数
#[derive(Debug, Clone)]
pub struct ExecutionInput {
    pub command: String,
    pub args: Vec<String>,
    pub env: std::collections::HashMap<String, String>,
    pub timeout: Duration,
}

/// 通用的异步执行器接口
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, input: ExecutionInput) -> Result<ExecutionOutput>;
}

/// 原生执行器适配器：包装现有的 CommandExecutor
pub struct NativeExecutor {
    inner: std::sync::Arc<CommandExecutor>,
}

impl NativeExecutor {
    pub fn new(inner: std::sync::Arc<CommandExecutor>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Executor for NativeExecutor {
    async fn execute(&self, input: ExecutionInput) -> Result<ExecutionOutput> {
        // 直接委托给现有执行器（其内部已处理安全策略与超时控制）
        let (exit_code, stdout, stderr) = self
            .inner
            .execute(&input.command, &input.args, &input.env)
            .await?;

        Ok(ExecutionOutput {
            stdout,
            stderr,
            exit_code,
        })
    }
}

/// 策略执行器：基于正则表达式的 allow/deny 控制
pub struct PolicyExecutor<E: Executor> {
    inner: E,
    allowed_patterns: Vec<Regex>,
    denied_patterns: Vec<Regex>,
}

impl<E: Executor> PolicyExecutor<E> {
    pub fn new(inner: E, allowed: Vec<String>, denied: Vec<String>) -> Result<Self> {
        let allowed_patterns = allowed
            .into_iter()
            .map(|p| Regex::new(&p))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let denied_patterns = denied
            .into_iter()
            .map(|p| Regex::new(&p))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            inner,
            allowed_patterns,
            denied_patterns,
        })
    }
}

#[async_trait]
impl<E: Executor> Executor for PolicyExecutor<E> {
    async fn execute(&self, input: ExecutionInput) -> Result<ExecutionOutput> {
        let full = if input.args.is_empty() {
            input.command.clone()
        } else {
            format!("{} {}", input.command, input.args.join(" "))
        };

        if self.denied_patterns.iter().any(|re| re.is_match(&full)) {
            return Err(anyhow!("Command is explicitly denied by policy"));
        }
        if !self.allowed_patterns.is_empty()
            && !self.allowed_patterns.iter().any(|re| re.is_match(&full))
        {
            return Err(anyhow!("Command is not in the allowlist"));
        }

        self.inner.execute(input).await
    }
}
