use std::future::Future;
use std::time::Duration;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// 执行错误类型
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("Operation was cancelled")]
    Cancelled,
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),
    #[error("Execution failed: {0}")]
    Failed(#[from] anyhow::Error),
}

/// 优雅停机管理器
#[derive(Debug, Clone)]
pub struct GracefulShutdown {
    /// 取消令牌，用于通知所有任务停止
    pub token: CancellationToken,
}

impl GracefulShutdown {
    /// 创建新的优雅停机管理器
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// 等待停机信号（SIGINT, SIGTERM）
    pub async fn wait_for_signal(&self) {
        let ctrl_c = async {
            if let Err(e) = signal::ctrl_c().await {
                warn!(error = %e, "failed to install Ctrl+C handler");
                // 如果无法安装，避免阻塞：直接等待一个永不完成的 future
                std::future::pending::<()>().await;
            }
        };

        #[cfg(unix)]
        let terminate = async {
            match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                Ok(mut stream) => {
                    stream.recv().await;
                }
                Err(e) => {
                    warn!(error = %e, "failed to install SIGTERM handler");
                    std::future::pending::<()>().await;
                }
            }
        };

        tokio::select! {
            biased;
            _ = ctrl_c => {
                info!("Received Ctrl+C signal");
            }
            _ = terminate => {
                info!("Received SIGTERM signal");
            }
        }

        info!("Initiating graceful shutdown...");
        self.token.cancel();
    }

    /// 创建一个子令牌，用于特定的任务
    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    /// 检查是否已经收到停机信号
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// 等待停机信号
    pub async fn cancelled(&self) {
        self.token.cancelled().await
    }
}

impl Default for GracefulShutdown {
    fn default() -> Self {
        Self::new()
    }
}

/// 通用的可取消异步操作执行器
pub async fn with_cancellation<T>(
    future: impl Future<Output = T>,
    cancel_token: CancellationToken,
) -> Result<T, ExecutionError> {
    tokio::select! {
        result = future => Ok(result),
        _ = cancel_token.cancelled() => Err(ExecutionError::Cancelled),
    }
}

/// 带超时和取消的异步操作执行器
pub async fn with_cancellation_and_timeout<T>(
    future: impl Future<Output = T>,
    cancel_token: CancellationToken,
    timeout: Duration,
) -> Result<T, ExecutionError> {
    tokio::select! {
        result = future => Ok(result),
        _ = cancel_token.cancelled() => Err(ExecutionError::Cancelled),
        _ = tokio::time::sleep(timeout) => Err(ExecutionError::Timeout(timeout)),
    }
}

/// 可取消的进程执行器
/// 专门用于执行外部进程，支持取消和超时
pub async fn execute_process_with_cancellation(
    child: tokio::process::Child,
    cancel_token: CancellationToken,
    timeout: Duration,
    context: &str,
) -> Result<std::process::Output, ExecutionError> {
    let child_id = child.id();
    let wait_handle = tokio::spawn(async move { child.wait_with_output().await });

    tokio::select! {
        res = wait_handle => {
            match res {
                Ok(output_result) => output_result.map_err(|e| ExecutionError::Failed(anyhow::anyhow!("{} execution failed: {}", context, e))),
                Err(e) => Err(ExecutionError::Failed(anyhow::anyhow!("{} wait task failed: {}", context, e))),
            }
        }
        _ = cancel_token.cancelled() => {
            // 取消：杀死子进程
            if let Some(pid) = child_id {
                if let Err(e) = kill_process_safely(pid) {
                    warn!("Failed to kill process {} during cancellation: {}", pid, e);
                }
            }
            Err(ExecutionError::Cancelled)
        }
        _ = tokio::time::sleep(timeout) => {
            // 超时：杀死子进程
            if let Some(pid) = child_id {
                if let Err(e) = kill_process_safely(pid) {
                    warn!("Failed to kill process {} during timeout: {}", pid, e);
                }
            }
            Err(ExecutionError::Timeout(timeout))
        }
    }
}

/// 安全地终止进程
#[cfg(unix)]
fn kill_process_safely(pid: u32) -> Result<(), ExecutionError> {
    // 验证pid有效性
    if pid == 0 {
        return Err(ExecutionError::Failed(anyhow::anyhow!("Invalid PID: 0")));
    }

    // SAFETY: `pid` is validated to be non-zero and comes from an OS process ID.
    // libc::kill does not take ownership of pointers and only uses the numeric PID.
    #[allow(unsafe_code)]
    unsafe {
        // 更安全的调用：检查返回值
        let result = libc::kill(pid as i32, libc::SIGTERM);
        if result != 0 {
            return Err(ExecutionError::Failed(anyhow::anyhow!(
                "Failed to send SIGTERM to PID {}: errno {}",
                pid,
                std::io::Error::last_os_error()
            )));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn kill_process_safely(_pid: u32) -> Result<(), ExecutionError> {
    // 在非Unix平台上，我们无法直接杀死进程
    Err(ExecutionError::Failed(anyhow::anyhow!(
        "Process termination not supported on this platform"
    )))
}

/// 运行一个可取消的任务
pub async fn run_until_cancelled<F, Fut>(
    future: F,
    cancellation_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
{
    tokio::select! {
        result = future() => {
            result
        }
        _ = cancellation_token.cancelled() => {
            info!("Task cancelled by shutdown signal");
            Ok(())
        }
    }
}

/// 等待多个任务完成，支持超时
pub async fn wait_for_tasks_with_timeout(
    handles: Vec<tokio::task::JoinHandle<()>>,
    timeout_secs: u64,
) {
    let timeout = tokio::time::Duration::from_secs(timeout_secs);
    info!(
        "Waiting for {} tasks to complete (timeout: {}s)",
        handles.len(),
        timeout_secs
    );

    let abort_handles: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();

    let result = tokio::time::timeout(timeout, async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;

    if result.is_err() {
        warn!(
            remaining = abort_handles.len(),
            "Timeout waiting for tasks to complete, aborting remaining"
        );
        for h in abort_handles {
            h.abort();
        }
    }
}
