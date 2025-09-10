//! 指数退避重试策略
//!
//! 这个模块基于 backoff crate 提供了轻便的重试机制。

use backoff::{Error as BackoffError, ExponentialBackoff, future::retry};
use std::time::Duration;

/// 预定义的重试策略
pub fn heartbeat_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        initial_interval: Duration::from_millis(500),
        max_interval: Duration::from_millis(30000),
        multiplier: 2.0,
        max_elapsed_time: Some(Duration::from_secs(300)),
        randomization_factor: 0.1,
        ..Default::default()
    }
}

pub fn kv_operations_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        initial_interval: Duration::from_millis(200),
        max_interval: Duration::from_millis(5000),
        multiplier: 1.5,
        max_elapsed_time: Some(Duration::from_secs(60)),
        randomization_factor: 0.2,
        ..Default::default()
    }
}

pub fn network_publish_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_millis(2000),
        multiplier: 2.0,
        max_elapsed_time: Some(Duration::from_secs(30)),
        randomization_factor: 0.15,
        ..Default::default()
    }
}

/// 用于客户端网络连接的重试策略（如 gRPC 初次连接），支持更长等待窗口
pub fn network_connect_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        initial_interval: Duration::from_millis(200),
        max_interval: Duration::from_millis(5000),
        multiplier: 2.0,
        max_elapsed_time: Some(Duration::from_secs(5)),
        randomization_factor: 0.2,
        ..Default::default()
    }
}

pub fn fast_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_millis(5000),
        multiplier: 1.5,
        max_elapsed_time: Some(Duration::from_secs(30)),
        randomization_factor: 0.2,
        ..Default::default()
    }
}

/// 便捷方法：执行重试操作
pub async fn execute_with_backoff<F, Fut, T, E>(
    operation: F,
    backoff: ExponentialBackoff,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display + Send + 'static,
{
    let mut op = operation;
    let wrapped_operation = move || {
        let fut = op();
        async move { fut.await.map_err(BackoffError::transient) }
    };

    // backoff::future::retry 在失败时返回底层错误类型 E
    retry(backoff, wrapped_operation).await
}

/// 带错误分类的重试：仅对被判定为“瞬态”的错误进行重试；否则立即失败
pub async fn execute_with_backoff_selective<F, Fut, T, E>(
    mut operation: F,
    backoff: ExponentialBackoff,
    is_transient: std::sync::Arc<dyn Fn(&E) -> bool + Send + Sync + 'static>,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display + Send + 'static,
{
    let wrapped_operation = move || {
        let fut = operation();
        let classify = is_transient.clone();
        async move {
            match fut.await {
                Ok(v) => Ok(v),
                Err(e) => {
                    if (classify)(&e) {
                        Err(BackoffError::transient(e))
                    } else {
                        Err(BackoffError::permanent(e))
                    }
                }
            }
        }
    };

    retry(backoff, wrapped_operation).await
}

pub fn delay_for_attempt(cfg: &ExponentialBackoff, attempt: u32) -> Duration {
    // 使用毫秒为单位进行浮点计算，再裁剪到上限
    let base_ms = cfg.initial_interval.as_millis() as f64;
    let factor = cfg.multiplier.powi(attempt as i32);
    let raw_ms = base_ms * factor;
    let capped_ms = raw_ms.min(cfg.max_interval.as_millis() as f64);
    Duration::from_millis(capped_ms as u64)
}
