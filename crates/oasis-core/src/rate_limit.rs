use governor::{Quota, RateLimiter, clock::DefaultClock, state::InMemoryState};
use std::{num::NonZeroU32, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::error::{CoreError, Result};

/// 简化的限流器类型
pub type SimpleRateLimiter = RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock>;

/// 限流配置
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub max_operations: NonZeroU32,
    pub time_window: Duration,
    pub max_wait_time: Option<Duration>,
}

impl RateLimitConfig {
    /// 创建新的限流配置
    pub fn new(max_operations: u32, time_window: Duration) -> Result<Self> {
        let max_ops = NonZeroU32::new(max_operations).ok_or_else(|| CoreError::Config {
            message: "max_operations must be greater than 0".to_string(),
        })?;

        Ok(Self {
            max_operations: max_ops,
            time_window,
            max_wait_time: Some(time_window),
        })
    }

    /// 预设配置
    pub fn nats() -> Self {
        Self::new(100, Duration::from_secs(1))
            .unwrap()
            .with_max_wait(Duration::from_millis(500))
    }

    pub fn heartbeat() -> Self {
        Self::new(10, Duration::from_secs(1))
            .unwrap()
            .with_max_wait(Duration::from_millis(100))
    }

    pub fn task_publish() -> Self {
        Self::new(20, Duration::from_secs(1))
            .unwrap()
            .with_max_wait(Duration::from_secs(1))
    }

    /// 设置最大等待时间
    pub fn with_max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait_time = Some(max_wait);
        self
    }

    /// 创建限流器
    pub fn build(&self) -> SimpleRateLimiter {
        let quota = Quota::with_period(self.time_window)
            .expect("valid time window")
            .allow_burst(self.max_operations);
        RateLimiter::direct(quota)
    }
}

/// 带取消支持的限流器
#[derive(Debug)]
pub struct CancellableRateLimiter {
    limiter: SimpleRateLimiter,
    max_wait_time: Option<Duration>,
}

impl CancellableRateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            limiter: config.build(),
            max_wait_time: config.max_wait_time,
        }
    }

    /// 等待许可
    pub async fn wait_for_permission(
        &self,
        cancellation_token: Option<CancellationToken>,
        operation_name: &str,
    ) -> Result<()> {
        // 快速路径：立即检查
        if self.limiter.check().is_ok() {
            debug!(operation = operation_name, "Rate limit passed");
            return Ok(());
        }

        let wait_future = self.limiter.until_ready();

        match (self.max_wait_time, cancellation_token) {
            (Some(max_wait), Some(token)) => {
                let timeout_fut = tokio::time::sleep(max_wait);
                tokio::pin!(timeout_fut);
                tokio::select! {
                    _ = wait_future => Ok(()),
                    _ = &mut timeout_fut => {
                        warn!(operation = operation_name, "Rate limit timeout");
                        Err(CoreError::Internal { message: format!("Rate limit timeout for {}", operation_name) })
                    }
                    _ = token.cancelled() => {
                        debug!(operation = operation_name, "Rate limit cancelled");
                        Err(CoreError::Internal { message: format!("Rate limit cancelled for {}", operation_name) })
                    }
                }
            }
            (Some(max_wait), None) => {
                let timeout_fut = tokio::time::sleep(max_wait);
                tokio::pin!(timeout_fut);
                tokio::select! {
                    _ = wait_future => Ok(()),
                    _ = &mut timeout_fut => {
                        warn!(operation = operation_name, "Rate limit timeout");
                        Err(CoreError::Internal { message: format!("Rate limit timeout for {}", operation_name) })
                    }
                }
            }
            (None, Some(token)) => {
                tokio::select! {
                    _ = wait_future => Ok(()),
                    _ = token.cancelled() => {
                        debug!(operation = operation_name, "Rate limit cancelled");
                        Err(CoreError::Internal { message: format!("Rate limit cancelled for {}", operation_name) })
                    }
                }
            }
            (None, None) => {
                wait_future.await;
                Ok(())
            }
        }
    }

    /// 立即尝试获取许可
    pub fn try_permission(&self) -> bool {
        self.limiter.check().is_ok()
    }
}

/// 限流器集合
#[derive(Debug)]
pub struct RateLimiterCollection {
    pub nats: CancellableRateLimiter,
    pub heartbeat: CancellableRateLimiter,
    pub task_publish: CancellableRateLimiter,
}

impl Default for RateLimiterCollection {
    fn default() -> Self {
        Self {
            nats: CancellableRateLimiter::new(RateLimitConfig::nats()),
            heartbeat: CancellableRateLimiter::new(RateLimitConfig::heartbeat()),
            task_publish: CancellableRateLimiter::new(RateLimitConfig::task_publish()),
        }
    }
}

/// 执行限流操作
pub async fn rate_limited_operation<F, Fut, T>(
    rate_limiter: &CancellableRateLimiter,
    operation: F,
    cancellation_token: Option<CancellationToken>,
    operation_name: &str,
) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    rate_limiter
        .wait_for_permission(cancellation_token.clone(), operation_name)
        .await?;

    if let Some(ref token) = cancellation_token {
        if token.is_cancelled() {
            return Err(CoreError::Internal {
                message: "Operation cancelled before execution".to_string(),
            });
        }
    }

    operation().await
}
