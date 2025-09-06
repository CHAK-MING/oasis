use futures::future::pending;
use governor::{Quota, RateLimiter, clock::DefaultClock, state::InMemoryState};
use std::pin::Pin;
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
        let max_ops = NonZeroU32::new(max_operations)
            .ok_or_else(|| CoreError::config_error("max_operations must be greater than 0"))?;

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
            .unwrap_or_else(|| {
                tracing::error!("Invalid time window, using default");
                Quota::with_period(Duration::from_secs(1)).unwrap()
            })
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
        let mut wait_fut = Box::pin(self.limiter.until_ready());
        let mut timeout_fut = match self.max_wait_time {
            Some(dur) => Box::pin(tokio::time::sleep(dur))
                as Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
            None => Box::pin(pending()) as Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
        };
        let mut cancel_fut = match cancellation_token {
            Some(token) => Box::pin(token.cancelled_owned())
                as Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
            None => Box::pin(pending()) as Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
        };

        tokio::select! {
            _ = &mut wait_fut => Ok(()),
            _ = &mut timeout_fut => {
                warn!(operation = operation_name, "Rate limit timeout");
                Err(CoreError::internal_error(format!("Rate limit timeout for {}", operation_name)))
            },
            _ = &mut cancel_fut => {
                debug!(operation = operation_name, "Rate limit cancelled");
                Err(CoreError::internal_error(format!("Rate limit cancelled for {}", operation_name)))
            },
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
            return Err(CoreError::internal_error(
                "Operation cancelled before execution",
            ));
        }
    }

    operation().await
}
