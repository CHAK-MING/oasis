use futures::future::pending;
use governor::{Quota, RateLimiter, clock::DefaultClock, state::InMemoryState};
use std::pin::Pin;
use std::{num::NonZeroU32, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::config::{RateLimitBucketSettings, RateLimitSettings};
use crate::error::{CoreError, Result};

/// 限流器类型
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

    pub fn from_settings(settings: &RateLimitBucketSettings) -> Result<Self> {
        let max_operations = NonZeroU32::new(settings.max_operations)
            .ok_or_else(|| CoreError::config_error("max_operations must be greater than 0"))?;

        Ok(Self {
            max_operations,
            time_window: Duration::from_millis(settings.time_window_ms),
            max_wait_time: settings.max_wait_time_ms.map(Duration::from_millis),
        })
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
        Self::from_settings(&RateLimitSettings::default())
            .expect("default rate limit settings are valid")
    }
}

impl RateLimiterCollection {
    pub fn from_settings(settings: &RateLimitSettings) -> Result<Self> {
        Ok(Self {
            nats: CancellableRateLimiter::new(RateLimitConfig::from_settings(&settings.nats)?),
            heartbeat: CancellableRateLimiter::new(RateLimitConfig::from_settings(
                &settings.heartbeat,
            )?),
            task_publish: CancellableRateLimiter::new(RateLimitConfig::from_settings(
                &settings.task_publish,
            )?),
        })
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

    if let Some(token) = cancellation_token.as_ref() {
        if token.is_cancelled() {
            return Err(CoreError::internal_error(
                "Operation cancelled before execution",
            ));
        }
    }

    operation().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RateLimitBucketSettings, RateLimitSettings};
    use crate::error::CoreError;

    #[test]
    fn test_rate_limiter_collection_from_settings_uses_configured_values() {
        let settings = RateLimitSettings {
            nats: RateLimitBucketSettings {
                max_operations: 7,
                time_window_ms: 250,
                max_wait_time_ms: Some(50),
            },
            heartbeat: RateLimitBucketSettings {
                max_operations: 3,
                time_window_ms: 750,
                max_wait_time_ms: Some(25),
            },
            task_publish: RateLimitBucketSettings {
                max_operations: 11,
                time_window_ms: 2_000,
                max_wait_time_ms: None,
            },
        };

        let collection = RateLimiterCollection::from_settings(&settings).unwrap();

        assert_eq!(
            collection.nats.max_wait_time,
            Some(Duration::from_millis(50))
        );
        assert_eq!(
            collection.heartbeat.max_wait_time,
            Some(Duration::from_millis(25))
        );
        assert_eq!(collection.task_publish.max_wait_time, None);
        assert!(collection.nats.try_permission());
        assert!(collection.heartbeat.try_permission());
        assert!(collection.task_publish.try_permission());
    }

    #[test]
    fn test_rate_limiter_collection_rejects_zero_operations() {
        let settings = RateLimitSettings {
            nats: RateLimitBucketSettings {
                max_operations: 0,
                time_window_ms: 250,
                max_wait_time_ms: Some(50),
            },
            heartbeat: RateLimitBucketSettings {
                max_operations: 3,
                time_window_ms: 750,
                max_wait_time_ms: Some(25),
            },
            task_publish: RateLimitBucketSettings {
                max_operations: 11,
                time_window_ms: 2_000,
                max_wait_time_ms: None,
            },
        };

        let err = RateLimiterCollection::from_settings(&settings).unwrap_err();
        match err {
            CoreError::Config { message, .. } => {
                assert!(message.contains("max_operations must be greater than 0"));
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }
}
