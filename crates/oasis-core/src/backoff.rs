//! 指数退避重试策略
//!
//! 这个模块基于 backon crate 提供了生产级的重试机制配置。
//! 通过单一事实来源 `BackoffConfig` 统一管理策略，既用于生成执行器，也用于预估延迟。

use backon::ExponentialBuilder;
use std::time::Duration;

/// 重试策略配置
/// 封装了所有控制指数退避的参数，作为单一事实来源。
#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    /// 初始（最小）等待时间
    pub min_delay: Duration,
    /// 最大等待时间上限
    pub max_delay: Duration,
    /// 指数增长因子 (e.g., 2.0 表示每次翻倍)
    pub factor: f32,
    /// 最大重试次数 (None 表示无限重试，通常不建议)
    pub max_times: Option<usize>,
}

impl BackoffConfig {
    /// 构建 backon 的 ExponentialBuilder
    pub fn build(&self) -> ExponentialBuilder {
        let mut builder = ExponentialBuilder::default()
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_factor(self.factor);

        if let Some(times) = self.max_times {
            builder = builder.with_max_times(times);
        } else {
            builder = builder.with_max_times(usize::MAX);
        }

        builder
    }

    /// 估算指定尝试次数的延迟时间 (不包含随机 Jitter)
    /// 用于日志记录或 UI 展示
    pub fn estimate_delay(&self, attempt: u32) -> Duration {
        let base_ms = self.min_delay.as_millis() as f64;
        let max_ms = self.max_delay.as_millis() as f64;

        // 显式转换 f32 -> f64 以进行 powi 计算
        let factor = self.factor as f64;
        let raw_ms = base_ms * factor.powi(attempt as i32);
        let capped_ms = raw_ms.min(max_ms);

        Duration::from_millis(capped_ms as u64)
    }
}

/// 预定义的重试策略
pub fn heartbeat_backoff() -> BackoffConfig {
    BackoffConfig {
        min_delay: Duration::from_millis(500),
        max_delay: Duration::from_millis(30000),
        factor: 2.0,
        max_times: None, // 无限重试
    }
}

pub fn kv_operations_backoff() -> BackoffConfig {
    BackoffConfig {
        min_delay: Duration::from_millis(200),
        max_delay: Duration::from_millis(5000),
        factor: 1.5,
        max_times: Some(10),
    }
}

pub fn network_publish_backoff() -> BackoffConfig {
    BackoffConfig {
        min_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(2000),
        factor: 2.0,
        max_times: Some(5),
    }
}

/// 用于客户端网络连接的重试策略（如 gRPC 初次连接），支持更长等待窗口
pub fn network_connect_backoff() -> BackoffConfig {
    BackoffConfig {
        min_delay: Duration::from_millis(200),
        max_delay: Duration::from_millis(5000),
        factor: 2.0,
        max_times: Some(10),
    }
}

pub fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        min_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(5000),
        factor: 1.5,
        max_times: Some(5),
    }
}

/// 兼容旧 API 的辅助函数，但现在有了真正的配置支持
pub fn delay_for_attempt(config: &BackoffConfig, attempt: u32) -> Duration {
    config.estimate_delay(attempt)
}
