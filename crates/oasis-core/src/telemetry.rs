use time::UtcOffset;
use tracing_subscriber::{EnvFilter, fmt, prelude::*, reload};
use once_cell::sync::OnceCell;

// 全局可重载的日志过滤器句柄
static LOG_FILTER_HANDLE: OnceCell<reload::Handle<EnvFilter, tracing_subscriber::Registry>> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct LogConfig {
    pub level: String,  // info|debug
    pub format: String, // text|json
    pub no_ansi: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

impl From<&LogConfig> for (LogLevel, LogFormat) {
    fn from(cfg: &LogConfig) -> Self {
        let level = match cfg.level.to_lowercase().as_str() {
            "trace" => LogLevel::Trace,
            "debug" => LogLevel::Debug,
            "warn" => LogLevel::Warn,
            "error" => LogLevel::Error,
            _ => LogLevel::Info,
        };
        let format = if cfg.format.eq_ignore_ascii_case("json") {
            LogFormat::Json
        } else {
            LogFormat::Text
        };
        (level, format)
    }
}

fn build_filter(level: &str) -> EnvFilter {
    let base = match level.to_lowercase().as_str() {
        "trace" => "trace",
        "debug" => "debug",
        "warn" => "warn",
        "error" => "error",
        _ => "info",
    };

    EnvFilter::new(base).add_directive(
        "async_nats=warn".parse().unwrap_or_else(|_| {
            tracing::warn!("Failed to parse async_nats directive, using default");
            "async_nats=warn"
                .parse()
                .expect("async_nats=warn is a valid log level")
        }),
    )
}

/// 使用提供的配置初始化 tracing（支持运行时动态调整日志级别）
pub fn init_tracing_with(cfg: &LogConfig) {
    let (lvl_enum, fmt_enum): (LogLevel, LogFormat) = cfg.into();
    let lvl_str = match lvl_enum {
        LogLevel::Trace => "trace",
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warn => "warn",
        LogLevel::Error => "error",
    };

    let filter = build_filter(lvl_str);
    let (filter_layer, handle) = reload::Layer::new(filter);

    let base = fmt::layer().with_target(true).with_ansi(!cfg.no_ansi);
    let fmt_layer = match fmt_enum {
        LogFormat::Json => base.json().boxed(),
        LogFormat::Text => {
            let offset = UtcOffset::from_hms(8, 0, 0).unwrap_or(UtcOffset::UTC);
            base.with_timer(fmt::time::OffsetTime::new(
                offset,
                time::format_description::well_known::Rfc3339,
            ))
            .boxed()
        }
    };

    let _ = tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .try_init();

    // 存储可重载句柄（若重复初始化，则忽略）
    let _ = LOG_FILTER_HANDLE.set(handle);
}

/// 动态更新日志级别（仅级别可热更新，格式与 ANSI 目前仍需重启）
pub fn set_log_level(level: &str) {
    if let Some(handle) = LOG_FILTER_HANDLE.get() {
        let new_filter = build_filter(level);
        if let Err(e) = handle.reload(new_filter) {
            tracing::warn!("Failed to reload log filter: {}", e);
        } else {
            tracing::info!("Log level updated to {} via hot-reload", level);
        }
    } else {
        tracing::warn!("Log filter handle not initialized; cannot hot-reload log level");
    }
}
