use time::UtcOffset;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

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

/// 使用提供的配置初始化 tracing（推荐：TOML 驱动）
pub fn init_tracing_with(cfg: &LogConfig) {
    let (lvl_enum, fmt_enum): (LogLevel, LogFormat) = cfg.into();
    let lvl_str = match lvl_enum {
        LogLevel::Trace => "trace",
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warn => "warn",
        LogLevel::Error => "error",
    };

    let filter = EnvFilter::new(lvl_str);
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
        .with(filter)
        .with(fmt_layer)
        .try_init();
}
