use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 所有组件共享的基础配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonConfig {
    pub nats: NatsConfig,
    pub tls: TlsConfig,
    pub telemetry: TelemetryConfig,
}

impl Default for CommonConfig {
    fn default() -> Self {
        Self {
            nats: NatsConfig::default(),
            tls: TlsConfig::default(),
            telemetry: TelemetryConfig::default(),
        }
    }
}


/// NATS 连接配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    #[serde(default = "default_nats_url")]
    pub url: String,
    pub creds_file: Option<PathBuf>,
    pub tls_ca_file: Option<PathBuf>,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    #[serde(default)]
    pub tls_required: bool,
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_sec: u64,
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_sec: u64,
    #[serde(default = "default_max_reconnects")]
    pub max_reconnects: Option<usize>,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: default_nats_url(),
            creds_file: None,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_required: false,
            connection_timeout_sec: default_connection_timeout(),
            reconnect_delay_sec: default_reconnect_delay(),
            max_reconnects: default_max_reconnects(),
        }
    }
}

/// TLS 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    pub ca_cert: Option<PathBuf>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
    #[serde(default)]
    pub verify_hostname: bool,
    #[serde(default = "default_tls_min_version")]
    pub min_version: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            verify_hostname: true,
            min_version: default_tls_min_version(),
        }
    }
}

/// 遥测配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default)]
    pub log_no_ansi: bool,
    #[serde(default)]
    pub log_file: Option<PathBuf>,
    #[serde(default)]
    pub metrics_enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    #[serde(default)]
    pub tracing_enabled: bool,
    #[serde(default)]
    pub tracing_endpoint: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            log_format: default_log_format(),
            log_no_ansi: false,
            log_file: None,
            metrics_enabled: false,
            metrics_port: default_metrics_port(),
            tracing_enabled: false,
            tracing_endpoint: None,
        }
    }
}

// 默认值函数
fn default_nats_url() -> String {
    "nats://localhost:4222".to_string()
}

fn default_connection_timeout() -> u64 {
    5
}

fn default_reconnect_delay() -> u64 {
    1
}

fn default_max_reconnects() -> Option<usize> {
    None
}

fn default_tls_min_version() -> String {
    "1.2".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "text".to_string()
}

fn default_metrics_port() -> u16 {
    9090
}

impl CommonConfig {
    /// 验证配置的有效性
    pub fn validate(&self) -> crate::error::Result<()> {
        // 验证 NATS URL 格式
        if !self.nats.url.starts_with("nats://") && !self.nats.url.starts_with("nats://") {
            return Err(crate::error::CoreError::Config {
                message: "Invalid NATS URL format".to_string(),
            });
        }

        // 验证 TLS 配置
        if self.tls.enabled {
            if self.tls.client_cert.is_some() && self.tls.client_key.is_none() {
                return Err(crate::error::CoreError::Config {
                    message: "TLS client certificate requires private key".to_string(),
                });
            }
            if self.tls.client_key.is_some() && self.tls.client_cert.is_none() {
                return Err(crate::error::CoreError::Config {
                    message: "TLS private key requires client certificate".to_string(),
                });
            }
        }

        // 验证日志级别
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.telemetry.log_level.as_str()) {
            return Err(crate::error::CoreError::Config {
                message: format!("Invalid log level: {}", self.telemetry.log_level),
            });
        }

        // 验证日志格式
        let valid_formats = ["text", "json"];
        if !valid_formats.contains(&self.telemetry.log_format.as_str()) {
            return Err(crate::error::CoreError::Config {
                message: format!("Invalid log format: {}", self.telemetry.log_format),
            });
        }

        Ok(())
    }

    /// 合并另一个配置
    pub fn merge(&mut self, other: &CommonConfig) {
        // 深度合并逻辑
        if other.nats.url != default_nats_url() {
            self.nats.url = other.nats.url.clone();
        }
        if other.nats.creds_file.is_some() {
            self.nats.creds_file = other.nats.creds_file.clone();
        }
        if other.nats.tls_ca_file.is_some() {
            self.nats.tls_ca_file = other.nats.tls_ca_file.clone();
        }
        if other.nats.tls_cert_file.is_some() {
            self.nats.tls_cert_file = other.nats.tls_cert_file.clone();
        }
        if other.nats.tls_key_file.is_some() {
            self.nats.tls_key_file = other.nats.tls_key_file.clone();
        }
        if other.nats.tls_required {
            self.nats.tls_required = true;
        }
        if other.nats.connection_timeout_sec != default_connection_timeout() {
            self.nats.connection_timeout_sec = other.nats.connection_timeout_sec;
        }
        if other.nats.reconnect_delay_sec != default_reconnect_delay() {
            self.nats.reconnect_delay_sec = other.nats.reconnect_delay_sec;
        }
        if other.nats.max_reconnects != default_max_reconnects() {
            self.nats.max_reconnects = other.nats.max_reconnects;
        }

        // TLS 配置合并
        if other.tls.enabled {
            self.tls.enabled = true;
        }
        if other.tls.ca_cert.is_some() {
            self.tls.ca_cert = other.tls.ca_cert.clone();
        }
        if other.tls.client_cert.is_some() {
            self.tls.client_cert = other.tls.client_cert.clone();
        }
        if other.tls.client_key.is_some() {
            self.tls.client_key = other.tls.client_key.clone();
        }
        if !other.tls.verify_hostname {
            self.tls.verify_hostname = false;
        }
        if other.tls.min_version != default_tls_min_version() {
            self.tls.min_version = other.tls.min_version.clone();
        }

        // 遥测配置合并
        if other.telemetry.log_level != default_log_level() {
            self.telemetry.log_level = other.telemetry.log_level.clone();
        }
        if other.telemetry.log_format != default_log_format() {
            self.telemetry.log_format = other.telemetry.log_format.clone();
        }
        if other.telemetry.log_no_ansi {
            self.telemetry.log_no_ansi = true;
        }
        if other.telemetry.log_file.is_some() {
            self.telemetry.log_file = other.telemetry.log_file.clone();
        }
        if other.telemetry.metrics_enabled {
            self.telemetry.metrics_enabled = true;
        }
        if other.telemetry.metrics_port != default_metrics_port() {
            self.telemetry.metrics_port = other.telemetry.metrics_port;
        }
        if other.telemetry.tracing_enabled {
            self.telemetry.tracing_enabled = true;
        }
        if other.telemetry.tracing_endpoint.is_some() {
            self.telemetry.tracing_endpoint = other.telemetry.tracing_endpoint.clone();
        }
    }
}
