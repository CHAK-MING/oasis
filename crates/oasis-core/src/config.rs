//! The unified configuration module for the entire Oasis application.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// The unified configuration for the Oasis application.
/// This structure is loaded from the oasis.toml file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct OasisConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    pub nats: NatsConfig,
    #[serde(default)]
    pub grpc: GrpcConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub agent: AgentConfig,
}

impl OasisConfig {
    /// 从指定路径或当前目录中的 oasis.toml 加载配置；支持以 OASIS_ 为前缀的环境变量覆盖（Figment）
    pub fn load_config(path: Option<&str>) -> Result<Self, anyhow::Error> {
        use figment::{Figment, providers::Env, providers::Format, providers::Toml};
        use std::path::Path;

        // 基础：默认配置
        let mut figment = Figment::from(figment::providers::Serialized::defaults(
            OasisConfig::default(),
        ));

        // 文件层：显式路径优先，否则尝试工作目录下 oasis.toml
        let base_dir: std::path::PathBuf;
        if let Some(p) = path {
            let p = Path::new(p);
            base_dir = p.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
            figment = figment.merge(Toml::file(p));
        } else {
            let default_path = Path::new("oasis.toml");
            if default_path.exists() {
                base_dir = std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());
                figment = figment.merge(Toml::file(default_path));
            } else {
                base_dir = std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());
            }
        }

        // 环境变量层：使用新标准前缀
        figment = figment.merge(Env::prefixed("OASIS__").split("__"));

        // 提取并解析
        let mut cfg: OasisConfig = figment
            .extract()
            .map_err(|e| anyhow::anyhow!("Failed to load config via Figment: {}", e))?;

        // 解析相对路径
        cfg.resolve_relative_paths(&base_dir);

        // 验证配置
        cfg.validate()?;
        Ok(cfg)
    }

    /// 验证配置参数的有效性
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        // 验证监听地址格式
        if let Err(_) = self.listen_addr.parse::<std::net::SocketAddr>() {
            return Err(anyhow::anyhow!(
                "Invalid listen_addr format: {}",
                self.listen_addr
            ));
        }

        // 验证 Agent 相关配置
        if self.agent.heartbeat_interval_sec < 5 || self.agent.heartbeat_interval_sec > 300 {
            return Err(anyhow::anyhow!(
                "heartbeat_interval_sec must be between 5 and 300"
            ));
        }

        if self.agent.fact_collection_interval_sec < 30
            || self.agent.fact_collection_interval_sec > 3600
        {
            return Err(anyhow::anyhow!(
                "fact_collection_interval_sec must be between 30 and 3600"
            ));
        }

        Ok(())
    }

    /// 构建 gRPC URL，支持 IPv6 地址
    pub fn build_grpc_url(&self) -> String {
        if self.listen_addr.contains('[') && self.listen_addr.contains(']') {
            // IPv6 地址，保持方括号格式
            format!("https://{}", self.listen_addr)
        } else {
            // IPv4 地址或主机名
            format!("https://{}", self.listen_addr)
        }
    }
}

/// Telemetry configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default = "default_log_no_ansi")]
    pub log_no_ansi: bool,
}

/// NATS configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// NATS server URL
    #[serde(default = "default_nats_url")]
    pub url: String,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: default_nats_url(),
        }
    }
}

/// gRPC configuration (shared by server listen and agent connect URL)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    #[serde(default = "default_grpc_url")]
    pub url: String,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            url: default_grpc_url(),
        }
    }
}

/// TLS configuration with fixed certificate paths.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Certificate root directory
    #[serde(default = "default_certs_dir")]
    pub certs_dir: PathBuf,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            certs_dir: default_certs_dir(),
        }
    }
}

impl TlsConfig {
    /// Get NATS CA certificate path
    pub fn nats_ca_path(&self) -> PathBuf {
        self.certs_dir.join("nats-ca.pem")
    }

    /// Get NATS client certificate path
    pub fn nats_client_cert_path(&self) -> PathBuf {
        self.certs_dir.join("nats-client.pem")
    }

    /// Get NATS client key path
    pub fn nats_client_key_path(&self) -> PathBuf {
        self.certs_dir.join("nats-client-key.pem")
    }

    /// Get gRPC CA certificate path
    pub fn grpc_ca_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-ca.pem")
    }

    /// Get gRPC server certificate path
    pub fn grpc_server_cert_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-server.pem")
    }

    /// Get gRPC server key path
    pub fn grpc_server_key_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-server-key.pem")
    }

    /// Get gRPC client certificate path
    pub fn grpc_client_cert_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-client.pem")
    }

    /// Get gRPC client key path
    pub fn grpc_client_key_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-client-key.pem")
    }
}

/// Server-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_heartbeat_ttl")]
    pub heartbeat_ttl_sec: u64,
}

/// Agent-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_sec: u64,
    #[serde(default = "default_fact_collection_interval")]
    pub fact_collection_interval_sec: u64,
}

// --- Default value functions ---

fn default_listen_addr() -> String {
    "0.0.0.0:50051".to_string()
}

fn default_data_dir() -> PathBuf {
    std::env::temp_dir().join("oasis")
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "text".to_string()
}

fn default_log_no_ansi() -> bool {
    false
}

fn default_nats_url() -> String {
    "tls://127.0.0.1:4222".to_string()
}

fn default_certs_dir() -> PathBuf {
    PathBuf::from("certs")
}

fn default_heartbeat_ttl() -> u64 {
    60
}

fn default_grpc_url() -> String {
    "https://127.0.0.1:50051".to_string()
}

fn default_heartbeat_interval() -> u64 {
    30
}

fn default_fact_collection_interval() -> u64 {
    300
}

impl OasisConfig {
    pub fn resolve_relative_paths(&mut self, base_dir: &std::path::Path) {
        // Helper to make path absolute if it's relative
        fn make_absolute(path: &mut PathBuf, base: &std::path::Path) {
            if path.is_relative() {
                *path = base.join(&*path);
            }
        }

        make_absolute(&mut self.data_dir, base_dir);
        make_absolute(&mut self.tls.certs_dir, base_dir);
    }
}
