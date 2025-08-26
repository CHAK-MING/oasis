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
    pub tls: TlsConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub agent: AgentConfig,
}

impl OasisConfig {
    /// 从指定路径或当前目录中的 oasis.toml 加载配置；如果文件不存在则返回默认配置
    pub fn load_config(path: Option<&str>) -> Result<Self, anyhow::Error> {
        use std::path::Path;

        let (content, base_dir): (String, std::path::PathBuf) = if let Some(p) = path {
            let p = Path::new(p);
            let content = std::fs::read_to_string(p)
                .map_err(|e| anyhow::anyhow!("Failed to read config file at {}: {}", p.display(), e))?;
            let base = p.parent().unwrap_or_else(|| Path::new("."));
            (content, base.to_path_buf())
        } else {
            let default_path = Path::new("oasis.toml");
            if default_path.exists() {
                let content = std::fs::read_to_string(default_path)
                    .map_err(|e| anyhow::anyhow!("Failed to read config file oasis.toml: {}", e))?;
                (content, std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf()))
            } else {
                return Ok(OasisConfig::default());
            }
        };

        let mut cfg: OasisConfig = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse oasis.toml: {}", e))?;

        // Resolve relative paths against base_dir
        cfg.resolve_relative_paths(&base_dir);
        Ok(cfg)
    }
}

/// Telemetry configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// NATS 服务器地址
    #[serde(default = "default_nats_url")]
    pub url: String,
    /// NATS CA 证书路径
    #[serde(default = "default_nats_ca_path")]
    pub ca_path: PathBuf,
    /// NATS 客户端证书路径
    #[serde(default = "default_nats_client_cert_path")]
    pub client_cert_path: PathBuf,
    /// NATS 客户端私钥路径
    #[serde(default = "default_nats_client_key_path")]
    pub client_key_path: PathBuf,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: default_nats_url(),
            ca_path: default_nats_ca_path(),
            client_cert_path: default_nats_client_cert_path(),
            client_key_path: default_nats_client_key_path(),
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default = "default_grpc_ca_path")]
    pub grpc_ca_path: PathBuf,
    #[serde(default = "default_grpc_server_cert_path")]
    pub grpc_server_cert_path: PathBuf,
    #[serde(default = "default_grpc_server_key_path")]
    pub grpc_server_key_path: PathBuf,
    #[serde(default = "default_grpc_client_cert_path")]
    pub grpc_client_cert_path: PathBuf,
    #[serde(default = "default_grpc_client_key_path")]
    pub grpc_client_key_path: PathBuf,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            grpc_ca_path: default_grpc_ca_path(),
            grpc_server_cert_path: default_grpc_server_cert_path(),
            grpc_server_key_path: default_grpc_server_key_path(),
            grpc_client_cert_path: default_grpc_client_cert_path(),
            grpc_client_key_path: default_grpc_client_key_path(),
        }
    }
}

/// Server-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_heartbeat_ttl")]
    pub heartbeat_ttl_sec: u64,
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_sec: u64,
    #[serde(default = "default_renewal_interval")]
    pub renewal_interval_sec: u64,
}

/// Agent-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default = "default_server_url")]
    pub server_url: String,
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

fn default_nats_ca_path() -> PathBuf {
    PathBuf::from("certs/nats-ca.pem")
}

fn default_nats_client_cert_path() -> PathBuf {
    PathBuf::from("certs/nats-client.pem")
}

fn default_nats_client_key_path() -> PathBuf {
    PathBuf::from("certs/nats-client-key.pem")
}

fn default_grpc_ca_path() -> PathBuf {
    PathBuf::from("certs/grpc-ca.pem")
}

fn default_grpc_server_cert_path() -> PathBuf {
    PathBuf::from("certs/grpc-server.pem")
}

fn default_grpc_server_key_path() -> PathBuf {
    PathBuf::from("certs/grpc-server-key.pem")
}

fn default_grpc_client_cert_path() -> PathBuf {
    PathBuf::from("certs/grpc-client.pem")
}

fn default_grpc_client_key_path() -> PathBuf {
    PathBuf::from("certs/grpc-client-key.pem")
}

fn default_heartbeat_ttl() -> u64 {
    60
}

fn default_lease_ttl() -> u64 {
    30
}

fn default_renewal_interval() -> u64 {
    5
}

fn default_server_url() -> String {
    "http://127.0.0.1:50051".to_string()
}

fn default_heartbeat_interval() -> u64 {
    30
}

fn default_fact_collection_interval() -> u64 {
    300
}

impl OasisConfig {
    fn resolve_relative_paths(&mut self, base_dir: &std::path::Path) {
        // Helper to make path absolute if it's relative
        fn make_absolute(path: &mut PathBuf, base: &std::path::Path) {
            if path.is_relative() {
                *path = base.join(&*path);
            }
        }

        make_absolute(&mut self.data_dir, base_dir);

        // NATS cert paths
        make_absolute(&mut self.nats.ca_path, base_dir);
        make_absolute(&mut self.nats.client_cert_path, base_dir);
        make_absolute(&mut self.nats.client_key_path, base_dir);

        // gRPC cert paths
        make_absolute(&mut self.tls.grpc_ca_path, base_dir);
        make_absolute(&mut self.tls.grpc_server_cert_path, base_dir);
        make_absolute(&mut self.tls.grpc_server_key_path, base_dir);
        make_absolute(&mut self.tls.grpc_client_cert_path, base_dir);
        make_absolute(&mut self.tls.grpc_client_key_path, base_dir);
    }
}
