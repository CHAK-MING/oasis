use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 整个 Oasis 统一配置
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct OasisConfig {
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

        Ok(cfg)
    }

    pub fn build_grpc_url(&self) -> Result<String, anyhow::Error> {
        let url = self.grpc.url.trim().to_string();
        // 这里是TLS 连接，必须是输入 https，否则返回错误
        if !url.starts_with("https://") {
            return Err(anyhow::anyhow!("gRPC URL 必须是 https:// 开头的"));
        }
        Ok(url)
    }
}

/// 遥测配置
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default = "default_log_no_ansi")]
    pub log_no_ansi: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            log_format: default_log_format(),
            log_no_ansi: default_log_no_ansi(),
        }
    }
}

/// NATS 配置
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// NATS 服务器 URL
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

/// gRPC 配置 (服务器监听和代理连接 URL 共享)
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

/// TLS 配置
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// 证书根目录
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
    /// 获取 NATS CA 证书路径
    pub fn nats_ca_path(&self) -> PathBuf {
        self.certs_dir.join("nats-ca.pem")
    }

    /// 获取 NATS 客户端证书路径
    pub fn nats_client_cert_path(&self) -> PathBuf {
        self.certs_dir.join("nats-client.pem")
    }

    /// 获取 NATS 客户端密钥路径
    pub fn nats_client_key_path(&self) -> PathBuf {
        self.certs_dir.join("nats-client-key.pem")
    }

    /// 获取 gRPC CA 证书路径
    pub fn grpc_ca_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-ca.pem")
    }

    /// 获取 gRPC 服务器证书路径
    pub fn grpc_server_cert_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-server.pem")
    }

    /// 获取 gRPC 服务器密钥路径
    pub fn grpc_server_key_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-server-key.pem")
    }

    /// 获取 gRPC 客户端证书路径
    pub fn grpc_client_cert_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-client.pem")
    }

    /// 获取 gRPC 客户端密钥路径
    pub fn grpc_client_key_path(&self) -> PathBuf {
        self.certs_dir.join("grpc-client-key.pem")
    }
}

/// 服务器特定配置
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_heartbeat_ttl")]
    pub heartbeat_ttl_sec: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            heartbeat_ttl_sec: default_heartbeat_ttl(),
        }
    }
}

fn default_listen_addr() -> String {
    "127.0.0.1:50051".to_string()
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

impl OasisConfig {
    pub fn resolve_relative_paths(&mut self, base_dir: &std::path::Path) {
        fn make_absolute(path: &mut PathBuf, base: &std::path::Path) {
            if path.is_relative() {
                *path = base.join(&*path);
            }
        }

        make_absolute(&mut self.tls.certs_dir, base_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    mod telemetry_config_tests {
        use super::*;

        #[test]
        fn test_default() {
            let config = TelemetryConfig::default();
            assert_eq!(config.log_level, "info");
            assert_eq!(config.log_format, "text");
            assert!(!config.log_no_ansi);
        }
    }

    mod nats_config_tests {
        use super::*;

        #[test]
        fn test_default() {
            let config = NatsConfig::default();
            assert_eq!(config.url, "tls://127.0.0.1:4222");
        }
    }

    mod grpc_config_tests {
        use super::*;

        #[test]
        fn test_default() {
            let config = GrpcConfig::default();
            assert_eq!(config.url, "https://127.0.0.1:50051");
        }
    }

    mod tls_config_tests {
        use super::*;

        #[test]
        fn test_default() {
            let config = TlsConfig::default();
            assert_eq!(config.certs_dir, PathBuf::from("certs"));
        }

        #[test]
        fn test_nats_paths() {
            let config = TlsConfig {
                certs_dir: PathBuf::from("/etc/oasis/certs"),
            };
            assert_eq!(
                config.nats_ca_path(),
                Path::new("/etc/oasis/certs/nats-ca.pem")
            );
            assert_eq!(
                config.nats_client_cert_path(),
                Path::new("/etc/oasis/certs/nats-client.pem")
            );
            assert_eq!(
                config.nats_client_key_path(),
                Path::new("/etc/oasis/certs/nats-client-key.pem")
            );
        }

        #[test]
        fn test_grpc_server_paths() {
            let config = TlsConfig {
                certs_dir: PathBuf::from("/certs"),
            };
            assert_eq!(config.grpc_ca_path(), Path::new("/certs/grpc-ca.pem"));
            assert_eq!(
                config.grpc_server_cert_path(),
                Path::new("/certs/grpc-server.pem")
            );
            assert_eq!(
                config.grpc_server_key_path(),
                Path::new("/certs/grpc-server-key.pem")
            );
        }

        #[test]
        fn test_grpc_client_paths() {
            let config = TlsConfig {
                certs_dir: PathBuf::from("/certs"),
            };
            assert_eq!(
                config.grpc_client_cert_path(),
                Path::new("/certs/grpc-client.pem")
            );
            assert_eq!(
                config.grpc_client_key_path(),
                Path::new("/certs/grpc-client-key.pem")
            );
        }
    }

    mod server_config_tests {
        use super::*;

        #[test]
        fn test_default() {
            let config = ServerConfig::default();
            assert_eq!(config.listen_addr, "127.0.0.1:50051");
            assert_eq!(config.heartbeat_ttl_sec, 60);
        }
    }

    mod oasis_config_tests {
        use super::*;

        #[test]
        fn test_default() {
            let config = OasisConfig::default();
            assert_eq!(config.nats.url, "tls://127.0.0.1:4222");
            assert_eq!(config.grpc.url, "https://127.0.0.1:50051");
        }

        #[test]
        fn test_build_grpc_url_valid_https() {
            let config = OasisConfig::default();
            let url = config.build_grpc_url().unwrap();
            assert_eq!(url, "https://127.0.0.1:50051");
        }

        #[test]
        fn test_build_grpc_url_rejects_http() {
            let mut config = OasisConfig::default();
            config.grpc.url = "http://127.0.0.1:50051".to_string();
            assert!(config.build_grpc_url().is_err());
        }

        #[test]
        fn test_build_grpc_url_trims_whitespace() {
            let mut config = OasisConfig::default();
            config.grpc.url = "  https://example.com:50051  ".to_string();
            let url = config.build_grpc_url().unwrap();
            assert_eq!(url, "https://example.com:50051");
        }

        #[test]
        fn test_resolve_relative_paths() {
            let mut config = OasisConfig::default();
            config.tls.certs_dir = PathBuf::from("certs");

            let base = Path::new("/opt/oasis");
            config.resolve_relative_paths(base);

            assert_eq!(config.tls.certs_dir, Path::new("/opt/oasis/certs"));
        }

        #[test]
        fn test_resolve_absolute_paths_unchanged() {
            let mut config = OasisConfig::default();
            config.tls.certs_dir = PathBuf::from("/etc/oasis/certs");

            let base = Path::new("/opt/oasis");
            config.resolve_relative_paths(base);

            assert_eq!(config.tls.certs_dir, Path::new("/etc/oasis/certs"));
        }

        #[test]
        fn test_serde_roundtrip() {
            let config = OasisConfig::default();
            let toml = toml::to_string(&config).unwrap();
            let deserialized: OasisConfig = toml::from_str(&toml).unwrap();

            assert_eq!(config.nats, deserialized.nats);
            assert_eq!(config.grpc, deserialized.grpc);
            assert_eq!(config.telemetry, deserialized.telemetry);
        }
    }
}
