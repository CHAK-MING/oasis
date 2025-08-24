use oasis_core::config::CommonConfig;
use serde::{Deserialize, Serialize};

/// 统一的 CLI 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    #[serde(flatten)]
    pub common: CommonConfig,

    /// CLI 特定配置
    pub cli: CliSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliSection {
    /// 服务器连接地址
    pub server_url: String,
    /// 连接超时时间（秒）
    pub connection_timeout_sec: u64,
    /// 请求超时时间（秒）
    pub request_timeout_sec: u64,
    /// 是否启用交互式模式
    pub interactive: bool,
    /// 输出格式
    pub output_format: OutputFormat,
    /// gRPC TLS 配置
    pub grpc_tls: Option<GrpcTlsConfig>,
}

/// TLS configuration for gRPC client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcTlsConfig {
    /// CA certificate path for server verification
    pub ca_cert: String,
    /// Client certificate path (for mutual TLS)
    pub client_cert: String,
    /// Client private key path
    pub client_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputFormat {
    #[serde(rename = "json")]
    Json,
    #[serde(rename = "yaml")]
    Yaml,
    #[serde(rename = "table")]
    Table,
    #[serde(rename = "text")]
    Text,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            common: CommonConfig::default(),
            cli: CliSection::default(),
        }
    }
}

impl Default for CliSection {
    fn default() -> Self {
        Self {
            server_url: "https://127.0.0.1:50051".into(),
            connection_timeout_sec: 30,
            request_timeout_sec: 60,
            interactive: false,
            output_format: OutputFormat::Table,
            grpc_tls: Some(GrpcTlsConfig::default()),
        }
    }
}

impl Default for GrpcTlsConfig {
    fn default() -> Self {
        Self {
            ca_cert: "certs/grpc-ca.pem".into(),
            client_cert: "certs/grpc-client.pem".into(),
            client_key: "certs/grpc-client-key.pem".into(),
        }
    }
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Table
    }
}
