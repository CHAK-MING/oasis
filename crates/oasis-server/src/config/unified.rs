use async_trait::async_trait;
use oasis_core::config::{CommonConfig, ComponentConfig, ConfigLoadOptions, ConfigSource};
use oasis_core::error::Result;
use serde::{Deserialize, Serialize};

/// 统一的 Server 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(flatten)]
    pub common: CommonConfig,

    /// 服务器配置
    pub server: ServerSection,

    /// 流配置
    pub streams: StreamsSection,

    /// 流式处理配置
    pub streaming: StreamingBackoffSection,

    /// KV 存储配置
    pub kv: KvSection,

    /// 发布配置
    pub publish: PublishSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSection {
    pub grpc_addr: String,
    /// Heartbeat KV TTL in seconds (should be 2x agent heartbeat interval)
    pub heartbeat_ttl_sec: u64,
    /// TLS configuration for gRPC
    pub grpc_tls: Option<GrpcTlsConfig>,
    /// Leader election configuration
    pub leader_election: Option<LeaderElectionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingBackoffSection {
    /// Max retries when fetching next result fails
    pub max_retries: u32,
    /// Initial polling interval when streaming results (milliseconds)
    pub initial_poll_interval_ms: u64,
    /// Max polling interval when backing off due to empty results (milliseconds)
    pub max_poll_interval_ms: u64,
    /// How many consecutive empty polls before backing off interval
    pub empty_results_threshold: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvSection {
    /// Max value size for facts/labels entries
    pub max_value_size: u32,
    /// Facts history depth
    pub facts_history: u32,
    /// Labels history depth
    pub labels_history: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsSection {
    pub tasks: StreamParams,
    pub results: StreamParams,
    pub rollouts: StreamParams,
    pub dlq: DlqStreamParams,
    /// 对象存储（用于分发文件）的参数
    pub artifacts: ObjectStoreParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamParams {
    pub max_age_sec: u64,
    pub max_msgs: i64,
    pub max_bytes: i64,
    pub max_msg_size: i32,
    pub storage: String,
    pub replicas: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqStreamParams {
    pub max_age_sec: u64,
    pub max_msgs: i64,
    pub max_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreParams {
    pub max_age_sec: u64,
    pub max_bytes: i64,
    pub storage: String,
    pub replicas: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishSection {
    pub max_publishes_per_sec: u32,
    pub max_retries: u32,
    pub initial_retry_delay_ms: u64,
    pub max_retry_delay_ms: u64,
}

/// TLS configuration for gRPC server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcTlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// CA certificate path for client verification
    pub ca_cert: String,
    /// Server certificate path
    pub server_cert: String,
    /// Server private key path
    pub server_key: String,
    /// Require client certificate (mTLS)
    pub require_client_cert: bool,
    /// Certificate file check interval for hot reload (seconds)
    pub cert_check_interval_sec: u64,
}

/// Leader election configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderElectionConfig {
    /// Lease TTL in seconds
    pub lease_ttl_sec: u64,
    /// Renewal interval in seconds
    pub renewal_interval_sec: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            common: CommonConfig::default(),
            server: ServerSection::default(),
            streams: StreamsSection::default(),
            streaming: StreamingBackoffSection::default(),
            kv: KvSection::default(),
            publish: PublishSection::default(),
        }
    }
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            grpc_addr: "[::1]:50052".into(),
            heartbeat_ttl_sec: 60, // 2x default agent heartbeat interval (30s)
            grpc_tls: Some(GrpcTlsConfig::default()),
            leader_election: None,
        }
    }
}

impl Default for StreamingBackoffSection {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_poll_interval_ms: 50,
            max_poll_interval_ms: 500,
            empty_results_threshold: 5,
        }
    }
}

impl Default for KvSection {
    fn default() -> Self {
        Self {
            max_value_size: 65536, // 64KB
            facts_history: 50,
            labels_history: 50,
        }
    }
}

impl Default for StreamsSection {
    fn default() -> Self {
        Self {
            tasks: StreamParams::default(),
            results: StreamParams::default(),
            rollouts: StreamParams::default(),
            dlq: DlqStreamParams::default(),
            artifacts: ObjectStoreParams::default(),
        }
    }
}

impl Default for StreamParams {
    fn default() -> Self {
        Self {
            max_age_sec: 24 * 60 * 60, // 24 hours
            max_msgs: 100_000,
            max_bytes: 1024 * 1024 * 1024, // 1GB
            max_msg_size: 1024 * 1024,     // 1MB
            storage: "file".into(),
            replicas: 1,
        }
    }
}

impl Default for DlqStreamParams {
    fn default() -> Self {
        Self {
            max_age_sec: 7 * 24 * 60 * 60, // 7 days
            max_msgs: 10_000,
            max_bytes: 100 * 1024 * 1024, // 100MB
        }
    }
}

impl Default for ObjectStoreParams {
    fn default() -> Self {
        Self {
            max_age_sec: 24 * 60 * 60,          // 24 hours
            max_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            storage: "file".into(),
            replicas: 1,
        }
    }
}

impl Default for PublishSection {
    fn default() -> Self {
        Self {
            max_publishes_per_sec: 100,
            max_retries: 3,
            initial_retry_delay_ms: 100,
            max_retry_delay_ms: 5000,
        }
    }
}

impl Default for GrpcTlsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ca_cert: "certs/ca.pem".into(),
            server_cert: "certs/server.pem".into(),
            server_key: "certs/server-key.pem".into(),
            require_client_cert: true,
            cert_check_interval_sec: 300,
        }
    }
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            lease_ttl_sec: 30,
            renewal_interval_sec: 10,
        }
    }
}

impl ServerConfig {
    /// 智能配置加载
    pub async fn load_smart() -> Result<Self> {
        // Server 仅使用默认值，不自动加载配置文件
        Self::load_from_sources(&[ConfigSource::Defaults], ConfigLoadOptions::default()).await
    }

    /// 从指定路径加载配置
    pub async fn load_from_path(config_path: &str) -> Result<Self> {
        let sources = vec![
            ConfigSource::Defaults,
            ConfigSource::File(config_path.into()),
        ];

        Self::load_from_sources(&sources, ConfigLoadOptions::default()).await
    }
}

#[async_trait]
impl ComponentConfig for ServerConfig {
    type Common = CommonConfig;

    async fn load_from_sources(
        sources: &[ConfigSource],
        options: ConfigLoadOptions,
    ) -> Result<Self> {
        oasis_core::config::load_config(sources, options).await
    }

    fn merge_with_common(&mut self, common: Self::Common) {
        self.common = common;
    }

    fn common(&self) -> &Self::Common {
        &self.common
    }

    fn common_mut(&mut self) -> &mut Self::Common {
        &mut self.common
    }

    fn validate_business_rules(&self) -> Result<()> {
        // Server 特定的业务规则验证
        if self.server.grpc_addr.is_empty() {
            return Err(oasis_core::error::CoreError::Config {
                message: "grpc_addr cannot be empty".to_string(),
            });
        }

        if self.server.heartbeat_ttl_sec == 0 {
            return Err(oasis_core::error::CoreError::Config {
                message: "heartbeat_ttl_sec must be greater than 0".to_string(),
            });
        }

        if self.kv.max_value_size == 0 {
            return Err(oasis_core::error::CoreError::Config {
                message: "max_value_size must be greater than 0".to_string(),
            });
        }

        if self.publish.max_publishes_per_sec == 0 {
            return Err(oasis_core::error::CoreError::Config {
                message: "max_publishes_per_sec must be greater than 0".to_string(),
            });
        }

        // 验证流配置
        self.validate_stream_config()?;

        Ok(())
    }

    /// 检查配置是否可以热重载
    fn can_hot_reload(&self, other: &Self) -> bool {
        // 不可热重载的配置项
        if self.server.grpc_addr != other.server.grpc_addr {
            return false; // gRPC 地址变更需要重启
        }

        if self.common.nats.url != other.common.nats.url {
            return false; // NATS URL 变更需要重启
        }

        if self.common.nats.tls_required != other.common.nats.tls_required {
            return false; // TLS 要求变更需要重启
        }

        // 其他配置可以热重载
        true
    }

    /// 获取配置摘要
    fn summary(&self) -> std::collections::HashMap<String, String> {
        let mut summary = std::collections::HashMap::new();

        // 基本信息
        summary.insert("grpc_addr".to_string(), self.server.grpc_addr.clone());
        summary.insert("nats_url".to_string(), self.common.nats.url.clone());
        summary.insert(
            "log_level".to_string(),
            self.common.telemetry.log_level.clone(),
        );
        summary.insert(
            "log_format".to_string(),
            self.common.telemetry.log_format.clone(),
        );

        // 服务器配置
        summary.insert(
            "heartbeat_ttl_sec".to_string(),
            self.server.heartbeat_ttl_sec.to_string(),
        );

        // 流配置
        summary.insert(
            "tasks_max_age_sec".to_string(),
            self.streams.tasks.max_age_sec.to_string(),
        );
        summary.insert(
            "tasks_max_msgs".to_string(),
            self.streams.tasks.max_msgs.to_string(),
        );
        summary.insert(
            "results_max_age_sec".to_string(),
            self.streams.results.max_age_sec.to_string(),
        );
        summary.insert(
            "rollouts_max_age_sec".to_string(),
            self.streams.rollouts.max_age_sec.to_string(),
        );

        // KV 配置
        summary.insert(
            "kv_max_value_size".to_string(),
            self.kv.max_value_size.to_string(),
        );
        summary.insert(
            "kv_facts_history".to_string(),
            self.kv.facts_history.to_string(),
        );
        summary.insert(
            "kv_labels_history".to_string(),
            self.kv.labels_history.to_string(),
        );

        // 发布配置
        summary.insert(
            "publish_max_per_sec".to_string(),
            self.publish.max_publishes_per_sec.to_string(),
        );
        summary.insert(
            "publish_max_retries".to_string(),
            self.publish.max_retries.to_string(),
        );

        // TLS 配置
        if let Some(ref tls) = self.server.grpc_tls {
            summary.insert("grpc_tls_enabled".to_string(), tls.enabled.to_string());
            summary.insert(
                "grpc_tls_require_client_cert".to_string(),
                tls.require_client_cert.to_string(),
            );
        } else {
            summary.insert("grpc_tls_enabled".to_string(), "false".to_string());
        }

        // Leader election 配置
        if let Some(ref le) = self.server.leader_election {
            summary.insert("leader_election_enabled".to_string(), "true".to_string());
            summary.insert(
                "leader_election_lease_ttl_sec".to_string(),
                le.lease_ttl_sec.to_string(),
            );
        } else {
            summary.insert("leader_election_enabled".to_string(), "false".to_string());
        }

        summary
    }
}

impl ServerConfig {
    fn validate_stream_config(&self) -> Result<()> {
        // 验证任务流配置
        if self.streams.tasks.max_age_sec == 0 {
            return Err(oasis_core::error::CoreError::Config {
                message: "tasks.max_age_sec must be greater than 0".to_string(),
            });
        }

        if self.streams.results.max_age_sec == 0 {
            return Err(oasis_core::error::CoreError::Config {
                message: "results.max_age_sec must be greater than 0".to_string(),
            });
        }

        if self.streams.rollouts.max_age_sec == 0 {
            return Err(oasis_core::error::CoreError::Config {
                message: "rollouts.max_age_sec must be greater than 0".to_string(),
            });
        }

        Ok(())
    }
}
