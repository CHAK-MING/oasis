use oasis_core::config::CommonConfig;
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
    /// TLS configuration for gRPC (mandatory)
    pub grpc_tls: GrpcTlsConfig,
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
    /// CA certificate path for client verification
    pub ca_cert: String,
    /// Server certificate path
    pub server_cert: String,
    /// Server private key path
    pub server_key: String,
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
            grpc_addr: "0.0.0.0:50051".into(),
            heartbeat_ttl_sec: 90,
            grpc_tls: GrpcTlsConfig::default(),
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
            tasks: StreamParams {
                max_age_sec: 86400, // 24 hours
                max_msgs: 1000000,
                max_bytes: 1000000000, // 1GB
                max_msg_size: 1048576, // 1MB
                storage: "file".into(),
                replicas: 1,
            },
            results: StreamParams {
                max_age_sec: 604800,   // 7 days
                max_msgs: 10000000,    // 10M messages
                max_bytes: 5000000000, // 5GB
                max_msg_size: 1048576, // 1MB
                storage: "file".into(),
                replicas: 1,
            },
            rollouts: StreamParams {
                max_age_sec: 86400, // 1 day
                max_msgs: 1000000,
                max_bytes: 1000000000, // 1GB
                max_msg_size: 1048576, // 1MB
                storage: "file".into(),
                replicas: 1,
            },
            dlq: DlqStreamParams::default(),
            artifacts: ObjectStoreParams::default(),
        }
    }
}

impl Default for StreamParams {
    fn default() -> Self {
        Self {
            max_age_sec: 24 * 60 * 60, // 24 hours
            max_msgs: 10_000,
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
            max_age_sec: 86400,     // 24 hours
            max_bytes: 10737418240, // 10GB
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
            ca_cert: "certs/grpc-ca.pem".into(),
            server_cert: "certs/grpc-server.pem".into(),
            server_key: "certs/grpc-server-key.pem".into(),
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
