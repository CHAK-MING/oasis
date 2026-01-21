//! 提供通用的 NATS 连接逻辑，支持 TLS 和 JetStream 功能。

use crate::error::{CoreError, ErrorSeverity, Result};
use async_nats::ConnectOptions;
use async_nats::{Client, jetstream};
use backon::Retryable;
use std::time::Duration;
use tracing::{error, info, warn};

/// NATS 客户端工厂
/// 封装了 NATS 连接的所有细节，包括 TLS 配置和 JetStream 上下文。
pub struct NatsClientFactory;

impl NatsClientFactory {
    /// 使用配置连接到 NATS
    pub async fn connect_with_config(
        nats_config: &crate::config::NatsConfig,
        tls_config: &crate::config::TlsConfig,
    ) -> Result<Client> {
        let mut options = ConnectOptions::new();

        // 如果是 tls:// 开头，尽可能加载 CA 与客户端证书；否则用明文
        let is_tls = nats_config.url.starts_with("tls://");
        if is_tls {
            let ca_path = tls_config.nats_ca_path();
            let cert_path = tls_config.nats_client_cert_path();
            let key_path = tls_config.nats_client_key_path();

            info!(
                url = %nats_config.url,
                ca = %ca_path.display(),
                cert = %cert_path.display(),
                key = %key_path.display(),
                require_tls = %tls_config.require_tls,
                "Connecting to NATS (TLS mode)"
            );

            if ca_path.exists() {
                options = options.add_root_certificates(ca_path);
            } else if tls_config.require_tls {
                return Err(CoreError::Config {
                    message: format!(
                        "NATS CA certificate not found at '{}'. Set tls.require_tls=false to allow insecure connections.",
                        ca_path.display()
                    ),
                    severity: ErrorSeverity::Critical,
                });
            } else {
                warn!("NATS CA not found; proceeding without custom CA (require_tls=false)");
            }

            if cert_path.exists() && key_path.exists() {
                options = options.add_client_certificate(cert_path, key_path);
            } else if tls_config.require_tls {
                return Err(CoreError::Config {
                    message: format!(
                        "NATS client certificate not found at '{}' or key at '{}'. Set tls.require_tls=false to allow connections without mTLS.",
                        cert_path.display(),
                        key_path.display()
                    ),
                    severity: ErrorSeverity::Critical,
                });
            } else {
                warn!(
                    "NATS client certificate not found; proceeding without mTLS (require_tls=false)"
                );
            }

            options = options.require_tls(true);
        } else {
            info!(url = %nats_config.url, "Connecting to NATS without TLS");
        }

        // 健壮的心跳与超时，避免长时间空闲后连接“假存活”
        options = options
            .ping_interval(Duration::from_secs(20))
            .connection_timeout(Duration::from_secs(5))
            .event_callback(|event| async move {
                match event {
                    async_nats::Event::Connected => {
                        info!("NATS connected");
                    }
                    async_nats::Event::Disconnected => {
                        warn!("NATS disconnected");
                    }
                    async_nats::Event::LameDuckMode => {
                        warn!("NATS server entering lame duck mode");
                    }
                    async_nats::Event::SlowConsumer(_) => {
                        warn!("NATS slow consumer detected");
                    }
                    async_nats::Event::ServerError(err) => {
                        error!(error = %err, "NATS server error");
                    }
                    async_nats::Event::ClientError(err) => {
                        error!(error = %err, "NATS client error");
                    }
                    async_nats::Event::Draining => {
                        info!("NATS connection draining");
                    }
                    async_nats::Event::Closed => {
                        info!("NATS connection closed");
                    }
                }
            });

        // 连接到 NATS
        let client = match options.connect(&nats_config.url).await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to connect to NATS");
                return Err(CoreError::Nats {
                    message: format!("Failed to connect to NATS: {}", e),
                    severity: ErrorSeverity::Critical,
                });
            }
        };

        Ok(client)
    }

    /// 创建 NATS 客户端和 JetStream 上下文，包含连接重试逻辑
    /// 返回包含 client 和 jetstream 的结构体，方便 agent 使用
    pub async fn create_nats_client_with_jetstream(
        nats_config: &crate::config::NatsConfig,
        tls_config: &crate::config::TlsConfig,
    ) -> Result<NatsClientWithJetStream> {
        info!("Connecting to NATS with JetStream url={}", nats_config.url);

        // 使用退避策略进行连接重试
        let backoff = crate::backoff::network_publish_backoff().build();
        let client = (|| async {
            Self::connect_with_config(nats_config, tls_config)
                .await
                .map_err(|e| CoreError::Nats {
                    message: format!("NATS connection attempt failed: {}", e),
                    severity: ErrorSeverity::Error,
                })
        })
        .retry(&backoff)
        .await
        .map_err(|e| CoreError::Nats {
            message: format!("Failed to connect to NATS with retry: {}", e),
            severity: ErrorSeverity::Critical,
        })?;

        let jetstream = jetstream::new(client.clone());
        info!("NATS client and JetStream context created successfully with retry logic");

        Ok(NatsClientWithJetStream { client, jetstream })
    }
}

/// NATS 客户端和 JetStream 上下文的组合
/// 为 agent 和 server 提供统一的接口
#[derive(Clone)]
pub struct NatsClientWithJetStream {
    pub client: Client,
    pub jetstream: jetstream::Context,
}

/// JetStream Pull Consumer 配置构建器
pub struct ConsumerConfigBuilder {
    durable_name: String,
    filter_subject: String,
    max_deliver: i64,
    ack_wait_secs: u64,
}

const DEFAULT_MAX_DELIVER: i64 = 3;
const DEFAULT_ACK_WAIT_SECS: u64 = 120;

impl ConsumerConfigBuilder {
    /// 创建新的 Consumer 配置构建器
    pub fn new(durable_name: impl Into<String>, filter_subject: impl Into<String>) -> Self {
        Self {
            durable_name: durable_name.into(),
            filter_subject: filter_subject.into(),
            max_deliver: DEFAULT_MAX_DELIVER,
            ack_wait_secs: DEFAULT_ACK_WAIT_SECS,
        }
    }

    /// 设置最大重试次数
    pub fn max_deliver(mut self, max: i64) -> Self {
        self.max_deliver = max;
        self
    }

    /// 设置 ACK 超时时间（秒）
    pub fn ack_wait_secs(mut self, secs: u64) -> Self {
        self.ack_wait_secs = secs;
        self
    }

    /// 构建 JetStream Consumer 配置
    pub fn build(self) -> jetstream::consumer::pull::Config {
        jetstream::consumer::pull::Config {
            durable_name: Some(self.durable_name),
            filter_subject: self.filter_subject,
            deliver_policy: jetstream::consumer::DeliverPolicy::All,
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            max_deliver: self.max_deliver,
            ack_wait: Duration::from_secs(self.ack_wait_secs),
            ..Default::default()
        }
    }
}
