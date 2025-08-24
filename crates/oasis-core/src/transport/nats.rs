//! 统一的 NATS 客户端模块
//!
//! 提供通用的 NATS 连接逻辑，支持 TLS、JWT 凭证和 JetStream 功能。
//! 消除 oasis-agent 和 oasis-server 中的重复代码。

use crate::config::NatsConfig;
use anyhow::{Context, Result};
use async_nats::{
    jetstream::{self},
    Client, ConnectOptions,
};
use tracing::{info, warn};

/// 统一的 NATS 客户端
///
/// 封装了 NATS 连接的所有细节，包括 TLS 配置、JWT 凭证和 JetStream 上下文。
/// 提供统一的连接接口，消除 agent 和 server 中的重复代码。
#[derive(Clone)]
pub struct UnifiedNatsClient {
    /// 底层的 NATS 客户端
    pub client: Client,
    /// JetStream 上下文
    pub jetstream: jetstream::Context,
}

impl UnifiedNatsClient {
    /// 创建新的 NATS 客户端连接
    ///
    /// # Arguments
    ///
    /// * `config` - NATS 配置
    /// * `client_name` - 客户端名称（用于日志和连接标识）
    ///
    /// # Returns
    ///
    /// 返回包含 Client 和 JetStream 上下文的 UnifiedNatsClient
    pub async fn connect(config: &NatsConfig, client_name: &str) -> Result<Self> {
        let mut options = ConnectOptions::new()
            .name(client_name)
            .retry_on_initial_connect()
            .connection_timeout(std::time::Duration::from_secs(30)) // 增加连接超时时间
            .reconnect_delay_callback(|attempts| {
                std::time::Duration::from_secs(1.min(attempts as u64 * 2)) // 指数退避
            })
            // 添加事件回调来处理错误日志级别
            .event_callback(|ev| async move {
                match ev {
                    async_nats::Event::ClientError(e) => {
                        tracing::error!("NATS client error: {} (type: {:?})", e, e);
                        // 对于某些致命错误，可以考虑退出程序
                        if e.to_string().contains("TLS") || e.to_string().contains("certificate") {
                            tracing::error!("TLS/Certificate error detected. This is likely a configuration issue.");
                        }
                        // 添加更详细的错误信息
                        tracing::error!("NATS error details: {:?}", e);
                    }
                    async_nats::Event::Disconnected => {
                        tracing::warn!("NATS disconnected - connection lost");
                    }
                    async_nats::Event::Connected => {
                        tracing::info!("NATS connected successfully");
                    }
                    _ => {
                        tracing::debug!("NATS event: {:?}", ev);
                    }
                }
            });

        // 获取 URL
        let url = &config.url;

        // 强制 TLS 连接
        options = options.require_tls(true);

        // 添加 CA 证书
        if let Some(ca) = &config.tls_ca_file {
            if ca.exists() {
                options = options.add_root_certificates(ca.clone());
                info!("Added CA certificate: {}", ca.display());
            } else {
                anyhow::bail!("CA certificate file not found: {}", ca.display());
            }
        }

        // 添加客户端证书（双向认证）
        if let (Some(cert), Some(key)) = (&config.tls_cert_file, &config.tls_key_file) {
            if cert.exists() && key.exists() {
                options = options.add_client_certificate(cert.clone(), key.clone());
                info!(
                    "Added client certificate: {} and key: {}",
                    cert.display(),
                    key.display()
                );
            } else {
                anyhow::bail!(
                    "Client certificate or key file not found: cert={}, key={}",
                    cert.display(),
                    key.display()
                );
            }
        }

        // JWT 凭证（.creds 文件）- 可选
        if let Some(creds) = &config.creds_file {
            if creds.exists() {
                options = options
                    .credentials_file(creds.clone())
                    .await
                    .context(format!(
                        "failed to load credentials from file: {}",
                        creds.display()
                    ))?;
                info!("Added credentials file: {}", creds.display());
            } else {
                warn!("Credentials file not found, skipping: {}", creds.display());
            }
        }

        // 连接（使用 async-nats 内置的重连机制）
        let client = match options.connect(&url).await {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("NATS connect failed: {:?}", e);
                return Err(anyhow::anyhow!("failed to connect to NATS: {:?}", e));
            }
        };

        // 连接成功后打印日志
        info!("Successfully connected to NATS with TLS + JWT");

        let jetstream = jetstream::new(client.clone());
        info!("JetStream context created successfully");
        Ok(Self { client, jetstream })
    }

    /// 从现有的 Client 创建 JetStream 上下文
    pub fn create_jetstream(client: Client) -> jetstream::Context {
        jetstream::new(client)
    }

    /// 获取底层的 NATS 客户端
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// 获取 JetStream 上下文
    pub fn jetstream(&self) -> &jetstream::Context {
        &self.jetstream
    }

    /// 获取 JetStream 上下文的克隆
    pub fn jetstream_clone(&self) -> jetstream::Context {
        self.jetstream.clone()
    }
}

/// 便捷的工厂函数，用于创建不同类型的 NATS 连接
pub struct NatsClientFactory;

impl NatsClientFactory {
    /// 为 Agent 创建 NATS 客户端
    pub async fn for_agent(config: &NatsConfig) -> Result<UnifiedNatsClient> {
        UnifiedNatsClient::connect(config, "oasis-agent").await
    }

    /// 为 Server 创建 NATS 客户端
    pub async fn for_server(config: &NatsConfig) -> Result<UnifiedNatsClient> {
        UnifiedNatsClient::connect(config, "oasis-server").await
    }
}
