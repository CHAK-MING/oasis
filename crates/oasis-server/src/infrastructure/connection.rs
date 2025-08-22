use anyhow::{Context, Result};
use async_nats::ConnectOptions;
use oasis_core::config::NatsConfig;
use oasis_core::error::CoreError;
// 移除未使用的 PathBuf 导入
use tracing::info;

/// NATS 连接管理器
pub struct NatsConnectionManager;

impl NatsConnectionManager {
    /// 创建 NATS 连接
    pub async fn connect(nats_url: &str) -> Result<async_nats::Client> {
        Self::connect_with_config(nats_url, None).await
    }

    /// 创建带 TLS + JWT 的 NATS 连接
    pub async fn connect_with_tls(
        nats_url: &str,
        nats_config: &NatsConfig,
    ) -> Result<async_nats::Client> {
        // 使用统一的连接方法，传入 NATS 配置
        Self::connect_with_config(nats_url, Some(nats_config)).await
    }

    /// 统一的连接方法，处理所有连接场景
    async fn connect_with_config(
        nats_url: &str,
        nats_config: Option<&NatsConfig>,
    ) -> Result<async_nats::Client> {
        let is_tls = nats_config.is_some();
        let log_prefix = if is_tls { "with TLS" } else { "" };
        info!("Connecting to NATS {} at: {}", log_prefix, nats_url);

        let mut connect_options = ConnectOptions::new()
            .name("oasis-server")
            .retry_on_initial_connect();

        // 配置 TLS（如果启用）
        if let Some(config) = nats_config {
            if config.tls_required {
                connect_options = connect_options.require_tls(true);

                // 加载 CA 证书
                if let Some(ca_cert) = &config.tls_ca_file {
                    if ca_cert.exists() {
                        connect_options = connect_options.add_root_certificates(ca_cert.clone());
                        info!("Added CA certificate: {}", ca_cert.display());
                    }
                }

                // 加载客户端证书和私钥
                if let (Some(cert_file), Some(key_file)) =
                    (&config.tls_cert_file, &config.tls_key_file)
                {
                    if cert_file.exists() && key_file.exists() {
                        connect_options = connect_options
                            .add_client_certificate(cert_file.clone(), key_file.clone());
                        info!(
                            "Added client certificate: {} and key: {}",
                            cert_file.display(),
                            key_file.display()
                        );
                    }
                }
            }

            // 加载 credentials 文件（如果提供）
            if let Some(creds_file) = &config.creds_file {
                if creds_file.exists() {
                    connect_options = connect_options.credentials_file(creds_file.clone()).await?;
                    info!("Added credentials file: {}", creds_file.display());
                }
            }
        }

        let client = connect_options
            .connect(nats_url)
            .await
            .context("Failed to connect to NATS")
            .map_err(|e| CoreError::Network {
                message: format!("Failed to connect to NATS at {}: {}", nats_url, e),
            })?;

        let success_msg = if is_tls {
            "Successfully connected to NATS with TLS + JWT"
        } else {
            "Successfully connected to NATS"
        };
        info!("{}", success_msg);
        Ok(client)
    }

    /// 创建 JetStream 上下文
    pub async fn create_jetstream(nats_url: &str) -> Result<async_nats::jetstream::Context> {
        let client = Self::connect(nats_url).await?;
        let jetstream = async_nats::jetstream::new(client);
        Ok(jetstream)
    }

    /// 创建带 TLS + JWT 的 JetStream 上下文
    pub async fn create_jetstream_with_tls(
        nats_url: &str,
        nats_config: &NatsConfig,
    ) -> Result<async_nats::jetstream::Context> {
        let client = Self::connect_with_tls(nats_url, nats_config).await?;
        let jetstream = async_nats::jetstream::new(client);
        Ok(jetstream)
    }
}
