//! 提供通用的 NATS 连接逻辑，支持 TLS 和 JetStream 功能。

use anyhow::{Context, Result};
use async_nats::Client;
use async_nats::ConnectOptions;
use std::path::PathBuf;
use tracing::{info, warn, error};

/// NATS 客户端工厂
/// 封装了 NATS 连接的所有细节，包括 TLS 配置和 JetStream 上下文。
pub struct NatsClientFactory;

impl NatsClientFactory {
    /// 使用证书文件连接到 NATS
    pub async fn connect_with_certs(
        nats_url: &str,
        nats_ca: &str,
        nats_client_cert: &str,
        nats_client_key: &str,
    ) -> Result<Client> {
        let mut options = ConnectOptions::new();

        // 添加 TLS 配置
        let ca_path = PathBuf::from(nats_ca);
        let cert_path = PathBuf::from(nats_client_cert);
        let key_path = PathBuf::from(nats_client_key);

        if ca_path.exists() && cert_path.exists() && key_path.exists() {
            options = options
                .add_root_certificates(ca_path)
                .add_client_certificate(cert_path, key_path)
                .require_tls(true);
            info!(
                "Added TLS certificates: CA={}, Cert={}, Key={}",
                nats_ca, nats_client_cert, nats_client_key
            );
        } else {
            warn!(
                "Certificate files not found, skipping TLS: CA={}, Cert={}, Key={}",
                nats_ca, nats_client_cert, nats_client_key
            );
        }

        // 连接到 NATS
        let client = options
            .connect(nats_url)
            .await
            .context(format!("failed to connect to NATS at {}", nats_url))?;

        info!("Successfully connected to NATS with TLS at {}", nats_url);
        Ok(client)
    }

    /// 使用配置连接到 NATS
    pub async fn connect_with_config(config: &crate::config::NatsConfig) -> Result<Client> {
        // 创建 TLS 配置
        info!(
            url = %config.url,
            ca = %config.ca_path.display(),
            cert = %config.client_cert_path.display(),
            key = %config.client_key_path.display(),
            "Connecting to NATS with TLS"
        );
        let options = ConnectOptions::new()
            .add_root_certificates(config.ca_path.clone())
            .add_client_certificate(
                config.client_cert_path.clone(),
                config.client_key_path.clone(),
            )
            .require_tls(true);

        // 连接到 NATS（使用 TLS）
        let client = match options.connect(&config.url).await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to connect to NATS with TLS");
                return Err(e).context("Failed to connect to NATS");
            }
        };

        Ok(client)
    }
}
