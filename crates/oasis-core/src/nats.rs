//! 提供通用的 NATS 连接逻辑，支持 TLS 和 JetStream 功能。

use anyhow::{Context, Result};
use async_nats::ConnectOptions;
use async_nats::{Client, jetstream};
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
                "Connecting to NATS (TLS mode if certs present)"
            );

            if ca_path.exists() {
                options = options.add_root_certificates(ca_path);
            } else {
                warn!("NATS CA not found; proceeding without custom CA");
            }

            if cert_path.exists() && key_path.exists() {
                options = options.add_client_certificate(cert_path, key_path);
            } else {
                info!("NATS client certificate not found; proceeding without mTLS");
            }

            options = options.require_tls(true);
        } else {
            info!(url = %nats_config.url, "Connecting to NATS without TLS");
        }

        // 健壮的心跳与超时，避免长时间空闲后连接“假存活”
        options = options
            .ping_interval(Duration::from_secs(20))
            .connection_timeout(Duration::from_secs(5));

        // 连接到 NATS
        let client = match options.connect(&nats_config.url).await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to connect to NATS");
                return Err(e).context("Failed to connect to NATS");
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
        let backoff = crate::backoff::network_publish_backoff();
        let client = crate::backoff::execute_with_backoff(
            || async {
                Self::connect_with_config(nats_config, tls_config)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))
            },
            backoff,
        )
        .await
        .context("Failed to connect to NATS with retry")?;

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
