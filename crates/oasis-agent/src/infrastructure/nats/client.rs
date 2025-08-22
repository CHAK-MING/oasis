use anyhow::{Context, Result};
use async_nats::{jetstream, Client, ConnectOptions};

use oasis_core::config::NatsConfig;

#[derive(Clone)]
pub struct NatsClient {
    pub client: Client,
    pub jetstream: jetstream::Context,
}

impl NatsClient {
    pub async fn connect(config: &NatsConfig) -> Result<Self> {
        let mut options = ConnectOptions::new()
            .name("oasis-agent-new")
            .retry_on_initial_connect();

        // TLS 根证书
        if let Some(ca) = &config.tls_ca_file {
            if ca.exists() {
                options = options.add_root_certificates(ca.clone());
            }
        }
        // TLS 客户端证书
        if let (Some(cert), Some(key)) = (&config.tls_cert_file, &config.tls_key_file) {
            if cert.exists() && key.exists() {
                options = options.add_client_certificate(cert.clone(), key.clone());
            }
        }
        // JWT 凭证（.creds 文件）
        if let Some(creds) = &config.creds_file {
            if creds.exists() {
                let content =
                    std::fs::read_to_string(creds).context("failed to read creds file")?;
                options = options
                    .credentials(&content)
                    .context("failed to set credentials")?;
            }
        }

        // 连接（启用 NATS 内置的初始重连）
        let client = options
            .connect(&config.url)
            .await
            .context("failed to connect to NATS")?;
        // TLS 强制性检查：当配置要求 TLS 时，拒绝非加密连接
        if config.tls_required {
            // 如果 URL 不是 tls:// 或 wss://，也应拒绝
            let scheme = url::Url::parse(&config.url)
                .ok()
                .map(|u| u.scheme().to_string());
            let scheme_is_tls = matches!(scheme.as_deref(), Some("tls") | Some("wss"));
            if !scheme_is_tls {
                anyhow::bail!("配置要求 TLS，但连接 URL 不是 tls/wss：{}", config.url);
            }
        }

        let jetstream = jetstream::new(client.clone());
        Ok(Self { client, jetstream })
    }
}
