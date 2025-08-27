use anyhow::{Context, Result};
use async_nats::jetstream;
use oasis_core::{
    backoff::{execute_with_backoff, network_publish_backoff},
    transport::NatsClientFactory,
};
use tracing::info;

/// 使用配置创建 JetStream 上下文
pub async fn create_jetstream_context_with_config(
    config: &oasis_core::config::NatsConfig,
) -> Result<jetstream::Context> {
    info!("Connecting to NATS (JetStream) url={}", config.url);

    let backoff = network_publish_backoff();
    let client = execute_with_backoff(
        || async {
            NatsClientFactory::connect_with_config(config)
                .await
                .map_err(|e| anyhow::anyhow!(e))
        },
        backoff,
    )
    .await
    .context("Failed to connect to NATS")?;

    let js = jetstream::new(client);
    info!("JetStream context created successfully");
    Ok(js)
}
