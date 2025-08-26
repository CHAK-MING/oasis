use anyhow::{Context, Result};
use async_nats::jetstream;
use oasis_core::transport::NatsClientFactory;
use tracing::info;

/// 使用配置创建 JetStream 上下文
pub async fn create_jetstream_context_with_config(
    config: &oasis_core::config::NatsConfig,
) -> Result<jetstream::Context> {
    info!("Connecting to NATS (JetStream) url={}", config.url);

    // TODO：手动退避，需要修改。
    let mut delay_ms = 300u64;
    for attempt in 1..=3 {
        match NatsClientFactory::connect_with_config(config).await {
            Ok(client) => {
                let js = jetstream::new(client);
                info!("JetStream context created successfully");
                return Ok(js);
            }
            Err(e) => {
                if attempt == 3 {
                    return Err(e).context("Failed to connect to NATS");
                }
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(3000);
            }
        }
    }

    unreachable!()
}
