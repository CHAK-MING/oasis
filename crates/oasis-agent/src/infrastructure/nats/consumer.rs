use anyhow::{Context, Result};
use async_nats::jetstream;

use super::client::NatsClient;
use oasis_core::constants;

pub struct NatsConsumer {
    client: NatsClient,
}

impl NatsConsumer {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }

    pub async fn consume_tasks(
        &self,
        agent_id: &str,
    ) -> Result<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
        // 确保任务流存在（若不存在则由 Server 负责创建；这里尝试绑定）
        self.client
            .jetstream
            .get_stream(constants::JS_STREAM_TASKS)
            .await
            .context("bind to tasks stream")?;

        let filter_subject = constants::tasks_unicast_subject(agent_id);
        let durable_name = constants::unicast_consumer_name(agent_id);

        let consumer = self
            .client
            .jetstream
            .create_consumer_on_stream(
                jetstream::consumer::pull::Config {
                    durable_name: Some(durable_name),
                    filter_subject,
                    deliver_policy: jetstream::consumer::DeliverPolicy::All,
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    max_deliver: oasis_core::constants::DEFAULT_MAX_DELIVER,
                    // 提高 ack_wait 以覆盖长耗时任务
                    ack_wait: std::time::Duration::from_secs(120),
                    max_ack_pending: oasis_core::constants::DEFAULT_MAX_ACK_PENDING,
                    ..Default::default()
                },
                constants::JS_STREAM_TASKS,
            )
            .await
            .context("create tasks consumer")?;

        Ok(consumer)
    }
}
