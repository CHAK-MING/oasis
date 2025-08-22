use anyhow::{Context, Result};
use oasis_core::{
    agent::AgentHeartbeat,
    constants::{self, JS_KV_NODE_HEARTBEAT},
    task::TaskExecution,
};

use super::client::NatsClient;

pub struct NatsPublisher {
    client: NatsClient,
}

impl NatsPublisher {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }

    pub async fn publish_heartbeat(&self, hb: &AgentHeartbeat) -> Result<()> {
        // Heartbeat 写入 KV（短 TTL 由 bucket 配置控制）
        let kv = match self
            .client
            .jetstream
            .get_key_value(JS_KV_NODE_HEARTBEAT)
            .await
        {
            Ok(kv) => kv,
            Err(_) => self
                .client
                .jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: JS_KV_NODE_HEARTBEAT.to_string(),
                    history: 1,
                    ..Default::default()
                })
                .await
                .context("create heartbeat kv")?,
        };

        let key = constants::kv_key_heartbeat(hb.agent_id.as_str());
        let data = rmp_serde::to_vec(hb).context("serialize heartbeat")?;
        kv.put(&key, data.into())
            .await
            .context("kv put heartbeat")?;
        Ok(())
    }

    pub async fn publish_task_result(&self, exec: &TaskExecution) -> Result<()> {
        let subject = constants::result_subject_for_typed(&exec.task_id, &exec.agent_id);
        let data = serde_json::to_vec(exec).context("serialize task execution")?;
        
        // 发布并等待 ACK 确认，确保消息成功写入 JetStream
        let ack = self.client
            .jetstream
            .publish(subject.clone(), data.into())
            .await
            .context("publish task execution")?;
        
        // 等待 ACK 确认
        ack.await.context("wait for publish ack")?;
        
        tracing::info!(
            "Task result published successfully: task_id={}, agent_id={}, subject={}",
            exec.task_id.as_str(),
            exec.agent_id.as_str(),
            subject
        );
        
        Ok(())
    }
}
