use anyhow::{Context, Result};
use oasis_core::{
    agent::AgentHeartbeat,
    constants::{self, JS_KV_NODE_HEARTBEAT},
    task::TaskExecution,
};

use super::client::NatsClient;
use async_nats::HeaderMap;

pub struct NatsPublisher {
    client: NatsClient,
}

impl NatsPublisher {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }

    pub async fn publish_heartbeat(&self, hb: &AgentHeartbeat) -> Result<()> {
        // Heartbeat 写入 KV（短 TTL 由 bucket 配置控制）
        // 由服务器统一 ensure；Agent 仅获取，不再创建
        let kv = self
            .client
            .jetstream
            .get_key_value(JS_KV_NODE_HEARTBEAT)
            .await
            .context("bind heartbeat kv")?;

        let key = constants::kv_key_heartbeat(hb.agent_id.as_str());
        let proto: oasis_core::proto::AgentHeartbeat = hb.into();
        let data = oasis_core::proto_impls::encoding::to_vec(&proto);
        kv.put(&key, data.into())
            .await
            .context("kv put heartbeat")?;
        Ok(())
    }

    pub async fn publish_task_result(&self, exec: &TaskExecution) -> Result<()> {
        let subject = constants::result_subject_for_typed(&exec.task_id, &exec.agent_id);
        // Protobuf 编码 TaskExecution
        let proto: oasis_core::proto::TaskExecutionMsg = exec.into();
        let data = oasis_core::proto_impls::encoding::to_vec(&proto);

        // 设置 Nats-Msg-Id 以启用 JetStream 去重
        let mut headers = HeaderMap::new();
        let dedupe_key = format!("{}@{}", exec.task_id.as_str(), subject);
        headers.insert("Nats-Msg-Id", dedupe_key);

        // 发布并等待 ACK 确认，确保消息成功写入 JetStream（带 headers）
        let ack = self
            .client
            .jetstream
            .publish_with_headers(subject.clone(), headers, data.into())
            .await
            .context("publish task execution with headers")?;

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
