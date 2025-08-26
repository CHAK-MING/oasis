//! Dead Letter Queue (DLQ) 处理模块

use crate::error::{CoreError, Result};
use crate::task::TaskSpec;
use crate::type_defs::AgentId;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::debug;

/// 死信条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub task: TaskSpec,
    pub error: String,
    pub agent_id: AgentId,
    pub retry_count: u32,
    #[serde(with = "time::serde::timestamp")]
    pub timestamp: OffsetDateTime,
}

impl DeadLetterEntry {
    pub fn new(task: TaskSpec, error: String, agent_id: AgentId, retry_count: u32) -> Self {
        Self {
            task,
            error,
            agent_id,
            retry_count,
            timestamp: OffsetDateTime::now_utc(),
        }
    }

    /// 生成 DLQ 键
    pub fn dlq_key(&self) -> String {
        format!("dlq:{}:{}", self.agent_id, self.task.id)
    }

    /// 生成通知主题
    pub fn notification_subject(&self) -> String {
        format!("oasis.dlq.{}", self.agent_id)
    }
}

/// 判断是否应该路由到 DLQ
pub fn should_route_to_dlq(error: &CoreError, retry_count: u32) -> bool {
    const MAX_RETRIES: u32 = 3;

    // 不可重试的错误或重试次数超限
    !error.is_retriable() || retry_count >= MAX_RETRIES
}

/// 存储和发布 DLQ 条目的便捷函数
pub async fn handle_dead_letter(
    kv_store: &async_nats::jetstream::kv::Store,
    js_context: &async_nats::jetstream::Context,
    entry: &DeadLetterEntry,
) -> Result<()> {
    // 存储到 KV（Protobuf）
    let proto: crate::proto::DeadLetterEntryMsg = entry.into();
    let payload = crate::proto_impls::encoding::to_vec(&proto);

    kv_store
        .put(&entry.dlq_key(), payload.into())
        .await
        .map_err(|e| CoreError::Nats {
            message: format!("Failed to store DLQ entry: {}", e),
        })?;

    // 发布通知（Protobuf）
    let notification_payload = crate::proto_impls::encoding::to_vec(&proto);

    js_context
        .publish(entry.notification_subject(), notification_payload.into())
        .await
        .map_err(|e| CoreError::Nats {
            message: format!("Failed to publish DLQ notification: {}", e),
        })?;

    debug!(
        task_id = %entry.task.id,
        agent_id = %entry.agent_id,
        retry_count = entry.retry_count,
        "Handled dead letter entry"
    );

    Ok(())
}

/// 发布 DLQ 条目的简化函数
pub async fn publish_dlq(
    js_context: &async_nats::jetstream::Context,
    entry: &DeadLetterEntry,
) -> Result<()> {
    let proto: crate::proto::DeadLetterEntryMsg = entry.into();
    let payload = crate::proto_impls::encoding::to_vec(&proto);

    js_context
        .publish(entry.notification_subject(), payload.into())
        .await
        .map_err(|e| CoreError::Nats {
            message: format!("Failed to publish DLQ: {}", e),
        })?;

    debug!(
        task_id = %entry.task.id,
        agent_id = %entry.agent_id,
        "Published DLQ entry"
    );

    Ok(())
}
