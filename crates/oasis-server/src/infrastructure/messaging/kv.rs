use anyhow::Result;
use async_nats::jetstream::kv::Operation as KvOp;
use futures::StreamExt;
use oasis_core::{
    JS_KV_CONFIG, JS_KV_NODE_FACTS, JS_KV_NODE_HEARTBEAT, JS_KV_NODE_LABELS,
    agent::{AgentFacts, AgentHeartbeat, AgentLabels},
};
use prost::Message;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 确保KV存储存在并启动监听器
pub async fn ensure_kv_and_watch(
    js: &async_nats::jetstream::Context,
    cancel_token: CancellationToken,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    // 硬编码配置值
    let ttl_sec = 60; // 心跳 TTL
    let facts_history = 10; // Facts 历史版本数
    let labels_history = 10; // Labels 历史版本数
    let max_value_size = 1024 * 1024; // 1MB

    info!("Creating KV buckets with heartbeat TTL: {}s", ttl_sec);

    // Facts KV - 长期保存，版本化
    let kv_facts = match js.get_key_value(JS_KV_NODE_FACTS).await {
        Ok(kv) => kv,
        Err(_) => {
            js.create_key_value(async_nats::jetstream::kv::Config {
                bucket: JS_KV_NODE_FACTS.to_string(),
                description: "Agent facts (versioned, no TTL)".to_string(),
                history: facts_history as i64,
                max_value_size: max_value_size as i32,
                ..Default::default()
            })
            .await?
        }
    };

    // Heartbeat KV - 短期保存，TTL 自动清理
    let kv_heartbeat = match js.get_key_value(JS_KV_NODE_HEARTBEAT).await {
        Ok(kv) => kv,
        Err(_) => {
            js.create_key_value(async_nats::jetstream::kv::Config {
                bucket: JS_KV_NODE_HEARTBEAT.to_string(),
                description: "Agent heartbeat (TTL-based cleanup)".to_string(),
                max_age: Duration::from_secs(ttl_sec), // TTL = 2x 心跳间隔
                history: 1,                            // 只保留最新值
                ..Default::default()
            })
            .await?
        }
    };

    // Labels KV - 长期保存，可变更
    let kv_labels = match js.get_key_value(JS_KV_NODE_LABELS).await {
        Ok(kv) => kv,
        Err(_) => {
            js.create_key_value(async_nats::jetstream::kv::Config {
                bucket: JS_KV_NODE_LABELS.to_string(),
                description: "Agent labels (versioned, no TTL)".to_string(),
                history: labels_history as i64,
                max_value_size: max_value_size as i32,
                ..Default::default()
            })
            .await?
        }
    };

    // OASIS-CONFIG KV - 长期保存，可变更
    let kv_config = match js.get_key_value(JS_KV_CONFIG).await {
        Ok(kv) => kv,
        Err(_) => {
            js.create_key_value(async_nats::jetstream::kv::Config {
                bucket: JS_KV_CONFIG.to_string(),
                description: "OASIS configuration (versioned, no TTL)".to_string(),
                history: 50, // 使用默认值
                max_value_size: max_value_size as i32,
                ..Default::default()
            })
            .await?
        }
    };

    // 启动KV监听器
    let mut handles = Vec::new();

    handles.push(spawn_resilient_watcher(
        kv_heartbeat.clone(),
        cancel_token.clone(),
        |entry| {
            if let Ok(proto) = oasis_core::proto::AgentHeartbeat::decode(entry.value.as_ref()) {
                let heartbeat: AgentHeartbeat = (&proto).into();
                debug!(agent_id = %heartbeat.agent_id, status = ?heartbeat.status, "Agent heartbeat updated");
            }
        },
    ));

    // Facts监听器
    handles.push(spawn_resilient_watcher(
        kv_facts.clone(),
        cancel_token.clone(),
        |entry| {
            if let Ok(proto) = oasis_core::proto::AgentFacts::decode(entry.value.as_ref()) {
                let facts: AgentFacts = (&proto).into();
                debug!(agent_id = %facts.agent_id, hostname = %facts.hostname, "Agent facts updated");
            }
        },
    ));

    // Labels监听器
    handles.push(spawn_resilient_watcher(
        kv_labels.clone(),
        cancel_token.clone(),
        |entry| {
            if let Ok(proto) = oasis_core::proto::AgentLabels::decode(entry.value.as_ref()) {
                let labels: AgentLabels = (&proto).into();
                debug!(agent_id = %labels.agent_id, labels_count = labels.labels.len(), "Agent labels updated");
            }
        },
    ));

    // OASIS-CONFIG监听器
    handles.push(spawn_resilient_watcher(
        kv_config.clone(),
        cancel_token.clone(),
        |entry| {
            debug!(key = %entry.key, "OASIS configuration updated");
        },
    ));

    info!("KV watchers started");
    Ok(handles)
}

/// 启动具备自我修复能力的 KV 监听器。
/// - 当 watch 流异常结束时，使用 backoff 重新建立 watcher
/// - 响应 cancel 以便优雅退出
fn spawn_resilient_watcher<F>(
    kv: async_nats::jetstream::kv::Store,
    cancel: CancellationToken,
    on_put: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(&async_nats::jetstream::kv::Entry) + Send + Sync + 'static,
{
    let on_put = std::sync::Arc::new(on_put);
    tokio::spawn(async move {
        let backoff = oasis_core::backoff::kv_operations_backoff();

        loop {
            if cancel.is_cancelled() {
                break;
            }
            let attempt = oasis_core::backoff::execute_with_backoff(
                || {
                    let kv = kv.clone();
                    let cancel_clone = cancel.clone();
                    let on_put_clone = on_put.clone();
                    async move {
                        let mut watcher = kv.watch_all().await?;
                        info!("KV watcher started");
                        while let Some(entry) = watcher.next().await {
                            if cancel_clone.is_cancelled() {
                                break;
                            }
                            match entry {
                                Ok(entry) => match entry.operation {
                                    KvOp::Put => {
                                        let cb = on_put_clone.clone();
                                        cb(&entry);
                                    }
                                    KvOp::Delete | KvOp::Purge => {
                                        debug!(key = %entry.key, "KV entry deleted/purged");
                                    }
                                },
                                Err(e) => {
                                    warn!(error = %e, "KV watcher stream error, will recreate");
                                    break;
                                }
                            }
                        }
                        Ok::<(), anyhow::Error>(())
                    }
                },
                backoff.clone(),
            )
            .await;

            if let Err(e) = attempt {
                error!(error = %e, "KV watcher failed to start, backing off before retry");
            }

            // 小延迟以避免紧凑重连风暴
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
}
