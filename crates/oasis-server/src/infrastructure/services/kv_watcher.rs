use anyhow::Result;
use async_nats::jetstream::kv::Operation as KvOp;
use futures::StreamExt;
use oasis_core::{
    JS_KV_NODE_FACTS, JS_KV_NODE_HEARTBEAT, JS_KV_NODE_LABELS,
    agent::AgentHeartbeat,
    types::{AgentFacts, AgentLabels},
};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::domain::events::NodeEvent;
use crate::domain::events::NodeEvent::{FactsUpdated, LabelsUpdated, Offline, Online};
use oasis_core::backoff::{execute_with_backoff, kv_operations_backoff};

/// KV 监听服务 - 监听各种 KV 存储的变化并发送相应的事件
pub struct KvWatcherService {
    jetstream: async_nats::jetstream::Context,
    event_sender: broadcast::Sender<NodeEvent>,
    shutdown_token: tokio_util::sync::CancellationToken,
}

impl KvWatcherService {
    pub fn new(
        jetstream: async_nats::jetstream::Context,
        event_sender: broadcast::Sender<NodeEvent>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            jetstream,
            event_sender,
            shutdown_token,
        }
    }

    /// 启动所有 KV 监听器
    pub async fn start_watching(&self) -> Result<Vec<tokio::task::JoinHandle<()>>> {
        let mut handles = Vec::new();
        handles.push(self.start_heartbeat_watcher().await?);
        handles.push(self.start_labels_watcher().await?);
        handles.push(self.start_facts_watcher().await?);
        info!("All KV watchers started successfully");
        Ok(handles)
    }

    /// 通用 watcher 启动器：负责自愈、停机响应与事件派发
    async fn spawn_watcher(
        &self,
        bucket: &str,
        handler: Arc<
            dyn Fn(async_nats::jetstream::kv::Entry) -> Vec<NodeEvent> + Send + Sync + 'static,
        >,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let js = self.jetstream.clone();
        let tx = self.event_sender.clone();
        let shutdown = self.shutdown_token.clone();

        let bucket_name = bucket.to_string();
        let handle = tokio::spawn(async move {
            loop {
                let result = execute_with_backoff(
                    || async {
                        let kv = js.get_key_value(&bucket_name).await?;
                        let mut watcher = kv.watch_all().await?;
                        info!(bucket = %bucket_name, "KV watcher started");
                        loop {
                            tokio::select! {
                                _ = shutdown.cancelled() => {
                                    info!(bucket = %bucket_name, "Shutdown signal received, stopping KV watcher");
                                    return Ok::<(), anyhow::Error>(() );
                                }
                                next = watcher.next() => {
                                    match next {
                                        Some(entry_result) => match entry_result {
                                            Ok(entry) => {
                                                let events = (handler)(entry);
                                                for ev in events {
                                                    if tx.send(ev).is_err() {
                                                        warn!(bucket = %bucket_name, "Failed to broadcast KV event");
                                                    }
                                                }
                                            }
                                            Err(e) => { error!(bucket = %bucket_name, error = %e, "KV watcher error"); }
                                        },
                                        None => {
                                            info!(bucket = %bucket_name, "KV watcher stream ended; will recreate with backoff");
                                            return Err(anyhow::anyhow!("watcher stream ended"));
                                        }
                                    }
                                }
                            }
                        }
                    },
                    kv_operations_backoff(),
                ).await;

                if shutdown.is_cancelled() {
                    break;
                }
                if let Err(e) = result {
                    error!(bucket = %bucket_name, error = %e, "KV watcher failed, will retry");
                }
            }
        });

        Ok(handle)
    }

    /// 启动心跳监听器
    async fn start_heartbeat_watcher(&self) -> Result<tokio::task::JoinHandle<()>> {
        let handler: Arc<dyn Fn(async_nats::jetstream::kv::Entry) -> Vec<NodeEvent> + Send + Sync> =
            Arc::new(|entry| {
                let agent_id = entry.key.to_string();
                match entry.operation {
                    KvOp::Put => {
                        if let Ok(proto) = oasis_core::proto_impls::encoding::from_slice::<
                            oasis_core::proto::AgentHeartbeat,
                        >(entry.value.as_ref())
                        {
                            let hb: AgentHeartbeat = (&proto).into();
                            debug!(agent_id = %hb.agent_id, status = ?hb.status, "Agent heartbeat received");
                        } else {
                            warn!(agent_id = %agent_id, "Failed to decode AgentHeartbeat payload");
                        }
                        vec![Online {
                            node_id: agent_id,
                            timestamp: chrono::Utc::now(),
                        }]
                    }
                    KvOp::Delete | KvOp::Purge => {
                        vec![Offline {
                            node_id: agent_id,
                            timestamp: chrono::Utc::now(),
                        }]
                    }
                }
            });
        self.spawn_watcher(JS_KV_NODE_HEARTBEAT, handler).await
    }

    /// 启动标签监听器
    async fn start_labels_watcher(&self) -> Result<tokio::task::JoinHandle<()>> {
        let handler: Arc<dyn Fn(async_nats::jetstream::kv::Entry) -> Vec<NodeEvent> + Send + Sync> =
            Arc::new(|entry| {
                let agent_id = entry
                    .key
                    .strip_prefix("agent:labels:")
                    .unwrap_or(&entry.key)
                    .to_string();
                match entry.operation {
                    KvOp::Put => {
                        match oasis_core::proto_impls::encoding::from_slice::<
                            oasis_core::proto::AgentLabels,
                        >(entry.value.as_ref())
                        {
                            Ok(proto) => {
                                let labels: AgentLabels = (&proto).into();
                                let map = labels.labels.clone();
                                vec![LabelsUpdated {
                                    node_id: agent_id,
                                    labels: map,
                                    timestamp: chrono::Utc::now(),
                                }]
                            }
                            Err(e) => {
                                warn!(agent_id = %agent_id, error = %e, "Failed to decode AgentLabels payload");
                                Vec::new()
                            }
                        }
                    }
                    KvOp::Delete | KvOp::Purge => {
                        debug!(agent_id = %agent_id, "Agent labels deleted");
                        Vec::new()
                    }
                }
            });
        self.spawn_watcher(JS_KV_NODE_LABELS, handler).await
    }

    /// 启动事实监听器
    async fn start_facts_watcher(&self) -> Result<tokio::task::JoinHandle<()>> {
        let handler: Arc<dyn Fn(async_nats::jetstream::kv::Entry) -> Vec<NodeEvent> + Send + Sync> =
            Arc::new(|entry| {
                let agent_id = entry
                    .key
                    .strip_prefix("agent:facts:")
                    .unwrap_or(&entry.key)
                    .to_string();
                match entry.operation {
                    KvOp::Put => {
                        match oasis_core::proto_impls::encoding::from_slice::<
                            oasis_core::proto::AgentFacts,
                        >(entry.value.as_ref())
                        {
                            Ok(proto) => {
                                let facts: AgentFacts = (&proto).into();
                                vec![FactsUpdated {
                                    node_id: agent_id,
                                    facts: facts.clone(),
                                    timestamp: chrono::Utc::now(),
                                }]
                            }
                            Err(e) => {
                                warn!(agent_id = %agent_id, error = %e, "Failed to decode AgentFacts payload");
                                Vec::new()
                            }
                        }
                    }
                    KvOp::Delete | KvOp::Purge => {
                        debug!(agent_id = %agent_id, "Agent facts deleted");
                        Vec::new()
                    }
                }
            });
        self.spawn_watcher(JS_KV_NODE_FACTS, handler).await
    }
}
