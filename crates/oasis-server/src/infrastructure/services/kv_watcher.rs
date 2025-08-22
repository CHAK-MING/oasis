use anyhow::Result;
use async_nats::jetstream::kv::Operation as KvOp;
use futures::StreamExt;
use oasis_core::{
    types::{AgentFacts, AgentHeartbeat, AgentLabels}, 
    JS_KV_NODE_FACTS, JS_KV_NODE_HEARTBEAT,
    JS_KV_NODE_LABELS,
    backoff::{execute_with_backoff, kv_operations_backoff},
};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::domain::events::{NodeEvent, NodeEvent::*};
use crate::config::ServerConfig;

/// KV 监听器服务
pub struct KvWatcherService {
    jetstream: async_nats::jetstream::Context,
    event_sender: broadcast::Sender<NodeEvent>,
    shutdown_token: tokio_util::sync::CancellationToken,
    config: ServerConfig,
}

impl KvWatcherService {
    pub fn new(
        jetstream: async_nats::jetstream::Context,
        event_sender: broadcast::Sender<NodeEvent>,
        shutdown_token: tokio_util::sync::CancellationToken,
        config: ServerConfig,
    ) -> Self {
        Self {
            jetstream,
            event_sender,
            shutdown_token,
            config,
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

    /// 启动心跳监听器
    async fn start_heartbeat_watcher(&self) -> Result<tokio::task::JoinHandle<()>> {
        let jetstream = self.jetstream.clone();
        let event_sender = self.event_sender.clone();
        let shutdown_token = self.shutdown_token.clone();
        let heartbeat_ttl_sec = self.config.server.heartbeat_ttl_sec;

        let handle = tokio::spawn(async move {
            let result = execute_with_backoff(
                || async {
                    let kv = jetstream.get_key_value(JS_KV_NODE_HEARTBEAT).await?;
                    let mut watcher = kv.watch_all().await?;
                    info!("Heartbeat watcher started");
                    loop {
                        tokio::select! {
                            // 监听关闭信号
                            _ = shutdown_token.cancelled() => {
                                info!("Shutdown signal received, stopping heartbeat watcher");
                                break;
                            }
                            // 接收新事件
                            entry = watcher.next() => {
                                match entry {
                                    Some(entry) => {
                                        match entry {
                                            Ok(entry) => {
                                                // Agent 发送的心跳键就是 agent_id 本身
                                                let agent_id = entry.key.to_string();
                                                match entry.operation {
                                                    KvOp::Put => {
                                                        if let Ok(heartbeat) = rmp_serde::from_slice::<AgentHeartbeat>(&entry.value) {
                                                            // 检查心跳时间戳是否在有效期内
                                                            let now = chrono::Utc::now();
                                                            let heartbeat_age = now.signed_duration_since(chrono::DateTime::from_timestamp(heartbeat.last_seen, 0).unwrap_or(now)).num_seconds() as u64;
                                                            
                                                            if heartbeat_age <= heartbeat_ttl_sec {
                                                                // 心跳在有效期内，发送 Online 事件
                                                                let event = Online { node_id: agent_id.clone(), timestamp: now };
                                                                if let Err(e) = event_sender.send(event) { 
                                                                    warn!(error = %e, "Failed to send node online event"); 
                                                                }
                                                                debug!(agent_id = %heartbeat.agent_id, status = ?heartbeat.status, "Agent heartbeat updated");
                                                            } else {
                                                                // 心跳已过期，发送 Offline 事件
                                                                let event = Offline { node_id: agent_id.clone(), timestamp: now };
                                                                if let Err(e) = event_sender.send(event) { 
                                                                    warn!(error = %e, "Failed to send node offline event"); 
                                                                }
                                                                debug!(agent_id = %agent_id, "Agent heartbeat expired (age: {}s, ttl: {}s)", heartbeat_age, heartbeat_ttl_sec);
                                                            }
                                                        }
                                                    }
                                                    KvOp::Delete | KvOp::Purge => {
                                                        let event = Offline { node_id: agent_id.clone(), timestamp: chrono::Utc::now() };
                                                        if let Err(e) = event_sender.send(event) { warn!(error = %e, "Failed to send node offline event"); }
                                                        debug!(agent_id = %agent_id, "Agent heartbeat deleted");
                                                    }
                                                }
                                            }
                                            Err(e) => { error!(error = %e, "Heartbeat watcher error"); }
                                        }
                                    }
                                    None => {
                                        info!("Heartbeat watcher stream ended");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                },
                kv_operations_backoff(),
            ).await;

            if let Err(e) = result {
                error!(error = %e, "Heartbeat watcher failed");
            }
        });

        Ok(handle)
    }

    /// 启动标签监听器
    async fn start_labels_watcher(&self) -> Result<tokio::task::JoinHandle<()>> {
        let jetstream = self.jetstream.clone();
        let event_sender = self.event_sender.clone();
        let shutdown_token = self.shutdown_token.clone();

        let handle = tokio::spawn(async move {
            let result = execute_with_backoff(
                || async {
                    let kv = jetstream.get_key_value(JS_KV_NODE_LABELS).await?;
                    let mut watcher = kv.watch_all().await?;
                    info!("Labels watcher started");
                    loop {
                        tokio::select! {
                            // 监听关闭信号
                            _ = shutdown_token.cancelled() => {
                                info!("Shutdown signal received, stopping labels watcher");
                                break;
                            }
                            // 接收新事件
                            entry = watcher.next() => {
                                match entry {
                                    Some(entry) => {
                                        match entry {
                                            Ok(entry) => {
                                                let agent_id = entry.key.strip_prefix("agent:labels:")
                                                    .unwrap_or(&entry.key)
                                                    .to_string();
                                                match entry.operation {
                                                    KvOp::Put => {
                                                        if let Ok(labels) = rmp_serde::from_slice::<AgentLabels>(&entry.value) {
                                                            let labels_clone = labels.labels.clone();
                                                            let event = LabelsUpdated { node_id: agent_id.clone(), labels: labels_clone, timestamp: chrono::Utc::now() };
                                                            if let Err(e) = event_sender.send(event) { warn!(error = %e, "Failed to send labels updated event"); }
                                                            debug!(agent_id = %labels.agent_id, labels_count = labels.labels.len(), "Agent labels updated");
                                                        }
                                                    }
                                                    KvOp::Delete | KvOp::Purge => {
                                                        debug!(agent_id = %agent_id, "Agent labels deleted");
                                                    }
                                                }
                                            }
                                            Err(e) => { error!(error = %e, "Labels watcher error"); }
                                        }
                                    }
                                    None => {
                                        info!("Labels watcher stream ended");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                },
                kv_operations_backoff(),
            ).await;

            if let Err(e) = result {
                error!(error = %e, "Labels watcher failed");
            }
        });

        Ok(handle)
    }

    /// 启动事实监听器
    async fn start_facts_watcher(&self) -> Result<tokio::task::JoinHandle<()>> {
        let jetstream = self.jetstream.clone();
        let event_sender = self.event_sender.clone();
        let shutdown_token = self.shutdown_token.clone();

        let handle = tokio::spawn(async move {
            let result = execute_with_backoff(
                || async {
                    let kv = jetstream.get_key_value(JS_KV_NODE_FACTS).await?;
                    let mut watcher = kv.watch_all().await?;
                    info!("Facts watcher started");
                    loop {
                        tokio::select! {
                            // 监听关闭信号
                            _ = shutdown_token.cancelled() => {
                                info!("Shutdown signal received, stopping facts watcher");
                                break;
                            }
                            // 接收新事件
                            entry = watcher.next() => {
                                match entry {
                                    Some(entry) => {
                                        match entry {
                                            Ok(entry) => {
                                                let agent_id = entry.key.strip_prefix("agent:facts:")
                                                    .unwrap_or(&entry.key)
                                                    .to_string();
                                                match entry.operation {
                                                    KvOp::Put => {
                                                        if let Ok(facts) = rmp_serde::from_slice::<AgentFacts>(&entry.value) {
                                                            let facts_json = serde_json::to_string(&facts).unwrap_or_else(|_| "{}".to_string());
                                                            let event = FactsUpdated { node_id: agent_id, facts: facts_json, timestamp: chrono::Utc::now() };
                                                            if let Err(e) = event_sender.send(event) { warn!(error = %e, "Failed to send facts updated event"); }
                                                            debug!(agent_id = %facts.agent_id, hostname = %facts.hostname, "Agent facts updated");
                                                        }
                                                    }
                                                    KvOp::Delete | KvOp::Purge => {
                                                        debug!(agent_id = %agent_id, "Agent facts deleted");
                                                    }
                                                }
                                            }
                                            Err(e) => { error!(error = %e, "Facts watcher error"); }
                                        }
                                    }
                                    None => {
                                        info!("Facts watcher stream ended");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                },
                kv_operations_backoff(),
            ).await;

            if let Err(e) = result {
                error!(error = %e, "Facts watcher failed");
            }
        });

        Ok(handle)
    }
}
