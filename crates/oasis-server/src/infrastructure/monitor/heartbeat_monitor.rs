use async_nats::jetstream::Context;
use dashmap::DashMap;
use futures::StreamExt;
use oasis_core::{agent_types::AgentStatus, constants::*, core_types::AgentId, error::Result};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Agent 心跳状态信息
#[derive(Debug, Clone)]
pub struct AgentHeartbeatInfo {
    pub status: AgentStatus,
    pub last_heartbeat: i64,
    pub updated_at: i64, // 本地更新时间
    #[allow(dead_code)]
    pub remove_reason: Option<String>, // 移除原因
}

impl AgentHeartbeatInfo {
    pub fn new(status: AgentStatus, last_heartbeat: i64) -> Self {
        Self {
            status,
            last_heartbeat,
            updated_at: chrono::Utc::now().timestamp(),
            remove_reason: None,
        }
    }

    pub fn online(last_heartbeat: i64) -> Self {
        Self::new(AgentStatus::Online, last_heartbeat)
    }

    pub fn removed(remove_reason: String) -> Self {
        Self {
            status: AgentStatus::Removed,
            last_heartbeat: 0,
            updated_at: chrono::Utc::now().timestamp(),
            remove_reason: Some(remove_reason),
        }
    }
}

/// 心跳监控器
/// 负责监控所有 Agent 的心跳状态，维护内存中的状态缓存
#[derive(Debug, Clone)]
pub struct HeartbeatMonitor {
    jetstream: Arc<Context>,
    /// Agent 状态缓存: AgentId -> (Status, LastHeartbeat)
    status_cache: Arc<DashMap<AgentId, AgentHeartbeatInfo>>,
    /// 心跳超时时间（秒）
    heartbeat_timeout: u64,
    shutdown_token: CancellationToken,
}

impl HeartbeatMonitor {
    /// 创建新的心跳监控器
    pub fn new(
        jetstream: Arc<Context>,
        heartbeat_timeout: u64,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            jetstream,
            status_cache: Arc::new(DashMap::new()),
            heartbeat_timeout,
            shutdown_token,
        }
    }

    /// 启动心跳监控器
    pub async fn start(&self) -> Result<()> {
        info!(
            "Starting HeartbeatMonitor (timeout: {}s)",
            self.heartbeat_timeout
        );
        // 1. 启动定期扫描任务
        self.start_periodic_scan().await?;

        // 2. 启动心跳数据监听器
        self.start_heartbeat_watcher().await?;

        info!("HeartbeatMonitor started successfully");
        Ok(())
    }

    /// 以后台任务方式启动，返回 JoinHandle，便于生命周期管理注册
    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.start().await {
                error!("HeartbeatMonitor start error: {}", e);
            }
            // 等待关闭
            self.shutdown_token.cancelled().await;
        })
    }

    /// 启动定期扫描任务，检测心跳超时
    async fn start_periodic_scan(&self) -> Result<()> {
        let status_cache = self.status_cache.clone();
        let heartbeat_timeout = self.heartbeat_timeout;
        let shutdown_token = self.shutdown_token.clone();

        tokio::spawn(async move {
            info!("Starting periodic heartbeat timeout scanner");
            let mut interval = tokio::time::interval(Duration::from_secs(30));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let current_time = chrono::Utc::now().timestamp();
                        let mut timeout_count = 0;

                        // 只检查在线 Agent 的超时情况
                        for mut entry in status_cache.iter_mut() {
                            let agent_id = entry.key().clone();
                            let info = entry.value_mut();

                            // 只处理在线状态的 Agent
                            if matches!(info.status, AgentStatus::Online) {
                                let age = current_time - info.last_heartbeat;
                                if age > heartbeat_timeout as i64 {
                                    info.status = AgentStatus::Offline;
                                    info.updated_at = current_time;
                                    timeout_count += 1;
                                    debug!("Agent {} timed out (age: {}s)", agent_id, age);
                                }
                            }
                        }

                        if timeout_count > 0 {
                            debug!("Heartbeat scan: {} agents timed out", timeout_count);
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("Heartbeat timeout scanner stopped");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// 启动心跳数据监听器，实时更新
    async fn start_heartbeat_watcher(&self) -> Result<()> {
        let jetstream = self.jetstream.clone();
        let status_cache = self.status_cache.clone();
        let shutdown_token = self.shutdown_token.clone();

        tokio::spawn(async move {
            info!("Starting heartbeat data watcher");
            let mut backoff_ms: u64 = 500;
            loop {
                // 1) 获取 KV store
                let heartbeat_store = match jetstream.get_key_value(JS_KV_AGENT_HEARTBEAT).await {
                    Ok(store) => store,
                    Err(e) => {
                        error!("Failed to get heartbeat store for watcher: {}", e);
                        // 等待后重试或退出
                        tokio::select! {
                            _ = shutdown_token.cancelled() => { info!("Heartbeat watcher shutdown requested"); return; }
                            _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                        }
                        backoff_ms = (backoff_ms.saturating_mul(2)).min(30_000);
                        continue;
                    }
                };

                // 2) 建立 watcher
                match heartbeat_store.watch_all().await {
                    Ok(mut watcher) => {
                        info!("Heartbeat watcher established");
                        backoff_ms = 500; // 成功后重置退避

                        loop {
                            tokio::select! {
                                msg = watcher.next() => {
                                    match msg {
                                        Some(Ok(entry)) => {
                                            Self::handle_heartbeat_change(entry, &status_cache).await;
                                        }
                                        Some(Err(e)) => {
                                            warn!("Error watching heartbeat changes: {}", e);
                                        }
                                        None => {
                                            warn!("Heartbeat watcher stream ended; will restart after backoff");
                                            break; // 跳出内层循环，重新建立 watcher
                                        }
                                    }
                                }
                                _ = shutdown_token.cancelled() => {
                                    info!("Heartbeat watcher stopped");
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to start heartbeat watcher: {}", e);
                    }
                }

                // 3) 退避后重试
                tokio::select! {
                    _ = shutdown_token.cancelled() => { info!("Heartbeat watcher shutdown requested"); return; }
                    _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                }
                backoff_ms = (backoff_ms.saturating_mul(2)).min(30_000);
            }
        });

        Ok(())
    }

    /// 处理心跳变更事件
    async fn handle_heartbeat_change(
        entry: async_nats::jetstream::kv::Entry,
        status_cache: &Arc<DashMap<AgentId, AgentHeartbeatInfo>>,
    ) {
        let key = entry.key;

        let agent_id = AgentId::from(key.clone());

        match entry.operation {
            async_nats::jetstream::kv::Operation::Put => {
                // 心跳更新 - Agent 恢复在线
                if let Ok(timestamp_str) = String::from_utf8(entry.value.to_vec()) {
                    if let Ok(last_heartbeat) = timestamp_str.parse::<i64>() {
                        status_cache
                            .insert(agent_id.clone(), AgentHeartbeatInfo::online(last_heartbeat));
                        debug!("Agent {} heartbeat updated: {}", agent_id, last_heartbeat);
                    } else {
                        warn!(
                            "Invalid heartbeat timestamp for {}: {}",
                            agent_id, timestamp_str
                        );
                    }
                } else {
                    warn!("Invalid heartbeat data for {}", agent_id);
                }
            }
            async_nats::jetstream::kv::Operation::Delete => {
                // 显式删除 - 标记为已移除
                status_cache.insert(
                    agent_id.clone(),
                    AgentHeartbeatInfo::removed("Explicitly deleted via API".to_string()),
                );
                info!("Agent {} explicitly removed from heartbeat store", agent_id);
            }
            async_nats::jetstream::kv::Operation::Purge => {
                // 清除操作 - 也标记为已移除
                status_cache.insert(
                    agent_id.clone(),
                    AgentHeartbeatInfo::removed("Purged from storage".to_string()),
                );
                info!("Agent {} purged from heartbeat store", agent_id);
            }
        }
    }

    /// 获取所有状态映射的快照
    #[allow(dead_code)]
    pub fn get_status_snapshot(&self) -> std::collections::HashMap<AgentId, AgentHeartbeatInfo> {
        self.status_cache
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// 获取指定 Agent 的状态
    #[allow(dead_code)]
    pub fn get_agent_status(&self, agent_id: &AgentId) -> Option<AgentHeartbeatInfo> {
        self.status_cache
            .get(agent_id)
            .map(|entry| entry.value().clone())
    }

    /// 批量检查 Agent 在线状态
    #[allow(dead_code)]
    pub fn batch_check_online(
        &self,
        agent_ids: &[AgentId],
    ) -> std::collections::HashMap<AgentId, bool> {
        let mut result = std::collections::HashMap::with_capacity(agent_ids.len());

        for agent_id in agent_ids {
            let is_online = self
                .status_cache
                .get(agent_id)
                .map(|entry| matches!(entry.value().status, AgentStatus::Online))
                .unwrap_or(false);
            result.insert(agent_id.clone(), is_online);
        }

        result
    }

    /// 过滤出在线的 Agent
    pub fn filter_online_agents(&self, agent_ids: Vec<AgentId>) -> Vec<AgentId> {
        agent_ids
            .into_iter()
            .filter(|agent_id| {
                self.status_cache
                    .get(agent_id)
                    .map(|entry| matches!(entry.value().status, AgentStatus::Online))
                    .unwrap_or(false)
            })
            .collect()
    }

    /// 获取在线 Agent 数量
    #[allow(dead_code)]
    pub fn get_online_count(&self) -> usize {
        self.status_cache
            .iter()
            .filter(|entry| matches!(entry.value().status, AgentStatus::Online))
            .count()
    }

    /// 获取统计信息
    #[allow(dead_code)]
    pub fn get_stats(&self) -> HeartbeatMonitorStats {
        let total = self.status_cache.len();
        let online = self.get_online_count();
        let offline = total - online;

        HeartbeatMonitorStats {
            total_agents: total,
            online_agents: online,
            offline_agents: offline,
            heartbeat_timeout: self.heartbeat_timeout,
        }
    }
}

/// 心跳监控器统计信息
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct HeartbeatMonitorStats {
    pub total_agents: usize,
    pub online_agents: usize,
    pub offline_agents: usize,
    pub heartbeat_timeout: u64,
}
