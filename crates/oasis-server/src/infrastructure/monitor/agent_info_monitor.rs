use async_nats::jetstream::Context;
use dashmap::DashMap;
use futures::StreamExt;
use oasis_core::{
    agent_types::AgentInfo,
    constants::*,
    core_types::AgentId,
    error::{CoreError, ErrorSeverity, Result},
    proto,
};
use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 基于 KV 的 AgentInfo 监控器
/// - 维护 AgentInfo 快照
/// - 维护倒排索引：labels、system、groups
pub struct AgentInfoMonitor {
    jetstream: Arc<Context>,
    info_cache: Arc<DashMap<AgentId, Arc<AgentInfo>>>,
    index_labels: Arc<DashMap<(String, String), roaring::RoaringBitmap>>, // (k,v) -> bitmap
    index_system: Arc<DashMap<(String, String), roaring::RoaringBitmap>>, // (sys_k, v) -> bitmap
    index_groups: Arc<DashMap<String, roaring::RoaringBitmap>>,           // group -> bitmap
    shutdown_token: CancellationToken,
}

impl AgentInfoMonitor {
    pub fn new(jetstream: Arc<Context>, shutdown_token: CancellationToken) -> Self {
        Self {
            jetstream,
            info_cache: Arc::new(DashMap::new()),
            index_labels: Arc::new(DashMap::new()),
            index_system: Arc::new(DashMap::new()),
            index_groups: Arc::new(DashMap::new()),
            shutdown_token,
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting AgentInfoMonitor");
        self.initial_load().await?;
        self.start_watcher().await?;
        Ok(())
    }

    /// 以后台任务方式启动，返回 JoinHandle，便于生命周期管理注册
    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.start().await {
                error!("AgentInfoMonitor start error: {}", e);
            }
            self.shutdown_token.cancelled().await;
        })
    }

    async fn initial_load(&self) -> Result<()> {
        let store = self
            .jetstream
            .get_key_value(JS_KV_AGENT_INFOS)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to get agent infos store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        let mut keys = store.keys().await.map_err(|e| CoreError::Nats {
            message: format!("Failed to list agent infos keys: {}", e),
            severity: ErrorSeverity::Error,
        })?;

        while let Some(next) = keys.next().await {
            if let Ok(key) = next {
                if let Ok(Some(bytes)) = store.get(&key).await {
                    if let Ok(msg) = proto::AgentInfoMsg::decode(bytes.as_ref()) {
                        let info = AgentInfo::from(msg);
                        let agent_id = info.id.clone();
                        self.insert_info(agent_id.clone(), info);
                        debug!(
                            "agent_info_initial_loaded id={} id32={} labels={} system={} groups={}",
                            agent_id,
                            Self::to_bitmap_key(&agent_id),
                            self.index_labels.len(),
                            self.index_system.len(),
                            self.index_groups.len()
                        );
                    }
                }
            }
        }
        info!("AgentInfoMonitor initial load completed");
        Ok(())
    }

    async fn start_watcher(&self) -> Result<()> {
        let jetstream = self.jetstream.clone();
        let info_cache = self.info_cache.clone();
        let index_labels = self.index_labels.clone();
        let index_system = self.index_system.clone();
        let index_groups = self.index_groups.clone();
        let shutdown = self.shutdown_token.clone();

        tokio::spawn(async move {
            let store = match jetstream.get_key_value(JS_KV_AGENT_INFOS).await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to get agent infos store for watcher: {}", e);
                    return;
                }
            };

            match store.watch_all().await {
                Ok(mut watcher) => loop {
                    tokio::select! {
                        maybe = watcher.next() => {
                            match maybe {
                                Some(Ok(entry)) => {
                                    let key = entry.key;
                                    match entry.operation {
                                        async_nats::jetstream::kv::Operation::Put => {
                                            if let Ok(msg) = proto::AgentInfoMsg::decode(entry.value.as_ref()) {
                                                let info = AgentInfo::from(msg);
                                                let agent_id = info.id.clone();
                                                Self::insert_info_static(&info_cache, &index_labels, &index_system, &index_groups, agent_id.clone(), info);
                                                debug!("agent_info_watch_put id={} id32={} labels={} system={} groups={}",
                                                    agent_id,
                                                    Self::to_bitmap_key(&agent_id),
                                                    index_labels.len(),
                                                    index_system.len(),
                                                    index_groups.len());
                                            }
                                        }
                                        async_nats::jetstream::kv::Operation::Delete | async_nats::jetstream::kv::Operation::Purge => {
                                            let agent_id = AgentId::from(key.replace("agent.facts.", ""));
                                            Self::remove_info_static(&info_cache, &index_labels, &index_system, &index_groups, &agent_id);
                                            debug!("agent_info_watch_remove id={} id32={} labels={} system={} groups={}",
                                                agent_id,
                                                Self::to_bitmap_key(&agent_id),
                                                index_labels.len(),
                                                index_system.len(),
                                                index_groups.len());
                                        }
                                    }
                                }
                                Some(Err(e)) => warn!("AgentInfo watcher error: {}", e),
                                None => break,
                            }
                        }
                        _ = shutdown.cancelled() => {
                            info!("AgentInfoMonitor watcher stopped");
                            break;
                        }
                    }
                },
                Err(e) => error!("Failed to start agent infos watcher: {}", e),
            }
        });

        Ok(())
    }

    fn insert_info(&self, agent_id: AgentId, info: AgentInfo) {
        let _ = self.get_id32(&agent_id);
        Self::insert_info_static(
            &self.info_cache,
            &self.index_labels,
            &self.index_system,
            &self.index_groups,
            agent_id,
            info,
        );
    }

    fn insert_info_static(
        info_cache: &Arc<DashMap<AgentId, Arc<AgentInfo>>>,
        index_labels: &Arc<DashMap<(String, String), roaring::RoaringBitmap>>,
        index_system: &Arc<DashMap<(String, String), roaring::RoaringBitmap>>,
        index_groups: &Arc<DashMap<String, roaring::RoaringBitmap>>,
        agent_id: AgentId,
        info: AgentInfo,
    ) {
        let id32 = Self::to_bitmap_key(&agent_id);
        info_cache.insert(agent_id.clone(), Arc::new(info.clone()));

        // 解析 info map：__groups, __system_* 与普通 labels
        let mut groups: HashSet<String> = HashSet::new();
        for (k, v) in info.info.iter() {
            if k == "__groups" {
                // 逗号分隔或 json 数组均可在 core 层统一，这里假设逗号分隔
                for g in v.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    groups.insert(g.to_string());
                }
            } else if let Some(stripped) = k.strip_prefix("__system_") {
                let key = (stripped.to_string(), v.clone());
                index_system
                    .entry(key)
                    .or_insert_with(roaring::RoaringBitmap::new)
                    .insert(id32);
            } else {
                let key = (k.clone(), v.clone());
                index_labels
                    .entry(key)
                    .or_insert_with(roaring::RoaringBitmap::new)
                    .insert(id32);
            }
        }

        for g in groups {
            index_groups
                .entry(g)
                .or_insert_with(roaring::RoaringBitmap::new)
                .insert(id32);
        }
    }

    fn remove_info_static(
        info_cache: &Arc<DashMap<AgentId, Arc<AgentInfo>>>,
        index_labels: &Arc<DashMap<(String, String), roaring::RoaringBitmap>>,
        index_system: &Arc<DashMap<(String, String), roaring::RoaringBitmap>>,
        index_groups: &Arc<DashMap<String, roaring::RoaringBitmap>>,
        agent_id: &AgentId,
    ) {
        let id32 = Self::to_bitmap_key(agent_id);
        info_cache.remove(agent_id);

        // 朴素做法：全表扫描移除该 id32（规模中小可接受）；后续可维护反向映射提高删除效率
        for mut entry in index_labels.iter_mut() {
            entry.value_mut().remove(id32);
        }
        for mut entry in index_system.iter_mut() {
            entry.value_mut().remove(id32);
        }
        for mut entry in index_groups.iter_mut() {
            entry.value_mut().remove(id32);
        }
    }

    // 只读接口
    pub fn get_info(&self, agent_id: &AgentId) -> Option<Arc<AgentInfo>> {
        self.info_cache.get(agent_id).map(|v| v.clone())
    }

    pub fn snapshot_labels_index(&self) -> Arc<DashMap<(String, String), roaring::RoaringBitmap>> {
        self.index_labels.clone()
    }
    pub fn snapshot_system_index(&self) -> Arc<DashMap<(String, String), roaring::RoaringBitmap>> {
        self.index_system.clone()
    }
    pub fn snapshot_groups_index(&self) -> Arc<DashMap<String, roaring::RoaringBitmap>> {
        self.index_groups.clone()
    }

    fn to_bitmap_key(agent_id: &AgentId) -> u32 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        agent_id.hash(&mut hasher);
        (hasher.finish() & 0xFFFF_FFFF) as u32
    }

    pub fn get_id32(&self, agent_id: &AgentId) -> Option<u32> {
        Some(Self::to_bitmap_key(agent_id))
    }

    pub fn bitmap_to_agent_ids(&self, bm: &roaring::RoaringBitmap) -> Vec<AgentId> {
        let mut out = Vec::new();
        for entry in self.info_cache.iter() {
            let agent_id = entry.key();
            let id32 = Self::to_bitmap_key(agent_id);
            if bm.contains(id32) {
                out.push(agent_id.clone());
            }
        }
        out
    }
}
