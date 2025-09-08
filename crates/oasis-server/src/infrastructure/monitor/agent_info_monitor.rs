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
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 基于 KV 的 AgentInfo 监控器 - 采用延迟清理优化性能
/// - 维护 AgentInfo 快照
/// - 维护倒排索引：labels、system、groups
/// - 延迟清理已删除的 agent，避免删除时的全表扫描
#[derive(Debug, Clone)]
pub struct AgentInfoMonitor {
    jetstream: Arc<Context>,
    info_cache: Arc<DashMap<AgentId, Arc<AgentInfo>>>,
    index_labels: Arc<DashMap<(String, String), roaring::RoaringBitmap>>, // (k,v) -> bitmap
    index_system: Arc<DashMap<(String, String), roaring::RoaringBitmap>>, // (sys_k, v) -> bitmap
    index_groups: Arc<DashMap<String, roaring::RoaringBitmap>>,           // group -> bitmap

    // ID 反向映射
    id32_to_agent_id: Arc<DashMap<u32, AgentId>>,

    // 延迟清理机制 - 标记待清理的 agent ID，避免删除时全表扫描
    dirty_agents: Arc<RwLock<roaring::RoaringBitmap>>,

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
            id32_to_agent_id: Arc::new(DashMap::new()),
            dirty_agents: Arc::new(RwLock::new(roaring::RoaringBitmap::new())),
            shutdown_token,
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting AgentInfoMonitor");
        self.initial_load().await?;
        self.start_watcher().await?;
        Ok(())
    }

    /// 以子任务方式启动，返回 JoinHandle，便于生命周期管理注册
    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        let monitor = self.clone();
        tokio::spawn(async move {
            let cleanup_handle = monitor.start_cache_cleanup();

            let monitor_handle = async {
                if let Err(e) = monitor.start().await {
                    error!("AgentInfoMonitor start error: {}", e);
                }
            };

            tokio::select! {
                _ = cleanup_handle => {},
                _ = monitor_handle => {},
                _ = monitor.shutdown_token.cancelled() => {
                    info!("AgentInfoMonitor shutdown requested");
                }
            }
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

        while let Some(key) = keys.next().await {
            match key {
                Ok(k) => {
                    let agent_id = AgentId::from(k.clone());
                    if let Ok(Some(bytes)) = store.get(&k).await {
                        match proto::AgentInfoMsg::decode(bytes.as_ref()) {
                            Ok(proto_info) => {
                                let info = AgentInfo::from(proto_info);
                                self.insert_info(agent_id, info).await;
                            }
                            Err(e) => warn!("Failed to decode AgentInfo for {}: {}", agent_id, e),
                        }
                    }
                }
                Err(e) => warn!("Failed to get key from stream: {}", e),
            }
        }

        info!("Loaded {} agent infos", self.info_cache.len());
        Ok(())
    }

    async fn start_watcher(&self) -> Result<()> {
        let store = self
            .jetstream
            .get_key_value(JS_KV_AGENT_INFOS)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to get agent infos store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        let shutdown = self.shutdown_token.clone();
        let info_cache = self.info_cache.clone();
        let index_labels = self.index_labels.clone();
        let index_system = self.index_system.clone();
        let index_groups = self.index_groups.clone();
        let id32_to_agent_id = self.id32_to_agent_id.clone();
        let dirty_agents = self.dirty_agents.clone();

        tokio::spawn(async move {
            match store.watch_all().await {
                Ok(mut watcher) => loop {
                    tokio::select! {
                        msg = watcher.next() => {
                            match msg {
                                Some(Ok(entry)) => {
                                    let key = entry.key;
                                    let agent_id = AgentId::from(key.clone());
                                    match entry.operation {
                                        async_nats::jetstream::kv::Operation::Put => {

                                            match proto::AgentInfoMsg::decode(entry.value.as_ref()) {
                                                Ok(proto_info) => {
                                                    let info = AgentInfo::from(proto_info);
                                                    Self::insert_info_static(
                                                        &info_cache,
                                                        &index_labels,
                                                        &index_system,
                                                        &index_groups,
                                                        &id32_to_agent_id,
                                                        &dirty_agents,
                                                        agent_id,
                                                        info,
                                                    ).await;
                                                }
                                                Err(e) => {
                                                    warn!("Failed to decode AgentInfoMsg for {}: {}", agent_id, e);
                                                }
                                            }
                                        }
                                        async_nats::jetstream::kv::Operation::Delete => {
                                            Self::remove_info_static(
                                                &info_cache,
                                                &id32_to_agent_id,
                                                &dirty_agents,
                                                &agent_id,
                                            ).await;
                                        }
                                        _ => {} // Ignore other operations
                                    }
                                }
                                Some(Err(e)) => error!("AgentInfo watcher error: {}", e),
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

    async fn insert_info(&self, agent_id: AgentId, info: AgentInfo) {
        Self::insert_info_static(
            &self.info_cache,
            &self.index_labels,
            &self.index_system,
            &self.index_groups,
            &self.id32_to_agent_id,
            &self.dirty_agents,
            agent_id,
            info,
        )
        .await;
    }

    async fn insert_info_static(
        info_cache: &Arc<DashMap<AgentId, Arc<AgentInfo>>>,
        index_labels: &Arc<DashMap<(String, String), roaring::RoaringBitmap>>,
        index_system: &Arc<DashMap<(String, String), roaring::RoaringBitmap>>,
        index_groups: &Arc<DashMap<String, roaring::RoaringBitmap>>,
        id32_to_agent_id: &Arc<DashMap<u32, AgentId>>,
        dirty_agents: &Arc<RwLock<roaring::RoaringBitmap>>,
        agent_id: AgentId,
        info: AgentInfo,
    ) {
        let id32 = Self::to_bitmap_key(&agent_id);

        // 如果是更新操作，先将旧数据标记为脏（延迟清理）
        if info_cache.contains_key(&agent_id) {
            let mut dirty = dirty_agents.write().await;
            dirty.insert(id32);
        }

        // 更新缓存和反向映射
        info_cache.insert(agent_id.clone(), Arc::new(info.clone()));
        id32_to_agent_id.insert(id32, agent_id.clone());

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

    /// 延迟删除：不立即清理索引，只标记为脏，后台批量清理
    async fn remove_info_static(
        info_cache: &Arc<DashMap<AgentId, Arc<AgentInfo>>>,
        id32_to_agent_id: &Arc<DashMap<u32, AgentId>>,
        dirty_agents: &Arc<RwLock<roaring::RoaringBitmap>>,
        agent_id: &AgentId,
    ) {
        let id32 = Self::to_bitmap_key(agent_id);

        // 从缓存中移除
        info_cache.remove(agent_id);
        id32_to_agent_id.remove(&id32);

        // 标记为脏，等待后台清理 - O(1) 操作！
        let mut dirty = dirty_agents.write().await;
        dirty.insert(id32);

        debug!("Agent {} marked for lazy cleanup", agent_id);
    }

    /// 后台批量清理脏数据 - 核心优化：批量处理而不是逐个处理
    async fn batch_cleanup_dirty_agents(&self) {
        let dirty_snapshot = {
            let mut dirty = self.dirty_agents.write().await;
            if dirty.is_empty() {
                return;
            }
            let snapshot = dirty.clone();
            dirty.clear();
            snapshot
        };

        if dirty_snapshot.is_empty() {
            return;
        }

        let cleanup_count = dirty_snapshot.len();
        debug!("Starting batch cleanup of {} dirty agents", cleanup_count);

        // 批量清理各个索引 - 使用 RoaringBitmap 的高效减法操作
        for mut entry in self.index_labels.iter_mut() {
            *entry.value_mut() -= &dirty_snapshot;
        }
        for mut entry in self.index_system.iter_mut() {
            *entry.value_mut() -= &dirty_snapshot;
        }
        for mut entry in self.index_groups.iter_mut() {
            *entry.value_mut() -= &dirty_snapshot;
        }

        info!(
            "Batch cleanup completed: cleaned {} dirty agents from all indexes",
            cleanup_count
        );
    }

    // 只读接口
    pub fn get_info(&self, agent_id: &AgentId) -> Option<Arc<AgentInfo>> {
        self.info_cache.get(agent_id).map(|v| v.clone())
    }

    pub async fn update_agent_info(&self, agent_id: AgentId, info: AgentInfo) {
        self.insert_info(agent_id, info).await;
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

    pub fn get_agent_id_by_id32(&self, id32: u32) -> Option<AgentId> {
        self.id32_to_agent_id.get(&id32).map(|entry| entry.clone())
    }

    /// 优化后的 bitmap 转换函数：O(M) 而不是 O(N)
    pub fn bitmap_to_agent_ids(&self, bm: &roaring::RoaringBitmap) -> Vec<AgentId> {
        bm.iter()
            .filter_map(|id32| self.id32_to_agent_id.get(&id32).map(|entry| entry.clone()))
            .collect()
    }

    /// 启动定期缓存清理任务 - 包括空索引清理和脏数据清理
    async fn start_cache_cleanup(&self) {
        let shutdown_token = self.shutdown_token.clone();
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(3600)); // 每小时清理空索引
        let mut dirty_cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(30)); // 每30秒清理脏数据

        loop {
            tokio::select! {
                _ = cleanup_interval.tick() => {
                    // 清理空的索引条目
                    self.cleanup_empty_indexes();
                }
                _ = dirty_cleanup_interval.tick() => {
                    // 批量清理脏数据
                    self.batch_cleanup_dirty_agents().await;
                }
                _ = shutdown_token.cancelled() => {
                    // 关闭前执行最后一次清理
                    self.batch_cleanup_dirty_agents().await;
                    info!("AgentInfoMonitor cache cleanup task stopped");
                    break;
                }
            }
        }
    }

    /// 清理空的索引条目
    fn cleanup_empty_indexes(&self) {
        let mut cleaned_labels = 0;
        let mut cleaned_system = 0;
        let mut cleaned_groups = 0;

        // 清理空的 labels 索引
        self.index_labels.retain(|_, bitmap| {
            if bitmap.is_empty() {
                cleaned_labels += 1;
                false
            } else {
                true
            }
        });

        // 清理空的 system 索引
        self.index_system.retain(|_, bitmap| {
            if bitmap.is_empty() {
                cleaned_system += 1;
                false
            } else {
                true
            }
        });

        // 清理空的 groups 索引
        self.index_groups.retain(|_, bitmap| {
            if bitmap.is_empty() {
                cleaned_groups += 1;
                false
            } else {
                true
            }
        });

        if cleaned_labels > 0 || cleaned_system > 0 || cleaned_groups > 0 {
            debug!(
                "AgentInfoMonitor empty index cleanup: removed {} empty label entries, {} empty system entries, {} empty group entries",
                cleaned_labels, cleaned_system, cleaned_groups
            );
        }
    }

    pub fn get_all_agents_bitmap(&self) -> roaring::RoaringBitmap {
        let mut bitmap = roaring::RoaringBitmap::new();
        
        // 遍历所有在缓存中的 agent
        for entry in self.info_cache.iter() {
            let agent_id = entry.key();
            let id32 = Self::to_bitmap_key(agent_id);
            bitmap.insert(id32);
        }
        
        bitmap
    }
}
