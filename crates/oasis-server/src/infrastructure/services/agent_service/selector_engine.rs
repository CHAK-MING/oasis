use crate::infrastructure::monitor::agent_info_monitor::AgentInfoMonitor;
use crate::infrastructure::monitor::heartbeat_monitor::HeartbeatMonitor;
use dashmap::DashMap;
use oasis_core::agent_types::AgentInfo;
use oasis_core::core_types::AgentId;
use oasis_core::error::{CoreError, ErrorSeverity, Result as CoreResult};
use pest::Parser;
use roaring::RoaringBitmap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

#[derive(pest_derive::Parser)]
#[grammar_inline = r#"
WHITESPACE = _{ " " | "\t" | "\r" | "\n" }

selector = { SOI ~ (sys_eq | label_eq | in_groups | all_true | id_list) ~ EOI }

all_true = { ("all" | "true") }

id_list = { id ~ (comma ~ id)* }
comma = _{ "," }

id_char = _{ ASCII_ALPHANUMERIC | "-" | "_" | "." }
id = @{ id_char+ }

sys_eq = { "system" ~ "[" ~ string ~ "]" ~ "==" ~ string }
label_eq = { "labels" ~ "[" ~ string ~ "]" ~ "==" ~ string }
in_groups = { string ~ "in" ~ "groups" }

string = @{ dquote ~ (!dquote ~ ANY)* ~ dquote | squote ~ (!squote ~ ANY)* ~ squote }
dquote = _{ "\"" }
squote = _{ "'" }
"#]
struct SelectorParser;

/// 延迟求值的查询结果 - 核心优化：避免频繁的位图 <-> AgentId 转换
#[derive(Debug, Clone)]
pub struct QueryResult {
    bitmap: RoaringBitmap,
    agent_info_monitor: Arc<AgentInfoMonitor>,
    heartbeat_monitor: Arc<HeartbeatMonitor>,
}

impl QueryResult {
    /// 创建新的查询结果
    pub fn new(
        bitmap: RoaringBitmap,
        agent_info_monitor: Arc<AgentInfoMonitor>,
        heartbeat_monitor: Arc<HeartbeatMonitor>,
    ) -> Self {
        Self {
            bitmap,
            agent_info_monitor,
            heartbeat_monitor,
        }
    }

    /// 转换为所有匹配的 Agent - 延迟求值，只在需要时转换
    pub fn to_all_agents(&self) -> Vec<AgentId> {
        self.agent_info_monitor.bitmap_to_agent_ids(&self.bitmap)
    }

    /// 转换为在线的 Agent
    pub fn to_online_agents(&self) -> Vec<AgentId> {
        let mut all_agents = Vec::new();
        for id32 in self.bitmap.iter() {
            if let Some(agent_id) = self.agent_info_monitor.get_agent_id_by_id32(id32) {
                all_agents.push(agent_id);
            }
        }

        self.heartbeat_monitor.filter_online_agents(all_agents)
    }
}

#[derive(Debug, Clone)]
struct CachedQuery {
    bitmap: RoaringBitmap,
    cached_at: i64,
}

#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    pub max_entries: usize,
    pub ttl_seconds: u64,
    pub enable_cache: bool,
    pub cleanup_interval_seconds: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl_seconds: 300, // 5分钟缓存
            enable_cache: true,
            cleanup_interval_seconds: 60, // 1分钟清理一次
        }
    }
}

pub struct SelectorEngine {
    heartbeat: Arc<HeartbeatMonitor>,
    agent_info: Arc<AgentInfoMonitor>,
    // 查询缓存
    parse_cache: DashMap<String, CachedQuery>,
    cache_config: QueryCacheConfig,
    // "all" 查询的时间缓存 - 解决问题5
    all_agents_cache: std::sync::RwLock<Option<(RoaringBitmap, Instant)>>,
    // 添加 shutdown_token 支持
    shutdown_token: Option<CancellationToken>,
}

impl SelectorEngine {
    pub fn new(
        heartbeat: Arc<HeartbeatMonitor>,
        agent_info: Arc<AgentInfoMonitor>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            heartbeat,
            agent_info,
            parse_cache: DashMap::new(),
            cache_config: QueryCacheConfig::default(),
            all_agents_cache: std::sync::RwLock::new(None),
            shutdown_token: Some(shutdown_token),
        }
    }

    /// 自定义缓存配置
    pub fn with_cache_config(mut self, config: QueryCacheConfig) -> Self {
        self.cache_config = config;
        self
    }

    pub fn get_agent_info(&self, agent_id: &AgentId) -> Option<AgentInfo> {
        self.agent_info.get_info(agent_id).map(|arc| (*arc).clone())
    }

    /// 延迟求值查询 返回 QueryResult 而不是 Vec<AgentId>
    pub async fn query(&self, expression: &str) -> CoreResult<QueryResult> {
        let cache_key = format!("query|{}", expression);
        let bitmap = self
            .evaluate_to_bitmap_cached(&cache_key, expression)
            .await?;

        Ok(QueryResult::new(
            bitmap,
            self.agent_info.clone(),
            self.heartbeat.clone(),
        ))
    }

    /// 启动缓存清理任务 - 返回 JoinHandle 供生命周期管理
    pub fn spawn_cache_cleanup(self: Arc<Self>) -> Option<JoinHandle<()>> {
        if !self.cache_config.enable_cache {
            return None;
        }

        let shutdown_token = self.shutdown_token.clone()?;
        let cache = self.parse_cache.clone();
        let config = self.cache_config.clone();

        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                config.cleanup_interval_seconds,
            ));

            info!("SelectorEngine cache cleanup task started");

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::perform_cache_cleanup(&cache, &config).await;
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("SelectorEngine cache cleanup task stopped");
                        break;
                    }
                }
            }
        }))
    }

    /// 带缓存的位图评估
    async fn evaluate_to_bitmap_cached(
        &self,
        cache_key: &str,
        expression: &str,
    ) -> CoreResult<RoaringBitmap> {
        if self.cache_config.enable_cache {
            if let Some(cached_bitmap) = self.get_cached_result(cache_key).await? {
                debug!("Cache hit for key: {}", cache_key);
                return Ok(cached_bitmap);
            }
        }

        let bitmap = self.evaluate_to_bitmap(expression)?;

        if self.cache_config.enable_cache {
            if let Err(e) = self.cache_result(cache_key, &bitmap).await {
                warn!(
                    "Failed to cache query result for key '{}': {}",
                    cache_key, e
                );
            }
        }

        Ok(bitmap)
    }

    // 原有的 evaluate_to_bitmap 方法保持不变，但优化 union_all_agents
    fn evaluate_to_bitmap(&self, expression: &str) -> CoreResult<RoaringBitmap> {
        debug!("selector_evaluate expr={}", expression);
        let mut pairs = SelectorParser::parse(Rule::selector, expression).map_err(|e| {
            CoreError::InvalidTask {
                reason: format!("Invalid selector: {}", e),
                severity: ErrorSeverity::Error,
            }
        })?;

        let expr_pair = pairs.next().ok_or_else(|| CoreError::InvalidTask {
            reason: "Empty selector".to_string(),
            severity: ErrorSeverity::Error,
        })?;
        if pairs.next().is_some() {
            return Err(CoreError::InvalidTask {
                reason: "Ambiguous selector expression".to_string(),
                severity: ErrorSeverity::Error,
            });
        }

        let mut inner_iter = expr_pair.into_inner();
        let pair = inner_iter.next().ok_or_else(|| CoreError::InvalidTask {
            reason: "Invalid selector structure".to_string(),
            severity: ErrorSeverity::Error,
        })?;

        fn unquote(s: &str) -> String {
            s.trim_matches('"').trim_matches('\'').to_string()
        }

        debug!("selector_rule={:?}", pair.as_rule());
        match pair.as_rule() {
            Rule::all_true => Ok(self.union_all_agents()),
            Rule::id_list => {
                let mut ids: Vec<String> = Vec::new();
                for p in pair.into_inner() {
                    if p.as_rule() == Rule::id {
                        ids.push(p.as_str().to_string());
                    }
                }
                let bm = self.ids_to_bitmap(&ids);
                debug!("id_list size={}", bm.len());
                Ok(bm)
            }
            Rule::sys_eq => {
                let mut it = pair.into_inner();
                let sk = unquote(it.next().unwrap().as_str());
                let sv = unquote(it.next().unwrap().as_str());
                let bm = self.lookup_system(&sk, &sv);
                debug!("lookup_system key='{}' val='{}' size={}", sk, sv, bm.len());
                Ok(bm)
            }
            Rule::label_eq => {
                let mut it = pair.into_inner();
                let lk = unquote(it.next().unwrap().as_str());
                let lv = unquote(it.next().unwrap().as_str());
                let bm = self.lookup_label(&lk, &lv);

                let mut sample = 0;
                for entry in self.agent_info.snapshot_labels_index().iter() {
                    if sample > 3 {
                        break;
                    }
                    if entry.key().0 == lk {
                        debug!(
                            "indexed_label key='{}' val='{}' size={}",
                            lk,
                            entry.key().1,
                            entry.value().len()
                        );
                        sample += 1;
                    }
                }
                Ok(bm)
            }
            Rule::in_groups => {
                let mut it = pair.into_inner();
                let g = unquote(it.next().unwrap().as_str());
                let bm = self.lookup_group(&g);
                debug!("lookup_group name='{}' size={}", g, bm.len());
                Ok(bm)
            }
            _ => Err(CoreError::InvalidTask {
                reason: "Unsupported selector".into(),
                severity: ErrorSeverity::Error,
            }),
        }
    }

    fn union_all_agents(&self) -> RoaringBitmap {
        // 检查缓存（60秒过期）
        if let Ok(cache) = self.all_agents_cache.read() {
            if let Some((cached_bitmap, cached_time)) = cache.as_ref() {
                if cached_time.elapsed() < Duration::from_secs(60) {
                    debug!("All agents cache hit, size={}", cached_bitmap.len());
                    return cached_bitmap.clone();
                }
            }
        }

        // 重新计算全集
        let mut acc = RoaringBitmap::new();
        for entry in self.agent_info.snapshot_labels_index().iter() {
            acc |= entry.value().clone();
        }
        for entry in self.agent_info.snapshot_system_index().iter() {
            acc |= entry.value().clone();
        }
        for entry in self.agent_info.snapshot_groups_index().iter() {
            acc |= entry.value().clone();
        }

        // 更新缓存
        if let Ok(mut cache) = self.all_agents_cache.write() {
            *cache = Some((acc.clone(), Instant::now()));
            debug!("All agents cache updated, size={}", acc.len());
        }

        acc
    }

    fn ids_to_bitmap(&self, ids: &[String]) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        for id in ids {
            if let Some(id32) = self.agent_info.get_id32(&AgentId::from(id.clone())) {
                bm.insert(id32);
            }
        }
        bm
    }

    fn lookup_label(&self, key: &str, value: &str) -> RoaringBitmap {
        self.agent_info
            .snapshot_labels_index()
            .get(&(key.to_string(), value.to_string()))
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }

    fn lookup_system(&self, key: &str, value: &str) -> RoaringBitmap {
        self.agent_info
            .snapshot_system_index()
            .get(&(key.to_string(), value.to_string()))
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }

    fn lookup_group(&self, group: &str) -> RoaringBitmap {
        self.agent_info
            .snapshot_groups_index()
            .get(group)
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }

    async fn get_cached_result(&self, cache_key: &str) -> CoreResult<Option<RoaringBitmap>> {
        let now = chrono::Utc::now().timestamp();

        if let Some(cached) = self.parse_cache.get(cache_key) {
            if now - cached.cached_at < self.cache_config.ttl_seconds as i64 {
                return Ok(Some(cached.bitmap.clone()));
            } else {
                // 过期了，直接删除
                self.parse_cache.remove(cache_key);
                debug!("Cache expired for key: {}", cache_key);
            }
        }

        Ok(None)
    }

    async fn cache_result(&self, cache_key: &str, bitmap: &RoaringBitmap) -> CoreResult<()> {
        // 检查缓存大小限制
        if self.parse_cache.len() >= self.cache_config.max_entries {
            // 简单的 LRU：随机删除一些旧条目
            let keys_to_remove: Vec<String> = self
                .parse_cache
                .iter()
                .take(self.cache_config.max_entries / 10)
                .map(|entry| entry.key().clone())
                .collect();

            for key in keys_to_remove {
                self.parse_cache.remove(&key);
            }
        }

        let cached_query = CachedQuery {
            bitmap: bitmap.clone(),
            cached_at: chrono::Utc::now().timestamp(),
        };

        self.parse_cache.insert(cache_key.to_string(), cached_query);
        debug!("Cached result for key: {}", cache_key);
        Ok(())
    }

    async fn perform_cache_cleanup(
        cache: &DashMap<String, CachedQuery>,
        config: &QueryCacheConfig,
    ) {
        let now = chrono::Utc::now().timestamp();
        let mut cleaned_count = 0;

        cache.retain(|_, cached| {
            let is_valid = now - cached.cached_at < config.ttl_seconds as i64;
            if !is_valid {
                cleaned_count += 1;
            }
            is_valid
        });

        if cleaned_count > 0 {
            debug!("Cache cleanup: removed {} expired entries", cleaned_count);
        }
    }
}
