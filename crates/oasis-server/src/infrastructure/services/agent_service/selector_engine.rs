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
use tokio::sync::RwLock as AsyncRwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

#[derive(pest_derive::Parser)]
#[grammar_inline = r#"
WHITESPACE = _{ " " | "\t" | "\r" | "\n" }

// 主选择器表达式，支持逻辑运算
selector = { SOI ~ logical_or ~ EOI }

// 逻辑或（优先级最低）
logical_or = { logical_and ~ ("or" ~ logical_and)* }

// 逻辑与（中等优先级）
logical_and = { logical_not ~ ("and" ~ logical_not)* }

// 逻辑非（高优先级）
logical_not = { ("not" ~ primary) | primary }

// 基础表达式（最高优先级）
primary = { 
    ("(" ~ logical_or ~ ")") | 
    sys_eq | 
    label_eq | 
    in_groups | 
    all_true | 
    id_list 
}

// 原有的基础选择器
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

#[derive(Debug, Clone)]
enum SelectorAst {
    And(Box<SelectorAst>, Box<SelectorAst>),
    Or(Box<SelectorAst>, Box<SelectorAst>),
    Not(Box<SelectorAst>),
    AllTrue,
    IdList(Vec<String>),
    SystemEq(String, String),
    LabelEq(String, String),
    InGroups(String),
}

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
    // "all" 查询的时间缓存
    all_agents_cache: AsyncRwLock<Option<(RoaringBitmap, Instant)>>,
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
            all_agents_cache: AsyncRwLock::new(None),
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
                debug!(
                    "Cache hit for key: {} (len={})",
                    cache_key,
                    cached_bitmap.len()
                );
                return Ok(cached_bitmap);
            }
        }

        let t0 = std::time::Instant::now();
        let bitmap = self.evaluate_to_bitmap(expression)?;
        let elapsed = t0.elapsed();
        debug!(
            "selector_evaluate elapsed={}ms size={}",
            elapsed.as_millis(),
            bitmap.len()
        );

        // 不缓存空结果，避免在系统刚启动时把空查询结果缓存太久
        if self.cache_config.enable_cache && !bitmap.is_empty() {
            if let Err(e) = self.cache_result(cache_key, &bitmap).await {
                warn!(
                    "Failed to cache query result for key '{}': {}",
                    cache_key, e
                );
            } else {
                debug!(
                    "Cached key={} size={} ttl={}s",
                    cache_key,
                    bitmap.len(),
                    self.cache_config.ttl_seconds
                );
            }
        }

        Ok(bitmap)
    }

    fn evaluate_to_bitmap(&self, expression: &str) -> CoreResult<RoaringBitmap> {
        debug!("selector_evaluate expr={}", expression);

        // 解析表达式为 AST
        let ast = self.parse_to_ast(expression)?;

        // 递归评估 AST
        self.evaluate_ast(&ast)
    }

    // 将表达式解析为 AST
    fn parse_to_ast(&self, expression: &str) -> CoreResult<SelectorAst> {
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

        // 开始解析逻辑或表达式
        let mut inner = expr_pair.into_inner();
        let logical_or_pair = inner.next().ok_or_else(|| CoreError::InvalidTask {
            reason: "Invalid selector structure".to_string(),
            severity: ErrorSeverity::Error,
        })?;

        self.parse_logical_or(logical_or_pair)
    }

    // 解析逻辑或表达式
    fn parse_logical_or(&self, pair: pest::iterators::Pair<Rule>) -> CoreResult<SelectorAst> {
        let mut inner = pair.into_inner();
        let first = self.parse_logical_and(inner.next().unwrap())?;

        let mut result = first;
        while let Some(and_pair) = inner.next() {
            let right = self.parse_logical_and(and_pair)?;
            result = SelectorAst::Or(Box::new(result), Box::new(right));
        }

        Ok(result)
    }

    // 解析逻辑与表达式
    fn parse_logical_and(&self, pair: pest::iterators::Pair<Rule>) -> CoreResult<SelectorAst> {
        let mut inner = pair.into_inner();
        let first = self.parse_logical_not(inner.next().unwrap())?;

        let mut result = first;
        while let Some(not_pair) = inner.next() {
            let right = self.parse_logical_not(not_pair)?;
            result = SelectorAst::And(Box::new(result), Box::new(right));
        }

        Ok(result)
    }

    // 解析逻辑非表达式
    fn parse_logical_not(&self, pair: pest::iterators::Pair<Rule>) -> CoreResult<SelectorAst> {
        let pair_str = pair.as_str().trim();

        // 检查是否以 "not" 开头
        if pair_str.starts_with("not ") {
            // 这是一个 NOT 表达式，需要提取内部的 primary
            let mut inner = pair.into_inner();
            let primary_pair = inner.next().unwrap(); // 这应该是 primary
            let inner_ast = self.parse_primary(primary_pair)?;
            Ok(SelectorAst::Not(Box::new(inner_ast)))
        } else {
            // 这是一个普通的 primary 表达式
            let mut inner = pair.into_inner();
            let primary_pair = inner.next().unwrap();
            self.parse_primary(primary_pair)
        }
    }

    // 解析基础表达式
    fn parse_primary(&self, pair: pest::iterators::Pair<Rule>) -> CoreResult<SelectorAst> {
        let mut inner = pair.into_inner();
        let first_pair = inner.next().unwrap();

        match first_pair.as_rule() {
            Rule::logical_or => {
                // 这是括号内的表达式
                self.parse_logical_or(first_pair)
            }
            Rule::all_true => Ok(SelectorAst::AllTrue),
            Rule::id_list => {
                let mut ids = Vec::new();
                for p in first_pair.into_inner() {
                    if p.as_rule() == Rule::id {
                        ids.push(p.as_str().to_string());
                    }
                }
                Ok(SelectorAst::IdList(ids))
            }
            Rule::sys_eq => {
                let mut it = first_pair.into_inner();
                let sk = unquote(it.next().unwrap().as_str());
                let sv = unquote(it.next().unwrap().as_str());
                Ok(SelectorAst::SystemEq(sk, sv))
            }
            Rule::label_eq => {
                let mut it = first_pair.into_inner();
                let lk = unquote(it.next().unwrap().as_str());
                let lv = unquote(it.next().unwrap().as_str());
                Ok(SelectorAst::LabelEq(lk, lv))
            }
            Rule::in_groups => {
                let mut it = first_pair.into_inner();
                let group_name = unquote(it.next().unwrap().as_str());
                Ok(SelectorAst::InGroups(group_name))
            }
            _ => Err(CoreError::InvalidTask {
                reason: format!("Unsupported selector rule: {:?}", first_pair.as_rule()),
                severity: ErrorSeverity::Error,
            }),
        }
    }

    // 递归评估 AST 节点
    fn evaluate_ast(&self, ast: &SelectorAst) -> CoreResult<RoaringBitmap> {
        match ast {
            SelectorAst::And(left, right) => {
                let left_bitmap = self.evaluate_ast(left)?;
                let right_bitmap = self.evaluate_ast(right)?;
                Ok(left_bitmap & right_bitmap)
            }
            SelectorAst::Or(left, right) => {
                let left_bitmap = self.evaluate_ast(left)?;
                let right_bitmap = self.evaluate_ast(right)?;
                Ok(left_bitmap | right_bitmap)
            }
            SelectorAst::Not(inner) => {
                let inner_bitmap = self.evaluate_ast(inner)?;
                let all_bitmap = self.union_all_agents();
                Ok(all_bitmap - inner_bitmap)
            }
            SelectorAst::AllTrue => Ok(self.union_all_agents()),
            SelectorAst::IdList(ids) => Ok(self.ids_to_bitmap(ids)),
            SelectorAst::SystemEq(key, value) => Ok(self.lookup_system(key, value)),
            SelectorAst::LabelEq(key, value) => Ok(self.lookup_label(key, value)),
            SelectorAst::InGroups(group) => Ok(self.lookup_group(group)),
        }
    }

    fn union_all_agents(&self) -> RoaringBitmap {
        // 检查缓存（60秒过期）
        if let Ok(cache) = self.all_agents_cache.try_read() {
            if let Some((cached_bitmap, cached_time)) = cache.as_ref() {
                if cached_time.elapsed() < Duration::from_secs(60) {
                    debug!("All agents cache hit, size={}", cached_bitmap.len());
                    return cached_bitmap.clone();
                }
            }
        }

        // 使用 AgentInfoMonitor 的新方法获取所有 agent
        let acc = self.agent_info.get_all_agents_bitmap();

        // 更新缓存
        if let Ok(mut cache) = self.all_agents_cache.try_write() {
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
                self.parse_cache.remove(cache_key);
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

    pub async fn update_agent_info(&self, agent_id: AgentId, info: AgentInfo) {
        self.agent_info.update_agent_info(agent_id, info).await;
    }

    pub fn remove_agent(&self, agent_id: &AgentId) {
        // 计算该 Agent 的位图键
        let Some(id32) = self.agent_info.get_id32(agent_id) else {
            tracing::debug!(agent_id = %agent_id, "remove_agent: id32 not found (maybe already removed)");
            return;
        };

        // 1) 精确更新已缓存的查询结果：从每个缓存位图中移除该 id32
        let mut affected = 0usize;
        for mut entry in self.parse_cache.iter_mut() {
            let bm = &mut entry.value_mut().bitmap;
            if bm.contains(id32) {
                bm.remove(id32);
                affected += 1;
            }
        }

        // 2) 更新 "all" 的缓存位图：移除该 id32
        if let Ok(mut cache) = self.all_agents_cache.try_write() {
            if let Some((ref mut bm, _ts)) = *cache {
                bm.remove(id32);
            }
        }

        tracing::debug!(agent_id = %agent_id, affected_cache_entries = affected, "SelectorEngine: removed agent from cached bitmaps");
    }
}

fn unquote(s: &str) -> String {
    s.trim_matches('"').trim_matches('\'').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unquote_tests {
        use super::*;

        #[test]
        fn test_double_quoted() {
            assert_eq!(unquote("\"hello\""), "hello");
        }

        #[test]
        fn test_single_quoted() {
            assert_eq!(unquote("'hello'"), "hello");
        }

        #[test]
        fn test_no_quotes() {
            assert_eq!(unquote("hello"), "hello");
        }

        #[test]
        fn test_empty_quoted() {
            assert_eq!(unquote("\"\""), "");
            assert_eq!(unquote("''"), "");
        }

        #[test]
        fn test_inner_quotes_preserved() {
            assert_eq!(unquote("\"hello world\""), "hello world");
        }
    }

    mod query_cache_config_tests {
        use super::*;

        #[test]
        fn test_default() {
            let config = QueryCacheConfig::default();
            assert_eq!(config.max_entries, 1000);
            assert_eq!(config.ttl_seconds, 300);
            assert!(config.enable_cache);
            assert_eq!(config.cleanup_interval_seconds, 60);
        }
    }

    mod selector_parser_tests {
        use super::*;

        #[test]
        fn test_parse_all() {
            let result = SelectorParser::parse(Rule::selector, "all");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_true() {
            let result = SelectorParser::parse(Rule::selector, "true");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_single_id() {
            let result = SelectorParser::parse(Rule::selector, "agent-001");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_id_list() {
            let result = SelectorParser::parse(Rule::selector, "agent-001, agent-002, agent-003");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_label_eq() {
            let result = SelectorParser::parse(Rule::selector, "labels[\"env\"]==\"production\"");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_label_eq_single_quotes() {
            let result = SelectorParser::parse(Rule::selector, "labels['region']=='us-east-1'");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_system_eq() {
            let result = SelectorParser::parse(Rule::selector, "system[\"os\"]==\"linux\"");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_in_groups() {
            let result = SelectorParser::parse(Rule::selector, "\"web-servers\" in groups");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_and_expression() {
            let result = SelectorParser::parse(
                Rule::selector,
                "labels[\"env\"]==\"prod\" and labels[\"tier\"]==\"web\"",
            );
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_or_expression() {
            let result = SelectorParser::parse(
                Rule::selector,
                "labels[\"env\"]==\"prod\" or labels[\"env\"]==\"staging\"",
            );
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_not_expression() {
            let result = SelectorParser::parse(Rule::selector, "not labels[\"env\"]==\"prod\"");
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_complex_expression() {
            let result = SelectorParser::parse(
                Rule::selector,
                "(labels[\"env\"]==\"prod\" and labels[\"tier\"]==\"web\") or labels[\"critical\"]==\"true\"",
            );
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_parentheses() {
            let result = SelectorParser::parse(
                Rule::selector,
                "(labels[\"a\"]==\"1\" or labels[\"b\"]==\"2\") and labels[\"c\"]==\"3\"",
            );
            assert!(result.is_ok());
        }

        #[test]
        fn test_parse_invalid_syntax_fails() {
            let result = SelectorParser::parse(Rule::selector, "labels[env]=");
            assert!(result.is_err());
        }

        #[test]
        fn test_parse_empty_fails() {
            let result = SelectorParser::parse(Rule::selector, "");
            assert!(result.is_err());
        }
    }

    mod selector_ast_tests {
        use super::*;

        #[test]
        fn test_ast_all_true() {
            let ast = SelectorAst::AllTrue;
            assert!(matches!(ast, SelectorAst::AllTrue));
        }

        #[test]
        fn test_ast_id_list() {
            let ast = SelectorAst::IdList(vec!["agent-1".to_string(), "agent-2".to_string()]);
            if let SelectorAst::IdList(ids) = ast {
                assert_eq!(ids.len(), 2);
                assert_eq!(ids[0], "agent-1");
            } else {
                panic!("Expected IdList");
            }
        }

        #[test]
        fn test_ast_label_eq() {
            let ast = SelectorAst::LabelEq("env".to_string(), "prod".to_string());
            if let SelectorAst::LabelEq(key, value) = ast {
                assert_eq!(key, "env");
                assert_eq!(value, "prod");
            } else {
                panic!("Expected LabelEq");
            }
        }

        #[test]
        fn test_ast_system_eq() {
            let ast = SelectorAst::SystemEq("os".to_string(), "linux".to_string());
            if let SelectorAst::SystemEq(key, value) = ast {
                assert_eq!(key, "os");
                assert_eq!(value, "linux");
            } else {
                panic!("Expected SystemEq");
            }
        }

        #[test]
        fn test_ast_in_groups() {
            let ast = SelectorAst::InGroups("web-servers".to_string());
            if let SelectorAst::InGroups(group) = ast {
                assert_eq!(group, "web-servers");
            } else {
                panic!("Expected InGroups");
            }
        }

        #[test]
        fn test_ast_and() {
            let left = SelectorAst::AllTrue;
            let right = SelectorAst::LabelEq("env".to_string(), "prod".to_string());
            let ast = SelectorAst::And(Box::new(left), Box::new(right));
            assert!(matches!(ast, SelectorAst::And(_, _)));
        }

        #[test]
        fn test_ast_or() {
            let left = SelectorAst::AllTrue;
            let right = SelectorAst::AllTrue;
            let ast = SelectorAst::Or(Box::new(left), Box::new(right));
            assert!(matches!(ast, SelectorAst::Or(_, _)));
        }

        #[test]
        fn test_ast_not() {
            let inner = SelectorAst::AllTrue;
            let ast = SelectorAst::Not(Box::new(inner));
            assert!(matches!(ast, SelectorAst::Not(_)));
        }

        #[test]
        fn test_ast_clone() {
            let ast = SelectorAst::LabelEq("env".to_string(), "prod".to_string());
            let cloned = ast.clone();
            if let SelectorAst::LabelEq(key, value) = cloned {
                assert_eq!(key, "env");
                assert_eq!(value, "prod");
            } else {
                panic!("Clone failed");
            }
        }
    }
}
