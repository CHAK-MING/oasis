//! CEL 选择器 - 基于 CEL 表达式的节点选择器

use cel::{Context, Program, Value};
use moka::sync::Cache;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::debug;

use crate::error::{CoreError, Result};
use crate::type_defs::AgentId;

/// 缓存统计信息
#[derive(Debug, Default)]
pub struct CacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl CacheStats {
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::AcqRel);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::AcqRel);
    }

    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::AcqRel);
    }

    pub fn get_stats(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Acquire),
            self.misses.load(Ordering::Acquire),
            self.evictions.load(Ordering::Acquire),
        )
    }
}

/// 缓存统计实例
static CACHE_STATS: Lazy<CacheStats> = Lazy::new(CacheStats::default);

/// 编译后的程序缓存
static PROGRAM_CACHE: Lazy<Cache<String, Arc<Program>>> = Lazy::new(|| {
    // 创建淘汰监听器来记录淘汰事件
    let listener = |_k: Arc<String>, _v: Arc<Program>, cause| {
        use moka::notification::RemovalCause;

        // 记录因容量限制导致的淘汰
        if cause == RemovalCause::Size {
            CACHE_STATS.record_eviction();
        }
    };

    Cache::builder()
        .max_capacity(512)
        .time_to_live(Duration::from_secs(3600)) // 1小时过期
        .time_to_idle(Duration::from_secs(1800)) // 30分钟空闲过期
        .eviction_listener(listener)
        .build()
});

/// 编译 CEL 程序（带缓存和统计）
fn compile_cel_program(expression: &str) -> Result<Arc<Program>> {
    // 使用 entry API 来准确记录命中/未命中
    let entry = PROGRAM_CACHE
        .entry(expression.to_string())
        .or_try_insert_with(|| {
            Program::compile(expression)
                .map_err(|e| CoreError::Config {
                    message: format!("Invalid CEL expression '{}': {:?}", expression, e),
                })
                .map(Arc::new)
        })
        .map_err(|e| (*e).clone())?;

    if entry.is_fresh() {
        // 条目刚刚插入到缓存中
        CACHE_STATS.record_miss();
    } else {
        // 条目已经在缓存中
        CACHE_STATS.record_hit();
    }

    Ok(entry.into_value())
}

/// 获取缓存统计
/// 返回 (条目数量, 命中次数, 未命中次数)
pub fn cache_stats() -> (usize, usize, usize) {
    let cache = &*PROGRAM_CACHE;
    let (hits, misses, _evictions) = CACHE_STATS.get_stats();
    (cache.entry_count() as usize, hits as usize, misses as usize)
}

/// 节点属性
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAttributes {
    pub id: AgentId,
    pub labels: HashMap<String, String>,
    pub groups: Vec<String>,
    pub version: String,
    pub custom: HashMap<String, String>,
}

impl NodeAttributes {
    pub fn new(id: impl Into<AgentId>) -> Self {
        Self {
            id: id.into(),
            labels: HashMap::new(),
            groups: Vec::new(),
            version: "1.0.0".to_string(),
            custom: HashMap::new(),
        }
    }

    /// 转换为 CEL 变量映射
    fn to_cel_variables(&self) -> HashMap<String, Value> {
        let mut variables = HashMap::new();

        // 基本属性
        variables.insert("id".to_string(), self.id.as_ref().into());
        variables.insert("agent_id".to_string(), self.id.as_ref().into());
        variables.insert("version".to_string(), self.version.as_str().into());

        // 标签映射
        let labels_map: HashMap<String, Value> = self
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().into()))
            .collect();
        variables.insert("labels".to_string(), labels_map.into());

        // 组列表
        let groups_list: Vec<Value> = self.groups.iter().map(|g| g.as_str().into()).collect();
        variables.insert("groups".to_string(), groups_list.into());

        variables
    }
}

/// CEL 选择器
#[derive(Debug, Clone)]
pub struct CelSelector {
    expression: String,
    program: Arc<Program>,
}

impl CelSelector {
    pub fn new(expression: String) -> Result<Self> {
        // 使用缓存的编译函数
        let program = compile_cel_program(&expression)?;

        Ok(Self {
            expression,
            program,
        })
    }

    /// 评估单个节点
    pub fn matches(&self, node: &NodeAttributes) -> Result<bool> {
        let variables = node.to_cel_variables();
        let mut context = Context::default();

        // 添加变量到上下文
        for (key, value) in variables {
            context
                .add_variable(key, value)
                .map_err(|e| CoreError::Internal {
                    message: format!("Failed to add variable to CEL context: {:?}", e),
                })?;
        }

        let result = self.program.execute(&context).map_err(|e| {
            debug!(
                expression = %self.expression,
                node_id = %node.id,
                error = ?e,
                "CEL execution failed"
            );
            CoreError::Internal {
                message: format!("CEL execution failed: {:?}", e),
            }
        })?;

        match result {
            Value::Bool(b) => {
                debug!(
                    expression = %self.expression,
                    node_id = %node.id,
                    result = b,
                    "CEL evaluation completed"
                );
                Ok(b)
            }
            _ => Err(CoreError::Config {
                message: format!(
                    "CEL expression '{}' must return boolean, got: {:?}",
                    self.expression, result
                ),
            }),
        }
    }

    /// 批量评估
    pub fn filter_nodes(&self, nodes: &[NodeAttributes]) -> Result<Vec<AgentId>> {
        let mut matched = Vec::new();

        for node in nodes {
            if self.matches(node)? {
                matched.push(node.id.clone());
            }
        }

        Ok(matched)
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }
}

/// 便捷的选择器构建器
#[derive(Debug, Default)]
pub struct SelectorBuilder {
    conditions: Vec<String>,
}

impl SelectorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// 添加 AND 条件
    pub fn and(mut self, condition: impl Into<String>) -> Self {
        self.conditions.push(condition.into());
        self
    }

    /// 环境匹配（映射到 labels['environment']）
    pub fn environment(self, env: impl Into<String>) -> Self {
        self.label("environment", env)
    }

    /// 标签匹配
    pub fn label(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.and(format!("labels['{}'] == '{}'", key.into(), value.into()))
    }

    /// 组匹配
    pub fn in_group(self, group: impl Into<String>) -> Self {
        self.and(format!("'{}' in groups", group.into()))
    }

    /// 构建选择器
    pub fn build(self) -> Result<CelSelector> {
        if self.conditions.is_empty() {
            return Err(CoreError::Config {
                message: "Empty selector expression".to_string(),
            });
        }

        let expression = self.conditions.join(" && ");
        CelSelector::new(expression)
    }
}

/// 选择器引擎 trait
pub trait SelectorEngine: Send + Sync {
    fn resolve(&self, expression: &str, nodes: &[NodeAttributes]) -> Result<Vec<AgentId>>;
}

/// 默认选择器引擎实现
pub struct DefaultSelectorEngine;

impl SelectorEngine for DefaultSelectorEngine {
    fn resolve(&self, expression: &str, nodes: &[NodeAttributes]) -> Result<Vec<AgentId>> {
        let selector = CelSelector::new(expression.to_string())?;
        selector.filter_nodes(nodes)
    }
}

/// 便捷函数：解析选择器
pub fn resolve_selector(expression: &str, nodes: &[NodeAttributes]) -> Result<Vec<AgentId>> {
    let engine = DefaultSelectorEngine;
    engine.resolve(expression, nodes)
}

/// 便捷函数：验证选择器语法
pub fn validate_selector(expression: &str) -> Result<()> {
    CelSelector::new(expression.to_string())?;
    Ok(())
}
