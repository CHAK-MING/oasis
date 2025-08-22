//! CEL 选择器 - 基于 CEL 表达式的节点选择器

use cel::{Context, Program, Value};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::error::{CoreError, Result};

/// 编译后的程序缓存
static PROGRAM_CACHE: Lazy<DashMap<String, Arc<Program>>> = Lazy::new(DashMap::new);
const CACHE_MAX_SIZE: usize = 512;

/// 缓存统计
#[derive(Debug, Default)]
struct CacheStats {
    hits: usize,
    misses: usize,
}

static CACHE_STATS: Lazy<std::sync::Mutex<CacheStats>> =
    Lazy::new(|| std::sync::Mutex::new(CacheStats::default()));

/// 获取缓存统计
pub fn cache_stats() -> (usize, usize, usize) {
    let stats = CACHE_STATS.lock().unwrap_or_else(|e| e.into_inner());
    (PROGRAM_CACHE.len(), stats.hits, stats.misses)
}

/// 节点属性
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAttributes {
    pub id: String,
    pub labels: HashMap<String, String>,
    pub groups: Vec<String>,
    pub version: String,
    pub custom: HashMap<String, String>,
}

impl NodeAttributes {
    pub fn new(id: String) -> Self {
        Self {
            id,
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
        variables.insert("id".to_string(), self.id.as_str().into());
        variables.insert("agent_id".to_string(), self.id.as_str().into());
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
        // 检查缓存
        if let Some(program) = PROGRAM_CACHE.get(&expression) {
            if let Ok(mut stats) = CACHE_STATS.lock() {
                stats.hits += 1;
            }
            return Ok(Self {
                expression,
                program: program.clone(),
            });
        }

        // 缓存未命中，编译新程序
        let program = Program::compile(&expression).map_err(|e| CoreError::Config {
            message: format!("Invalid CEL expression '{}': {:?}", expression, e),
        })?;

        let program_arc = Arc::new(program);

        // 缓存管理
        if PROGRAM_CACHE.len() >= CACHE_MAX_SIZE {
            if let Some(entry) = PROGRAM_CACHE.iter().next() {
                let key = entry.key().clone();
                drop(entry);
                PROGRAM_CACHE.remove(&key);
            }
        }

        PROGRAM_CACHE.insert(expression.clone(), program_arc.clone());

        if let Ok(mut stats) = CACHE_STATS.lock() {
            stats.misses += 1;
        }

        Ok(Self {
            expression,
            program: program_arc,
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
    pub fn filter_nodes(&self, nodes: &[NodeAttributes]) -> Result<Vec<String>> {
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
    fn resolve(&self, expression: &str, nodes: &[NodeAttributes]) -> Result<Vec<String>>;
}

/// 默认选择器引擎实现
pub struct DefaultSelectorEngine;

impl SelectorEngine for DefaultSelectorEngine {
    fn resolve(&self, expression: &str, nodes: &[NodeAttributes]) -> Result<Vec<String>> {
        let selector = CelSelector::new(expression.to_string())?;
        selector.filter_nodes(nodes)
    }
}

/// 便捷函数：解析选择器
pub fn resolve_selector(expression: &str, nodes: &[NodeAttributes]) -> Result<Vec<String>> {
    let engine = DefaultSelectorEngine;
    engine.resolve(expression, nodes)
}

/// 便捷函数：验证选择器语法
pub fn validate_selector(expression: &str) -> Result<()> {
    CelSelector::new(expression.to_string())?;
    Ok(())
}
