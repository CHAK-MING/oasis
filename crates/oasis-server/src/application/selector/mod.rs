//! 选择器系统 - 基于倒排索引与 Roaring 位图
//!
//! 支持千级到万级 Agent 的毫秒级筛选

pub mod engine;
pub mod index;
pub mod parser;
pub mod planner;

pub use engine::SelectorEngine;
pub use index::InvertedIndex;

// use anyhow::Result;
use oasis_core::types::AgentId;

/// 选择器查询结果
#[derive(Debug, Clone)]
pub struct SelectorResult {
    pub agent_ids: Vec<AgentId>,
    pub execution_time_ms: u64,
    pub fallback_evaluations: usize,
}

/// 选择器统计信息
#[derive(Debug, Clone)]
pub struct SelectorStats {
    pub total_queries: u64,
    pub avg_execution_time_ms: f64,
    pub cache_hit_rate: f64,
    pub bitmap_operations: u64,
    pub fallback_evaluations: u64,
}
