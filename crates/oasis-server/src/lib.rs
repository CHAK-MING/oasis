//! Oasis Server - 基于DDD架构的新一代集群管理服务器
//!
//! 这个版本基于现有的oasis-core，采用领域驱动设计(DDD)架构，
//! 提供更好的可扩展性、可维护性和性能。

pub mod domain;
pub mod application;
pub mod interface;
pub mod infrastructure;

// 重新导出核心类型
pub use oasis_core::*;

// 重新导出主要模块
