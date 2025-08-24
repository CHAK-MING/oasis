//! 统一配置管理模块
//!
//! 提供跨组件的统一配置加载、验证和热更新功能。

pub mod common;

// 重导出主要类型
pub use common::{CommonConfig, NatsConfig, TelemetryConfig};
