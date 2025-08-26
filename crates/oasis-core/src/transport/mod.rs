//! 传输层模块
//!
//! 提供统一的网络传输功能，包括 NATS 客户端等。

pub mod nats;

pub use nats::NatsClientFactory;




