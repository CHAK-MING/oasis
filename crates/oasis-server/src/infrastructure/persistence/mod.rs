pub mod agent_repository;
pub mod file_repository;
pub mod rollout_repository;
pub mod task_repository;

/// 通用持久化工具：NATS KV/ObjectStore 初始化、错误映射、JSON编解码
pub mod utils {
    use oasis_core::error::CoreError;

    pub fn map_nats_err<E: std::fmt::Display>(e: E) -> CoreError {
        CoreError::Nats {
            message: e.to_string(),
        }
    }
}

pub use file_repository::*;

pub use rollout_repository::*;
pub use task_repository::*;
