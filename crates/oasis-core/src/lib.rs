pub mod agent;
pub mod backoff;
pub mod config;
pub mod constants;
pub mod dlq;
pub mod error;
pub mod patch;
pub mod proto_impls;
pub mod rate_limit;
pub mod selector;
pub mod shutdown;
pub mod task;
pub mod telemetry;
pub mod transport;
mod type_defs;

pub use constants::*;
pub use error::{CoreError, Result};

pub mod types {
    pub use crate::agent::*;
    pub use crate::task::*;
    pub use crate::type_defs::*;
}

pub mod proto {
    tonic::include_proto!("oasis");
}
