// Domain models will be defined here

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemFacts {
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub cpu_count: usize,
    pub memory_total: u64,
    pub memory_available: u64,
    pub disk_count: usize,
    pub network_interface_count: usize,
}
