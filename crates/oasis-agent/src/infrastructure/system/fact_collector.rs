use sysinfo::{Disks, Networks, System};
use tracing::info;

use crate::application::ports::fact_collector::FactCollectorPort;
use crate::domain::models::SystemFacts;
use oasis_core::error::Result as CoreResult;

pub struct SystemMonitor {}

impl SystemMonitor {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl FactCollectorPort for SystemMonitor {
    async fn collect(&self) -> CoreResult<SystemFacts> {
        let mut system = System::new_all();
        system.refresh_all();

        let cpu_count = system.cpus().len();
        // sysinfo 的 total_memory/available_memory 已经返回字节；避免重复乘以 1024
        let memory_total = system.total_memory();
        let memory_available = system.available_memory();

        let os_name = System::name().unwrap_or_else(|| "Unknown".to_string());
        let os_version = System::os_version().unwrap_or_else(|| "Unknown".to_string());
        let kernel_version = System::kernel_version().unwrap_or_else(|| "Unknown".to_string());

        // 直接创建 Disks 和 Networks 实例来获取列表
        let disks = Disks::new_with_refreshed_list();
        let networks = Networks::new_with_refreshed_list();

        let facts = SystemFacts {
            os_name,
            os_version,
            kernel_version,
            cpu_count,
            memory_total,
            memory_available,
            disk_count: disks.len(),
            network_interface_count: networks.len(),
        };

        info!(
            "Collected system facts os_name={} os_version={} cpu_count={} memory_total={}",
            facts.os_name, facts.os_version, facts.cpu_count, facts.memory_total
        );

        Ok(facts)
    }
}
