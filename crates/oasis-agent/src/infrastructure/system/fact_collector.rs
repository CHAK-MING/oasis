use anyhow::Result;
use sysinfo::{Disks, Networks, System};
use tracing::info;

use crate::application::ports::fact_collector::FactCollectorPort;
use crate::domain::models::SystemFacts;

pub struct SystemMonitor {
    // system: System, // TODO: 当前未使用，考虑移除或实现系统监控功能
}

impl SystemMonitor {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl FactCollectorPort for SystemMonitor {
    async fn collect(&self) -> Result<SystemFacts> {
        let mut sys = System::new_all();
        sys.refresh_all();

        // 直接创建 Disks 和 Networks 实例来获取列表
        let disks = Disks::new_with_refreshed_list();
        let networks = Networks::new_with_refreshed_list();

        let facts = SystemFacts {
            os_name: System::name().unwrap_or_default(),
            os_version: System::os_version().unwrap_or_default(),
            kernel_version: System::kernel_version().unwrap_or_default(),
            cpu_count: sys.cpus().len(),
            memory_total: sys.total_memory(),
            memory_available: sys.available_memory(),
            // 使用 disks 实例获取数量
            disk_count: disks.len(),
            // 使用 networks 实例获取数量
            network_interface_count: networks.len(),
        };

        info!(
            os_name = %facts.os_name,
            os_version = %facts.os_version,
            cpu_count = facts.cpu_count,
            memory_total = facts.memory_total,
            "Collected system facts"
        );

        Ok(facts)
    }
}
