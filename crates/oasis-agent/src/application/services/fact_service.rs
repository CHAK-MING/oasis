use crate::application::ports::fact_collector::FactCollectorPort;
use crate::application::ports::fact_repository::FactRepositoryPort;
use crate::domain::models::SystemFacts;
use oasis_core::{agent::AgentFacts, types::AgentId};
use std::sync::Arc;
use tokio::time::{Duration, interval};
use tracing::{error, info};

use crate::error::Result;

pub struct FactService {
    agent_id: AgentId,
    collector: Arc<dyn FactCollectorPort>,
    repository: Arc<dyn FactRepositoryPort>,
}

impl FactService {
    pub fn new(
        agent_id: AgentId,
        collector: Arc<dyn FactCollectorPort>,
        repository: Arc<dyn FactRepositoryPort>,
    ) -> Self {
        Self {
            agent_id,
            collector,
            repository,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(60)); // 每分钟更新一次

        loop {
            interval.tick().await;

            if let Err(e) = self.collect_and_publish_facts().await {
                error!(error = %e, "Failed to collect and publish facts");
            }
        }
    }

    async fn collect_and_publish_facts(&mut self) -> Result<()> {
        // 收集系统信息
        let facts = self.collector.collect().await?;

        // 转换为 AgentFacts 并发布到 KV 存储
        let _agent_facts = self.convert_to_agent_facts(&facts).await;
        self.repository
            .publish_facts(&facts)
            .await?;
        info!("Published AgentFacts to KV storage");

        Ok(())
    }

    async fn convert_to_agent_facts(&self, facts: &SystemFacts) -> AgentFacts {
        use if_addrs::get_if_addrs;
        use std::net::UdpSocket;

        // 获取主机名
        let hostname = hostname::get()
            .ok()
            .and_then(|s| s.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());

        // 获取主要 IP 地址：优先选非回环的 IPv4
        let primary_ip = get_if_addrs()
            .ok()
            .and_then(|ifs| {
                ifs.into_iter()
                    .filter(|ifa| !ifa.is_loopback())
                    .filter_map(|ifa| match ifa.addr {
                        if_addrs::IfAddr::V4(v4) => Some(v4.ip.to_string()),
                        _ => None,
                    })
                    .next()
            })
            .unwrap_or_else(|| {
                // 回退到本地绑定法
                UdpSocket::bind("0.0.0.0:0")
                    .ok()
                    .and_then(|s| s.local_addr().ok())
                    .map(|a| a.ip().to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string())
            });

        AgentFacts {
            agent_id: self.agent_id.clone(),
            hostname,
            primary_ip,
            cpu_arch: std::env::consts::ARCH.to_string(),
            cpu_cores: facts.cpu_count as u32,
            memory_total_bytes: facts.memory_total,
            os_name: facts.os_name.clone(),
            os_version: facts.os_version.clone(),
            kernel_version: facts.kernel_version.clone(),
            boot_id: read_boot_id().unwrap_or_default(),
            network_interfaces: collect_network_interfaces().unwrap_or_default(),
            cidrs: collect_cidrs().unwrap_or_default(),
            agent_version: env!("CARGO_PKG_VERSION").to_string(),
            collected_at: chrono::Utc::now().timestamp(),
        }
    }
}

fn read_boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|s| s.trim().to_string())
}

fn collect_network_interfaces() -> Option<Vec<oasis_core::agent::NetworkInterface>> {
    let ifs = if_addrs::get_if_addrs().ok()?;
    let mut out = Vec::new();
    // 使用 pnet_datalink 获取 MAC
    let mac_map: std::collections::HashMap<String, String> = pnet_datalink::interfaces()
        .into_iter()
        .filter_map(|iface| iface.mac.map(|m| (iface.name, m.to_string())))
        .collect();

    for ifa in ifs.into_iter().filter(|i| !i.is_loopback()) {
        let name = ifa.name.clone();
        let mac = mac_map.get(&name).cloned();
        let mut ipv4 = Vec::new();
        let mut ipv6 = Vec::new();
        match ifa.addr {
            if_addrs::IfAddr::V4(v4) => ipv4.push(v4.ip.to_string()),
            if_addrs::IfAddr::V6(v6) => ipv6.push(v6.ip.to_string()),
        }
        out.push(oasis_core::agent::NetworkInterface {
            name,
            mac,
            ipv4,
            ipv6,
        });
    }
    Some(out)
}

fn collect_cidrs() -> Option<Vec<String>> {
    // 简化：基于 if_addrs 组合 CIDR 字符串
    let ifs = if_addrs::get_if_addrs().ok()?;
    let mut out = Vec::new();
    for ifa in ifs.into_iter().filter(|i| !i.is_loopback()) {
        match ifa.addr {
            if_addrs::IfAddr::V4(v4) => out.push(format!(
                "{}/{}",
                v4.ip,
                v4.netmask
                    .octets()
                    .iter()
                    .map(|b| b.count_ones())
                    .sum::<u32>()
            )),
            if_addrs::IfAddr::V6(v6) => {
                // 统计 netmask 中 1 的个数作为前缀长度
                let segs = v6.netmask.segments();
                let ones: u32 = segs.iter().map(|seg| seg.count_ones()).sum();
                out.push(format!("{}/{}", v6.ip, ones))
            }
        }
    }
    Some(out)
}
