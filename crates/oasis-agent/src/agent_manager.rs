use crate::{
    file_manager::FileManager, heartbeat_service::HeartbeatService, nats_client::NatsClient,
    task_manager::TaskManager,
};
use anyhow::Result;
use if_addrs::get_if_addrs;
use oasis_core::{
    agent_types::{AgentInfo, AgentStatus},
    core_types::AgentId,
};
use std::{collections::HashMap, net::UdpSocket};
use sysinfo::System;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Clone)]
pub struct AgentManager {
    agent_id: AgentId,
    nats_client: NatsClient,
    info: HashMap<String, String>,
    shutdown_token: CancellationToken,
}

impl AgentManager {
    pub fn new(
        agent_id: AgentId,
        nats_client: NatsClient,
        info: HashMap<String, String>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            agent_id,
            nats_client,
            info,
            shutdown_token,
        }
    }

    pub async fn run(&self) -> Result<()> {
        // 确保 NATS 资源
        self.nats_client.ensure_resources().await?;

        // 发布初始 Agent 信息
        self.publish_agent_info(AgentStatus::Online).await?;

        // 创建各个服务
        let heartbeat_service = HeartbeatService::new(
            self.agent_id.clone(),
            self.nats_client.clone(),
            self.shutdown_token.clone(),
        );

        let task_manager = TaskManager::new(
            self.agent_id.clone(),
            self.nats_client.clone(),
            self.shutdown_token.clone(),
        );

        let file_manager = FileManager::new(
            self.agent_id.clone(),
            self.nats_client.clone(),
            self.shutdown_token.clone(),
        );

        // 启动所有服务
        let heartbeat_handle = tokio::spawn({
            let service = heartbeat_service.clone();
            async move {
                if let Err(e) = service.run().await {
                    error!("Heartbeat service failed: {}", e);
                }
            }
        });

        let task_handle = tokio::spawn({
            let manager = task_manager.clone();
            async move {
                if let Err(e) = manager.run().await {
                    error!("Task manager failed: {}", e);
                }
            }
        });

        let file_handle = tokio::spawn({
            let manager = file_manager.clone();
            async move {
                if let Err(e) = manager.run().await {
                    error!("File manager failed: {}", e);
                }
            }
        });

        info!("All services started successfully");

        // 等待关闭信号
        self.shutdown_token.cancelled().await;

        info!("Shutdown requested, stopping services...");

        // 发布离线状态
        if let Err(e) = self.publish_agent_info(AgentStatus::Offline).await {
            error!("Failed to publish offline status: {}", e);
        }

        // 等待所有服务完成
        let _ = tokio::join!(heartbeat_handle, task_handle, file_handle);

        Ok(())
    }

    async fn publish_agent_info(&self, status: AgentStatus) -> Result<()> {
        use oasis_core::constants::{JS_KV_AGENT_INFOS, kv_key_facts};
        use prost::Message;

        let mut info = self.info.clone();
        if status == AgentStatus::Online {
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

            let mut system = System::new_all();
            system.refresh_all();

            let cpu_count = system.cpus().len();
            let memory_total = system.total_memory();

            let os_name = System::name().unwrap_or_else(|| "Unknown".to_string());
            let os_version = System::os_version().unwrap_or_else(|| "Unknown".to_string());
            let kernel_version = System::kernel_version().unwrap_or_else(|| "Unknown".to_string());

            info.insert("__system_hostname".to_string(), hostname);
            info.insert("__system_primary_ip".to_string(), primary_ip);
            info.insert(
                "__system_cpu_arch".to_string(),
                std::env::consts::ARCH.to_string(),
            );
            info.insert(
                "__system_cpu_cores".to_string(),
                (cpu_count as u32).to_string(),
            );
            info.insert(
                "__system_memory_total_gb".to_string(),
                (memory_total / (1024 * 1024 * 1024)).to_string(),
            );
            info.insert("__system_os_name".to_string(), os_name.clone());
            info.insert("__system_os_version".to_string(), os_version.clone());
            info.insert(
                "__system_kernel_version".to_string(),
                kernel_version.clone(),
            );
        }

        let agent_info = AgentInfo {
            id: self.agent_id.clone(),
            status,
            info,
            last_heartbeat: chrono::Utc::now().timestamp(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            capabilities: vec![],
        };

        let kv = self
            .nats_client
            .jetstream
            .get_key_value(JS_KV_AGENT_INFOS)
            .await?;
        let key = kv_key_facts(self.agent_id.as_str());
        let proto: oasis_core::proto::AgentInfoMsg = (&agent_info).into();
        let data = proto.encode_to_vec();

        kv.put(&key, data.into()).await?;
        info!("Published agent info with status: {:?}", status);

        Ok(())
    }
}
