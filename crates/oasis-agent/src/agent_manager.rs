use crate::{
    file_manager::FileManager, nats_client::NatsClient,
    task_manager::TaskManager,
};
use if_addrs::get_if_addrs;
use oasis_core::{
    agent_types::{AgentInfo, AgentStatus},
    core_types::AgentId,
    error::Result,
    constants::{JS_KV_AGENT_HEARTBEAT, JS_KV_AGENT_INFOS, kv_key_facts, kv_key_heartbeat},
};
use std::{collections::HashMap, net::UdpSocket};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubServiceStatus {
    Running,
    Failed,
}

struct SubServiceHealth {
    task_manager: SubServiceStatus,
    file_manager: SubServiceStatus,
}

impl SubServiceHealth {
    fn new() -> Self {
        Self {
            task_manager: SubServiceStatus::Running,
            file_manager: SubServiceStatus::Running,
        }
    }

    fn overall_status(&self) -> AgentStatus {
        if self.task_manager == SubServiceStatus::Failed
            || self.file_manager == SubServiceStatus::Failed
        {
            AgentStatus::Degraded
        } else {
            AgentStatus::Online
        }
    }
}

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
        self.nats_client.ensure_resources().await?;
        self.publish_agent_info(AgentStatus::Online).await?;

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

        let mut task_handle = tokio::spawn({
            let manager = task_manager.clone();
            async move { manager.run().await }
        });

        let mut file_handle = tokio::spawn({
            let manager = file_manager.clone();
            async move { manager.run().await }
        });

        info!("All services started successfully");

        let mut health = SubServiceHealth::new();
        let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut task_done = false;
        let mut file_done = false;

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let current_status = health.overall_status();
                    if let Err(e) = self.send_heartbeat_with_status(current_status).await {
                        error!("Failed to send heartbeat: {}", e);
                    }
                }
                
                result = &mut task_handle, if !task_done => {
                    task_done = true;
                    match result {
                        Ok(Err(e)) => {
                            error!("Task manager failed: {}", e);
                            health.task_manager = SubServiceStatus::Failed;
                            if let Err(pub_err) = self.publish_agent_info(AgentStatus::Degraded).await {
                                error!("Failed to publish degraded status: {}", pub_err);
                            }
                        }
                        Err(e) => {
                            error!("Task manager panicked: {}", e);
                            health.task_manager = SubServiceStatus::Failed;
                            if let Err(pub_err) = self.publish_agent_info(AgentStatus::Degraded).await {
                                error!("Failed to publish degraded status: {}", pub_err);
                            }
                        }
                        Ok(Ok(())) => {
                            info!("Task manager exited normally");
                        }
                    }
                }
                
                result = &mut file_handle, if !file_done => {
                    file_done = true;
                    match result {
                        Ok(Err(e)) => {
                            error!("File manager failed: {}", e);
                            health.file_manager = SubServiceStatus::Failed;
                            if let Err(pub_err) = self.publish_agent_info(AgentStatus::Degraded).await {
                                error!("Failed to publish degraded status: {}", pub_err);
                            }
                        }
                        Err(e) => {
                            error!("File manager panicked: {}", e);
                            health.file_manager = SubServiceStatus::Failed;
                            if let Err(pub_err) = self.publish_agent_info(AgentStatus::Degraded).await {
                                error!("Failed to publish degraded status: {}", pub_err);
                            }
                        }
                        Ok(Ok(())) => {
                            info!("File manager exited normally");
                        }
                    }
                }
                
                _ = self.shutdown_token.cancelled() => {
                    info!("Shutdown requested, stopping services...");
                    break;
                }
            }
        }

        if let Err(e) = self.publish_agent_info(AgentStatus::Offline).await {
            error!("Failed to publish offline status: {}", e);
        }

        Ok(())
    }

    async fn send_heartbeat_with_status(&self, status: AgentStatus) -> Result<()> {
        let kv = self
            .nats_client
            .jetstream
            .get_key_value(JS_KV_AGENT_HEARTBEAT)
            .await?;
        let key = kv_key_heartbeat(self.agent_id.as_str());
        let timestamp = chrono::Utc::now().timestamp();
        
        let heartbeat_data = format!("{}|{:?}", timestamp, status);
        kv.put(&key, heartbeat_data.into()).await?;
        debug!("Sent heartbeat for agent: {} with status: {:?}", self.agent_id, status);

        Ok(())
    }

    async fn publish_agent_info(&self, status: AgentStatus) -> Result<()> {
        use prost::Message;

        let info: HashMap<String, String> = if status == AgentStatus::Online {
            let mut info = self.info.clone();
            let hostname = hostname::get()
                .ok()
                .and_then(|s| s.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string());

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
                    UdpSocket::bind("0.0.0.0:0")
                        .ok()
                        .and_then(|s| s.local_addr().ok())
                        .map(|a| a.ip().to_string())
                        .unwrap_or_else(|| "127.0.0.1".to_string())
                });

            let system = System::new_with_specifics(
                RefreshKind::nothing()
                    .with_cpu(CpuRefreshKind::nothing())
                    .with_memory(MemoryRefreshKind::nothing()),
            );

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
            info.insert("__system_os_name".to_string(), os_name);
            info.insert("__system_os_version".to_string(), os_version);
            info.insert("__system_kernel_version".to_string(), kernel_version);
            info
        } else {
            self.info.clone()
        };

        let agent_info = AgentInfo {
            agent_id: self.agent_id.clone(),
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
