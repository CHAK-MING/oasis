use anyhow::Result;
use async_nats::jetstream;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use oasis_core::error::CoreError;
use std::collections::HashMap;

use oasis_core::constants::{
    JS_KV_AGENT_FACTS, JS_KV_AGENT_HEARTBEAT, JS_KV_AGENT_LABELS, kv_key_facts, kv_key_heartbeat,
    kv_key_labels,
};
use oasis_core::proto;
use oasis_core::types::AgentId;
use prost::Message;

use crate::application::ports::repositories::AgentRepository;
use crate::domain::models::agent::Agent;
use crate::infrastructure::persistence::utils as persist;

/// NATS JetStream 实现的 Agent 仓储
pub struct NatsAgentRepository {
    js: jetstream::Context,
}

impl NatsAgentRepository {
    pub fn new(js: jetstream::Context) -> Self {
        Self { js }
    }

    /// 从分桶 KV（labels/facts/heartbeat）装配 Agent
    async fn get_agent_from_kv(&self, agent_id: &str) -> Result<Option<Agent>, CoreError> {
        // Heartbeat
        let hb_store = self
            .js
            .get_key_value(JS_KV_AGENT_HEARTBEAT)
            .await
            .map_err(persist::map_nats_err)?;
        let hb_key = kv_key_heartbeat(agent_id);
        let hb_entry = hb_store.get(&hb_key).await.map_err(persist::map_nats_err)?;
        if hb_entry.is_none() {
            return Ok(None);
        }
        // Labels (protobuf: AgentLabels)
        let labels_store = self
            .js
            .get_key_value(JS_KV_AGENT_LABELS)
            .await
            .map_err(persist::map_nats_err)?;
        let labels_key = kv_key_labels(agent_id);
        let labels_entry = labels_store
            .get(&labels_key)
            .await
            .map_err(persist::map_nats_err)?;
        let mut labels: HashMap<String, String> = if let Some(e) = labels_entry {
            let pl =
                proto::AgentLabels::decode(e.as_ref()).map_err(|e| CoreError::Serialization {
                    message: format!("Failed to decode AgentLabels: {}", e),
                })?;
            pl.labels
        } else {
            HashMap::new()
        };
        // Extract groups from special key if present
        let groups: Vec<String> = if let Some(g) = labels.remove("__groups") {
            g.split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        } else {
            Vec::new()
        };
        // Facts (protobuf: AgentFacts)
        let facts_store = self
            .js
            .get_key_value(JS_KV_AGENT_FACTS)
            .await
            .map_err(persist::map_nats_err)?;
        let facts_key = kv_key_facts(agent_id);
        let facts_entry = facts_store
            .get(&facts_key)
            .await
            .map_err(persist::map_nats_err)?;
        let facts: oasis_core::agent::AgentFacts = if let Some(e) = facts_entry {
            let pf =
                proto::AgentFacts::decode(e.as_ref()).map_err(|e| CoreError::Serialization {
                    message: format!("Failed to decode AgentFacts: {}", e),
                })?;
            oasis_core::agent::AgentFacts {
                agent_id: AgentId::from(pf.agent_id.unwrap_or_default().value),
                hostname: pf.hostname,
                primary_ip: pf.primary_ip,
                cpu_arch: pf.cpu_arch,
                cpu_cores: pf.cpu_cores as u32,
                memory_total_bytes: pf.memory_total_bytes as u64,
                os_name: pf.os_name,
                os_version: pf.os_version,
                kernel_version: pf.kernel_version,
                boot_id: pf.boot_id,
                network_interfaces: pf
                    .network_interfaces
                    .into_iter()
                    .map(|ni| oasis_core::agent::NetworkInterface {
                        name: ni.name,
                        mac: ni.mac.filter(|m| !m.is_empty()),
                        ipv4: ni.ipv4,
                        ipv6: ni.ipv6,
                    })
                    .collect(),
                cidrs: pf.cidrs,
                agent_version: pf.agent_version,
                collected_at: pf.collected_at,
            }
        } else {
            // Provide a minimal placeholder facts
            oasis_core::agent::AgentFacts {
                agent_id: AgentId::from(agent_id.to_string()),
                hostname: String::new(),
                primary_ip: String::new(),
                cpu_arch: String::new(),
                cpu_cores: 0,
                memory_total_bytes: 0,
                os_name: String::new(),
                os_version: String::new(),
                kernel_version: String::new(),
                boot_id: String::new(),
                network_interfaces: Vec::new(),
                cidrs: Vec::new(),
                agent_version: String::new(),
                collected_at: 0,
            }
        };

        // Heartbeat (protobuf: AgentHeartbeat)
        let heartbeat: crate::domain::models::agent::AgentHeartbeat = if let Some(e) = hb_entry {
            let ph = proto::AgentHeartbeat::decode(e.as_ref()).map_err(|e| {
                CoreError::Serialization {
                    message: format!("Failed to decode AgentHeartbeat: {}", e),
                }
            })?;
            crate::domain::models::agent::AgentHeartbeat {
                timestamp: ph.last_seen,
                version: String::new(),
            }
        } else {
            crate::domain::models::agent::AgentHeartbeat {
                timestamp: 0,
                version: "".to_string(),
            }
        };

        let agent = Agent {
            id: AgentId::from(agent_id.to_string()),
            heartbeat,
            labels,
            facts,
            status: crate::domain::models::agent::AgentStatus::Online,
            groups,
            version: 0,
        };
        Ok(Some(agent))
    }

    /// 列出在线 Agent（基于心跳 KV 当前存在的键）
    async fn list_online_from_kv(&self) -> Result<Vec<String>, CoreError> {
        let store = self
            .js
            .get_key_value(JS_KV_AGENT_HEARTBEAT)
            .await
            .map_err(persist::map_nats_err)?;
        let mut ids = Vec::new();
        let mut stream = store.keys().await.map_err(persist::map_nats_err)?;
        while let Some(key) = stream.try_next().await.map_err(persist::map_nats_err)? {
            ids.push(key);
        }
        Ok(ids)
    }
}

#[async_trait]
impl AgentRepository for NatsAgentRepository {
    async fn get(&self, id: &str) -> Result<Agent, CoreError> {
        self.get_agent_from_kv(id)
            .await?
            .ok_or_else(|| CoreError::agent_not_found(id))
    }

    async fn list_online(&self) -> Result<Vec<String>, CoreError> {
        self.list_online_from_kv().await
    }

    async fn update_labels(
        &self,
        id: &str,
        labels: HashMap<String, String>,
    ) -> Result<(), CoreError> {
        let store = self
            .js
            .get_key_value(JS_KV_AGENT_LABELS)
            .await
            .map_err(persist::map_nats_err)?;
        let key = kv_key_labels(id);
        // 统一使用 Protobuf 序列化
        let pl = proto::AgentLabels {
            agent_id: Some(proto::AgentId {
                value: id.to_string(),
            }),
            labels,
            updated_at: chrono::Utc::now().timestamp(),
            updated_by: "oasis-server".to_string(),
        };
        let data = pl.encode_to_vec();
        store
            .put(&key, data.into())
            .await
            .map_err(persist::map_nats_err)?;
        Ok(())
    }

    async fn update_groups(&self, id: &str, groups: Vec<String>) -> Result<(), CoreError> {
        let store = self
            .js
            .get_key_value(JS_KV_AGENT_LABELS)
            .await
            .map_err(persist::map_nats_err)?;
        let key = kv_key_labels(id);
        // 读取现有（Protobuf），若不存在则创建空
        let mut labels: HashMap<String, String> =
            if let Some(entry) = store.get(&key).await.map_err(persist::map_nats_err)? {
                match proto::AgentLabels::decode(entry.as_ref()) {
                    Ok(pl) => pl.labels,
                    Err(_) => HashMap::new(),
                }
            } else {
                HashMap::new()
            };
        labels.insert("__groups".to_string(), groups.join(","));
        let pl = proto::AgentLabels {
            agent_id: Some(proto::AgentId {
                value: id.to_string(),
            }),
            labels,
            updated_at: chrono::Utc::now().timestamp(),
            updated_by: "oasis-server".to_string(),
        };
        let data = pl.encode_to_vec();
        store
            .put(&key, data.into())
            .await
            .map_err(persist::map_nats_err)?;
        Ok(())
    }

    async fn get_agents_batch(&self, agent_ids: &[String]) -> Result<Vec<Agent>, CoreError> {
        let mut tasks = futures::stream::FuturesUnordered::new();
        for agent_id in agent_ids {
            let repo = self.js.clone();
            let id = agent_id.clone();
            tasks.push(async move {
                let repo = NatsAgentRepository::new(repo);
                match repo.get_agent_from_kv(&id).await {
                    Ok(Some(agent)) => Some(agent),
                    _ => None,
                }
            });
        }
        let mut result = Vec::new();
        while let Some(opt) = tasks.next().await {
            if let Some(agent) = opt {
                result.push(agent);
            }
        }
        Ok(result)
    }
}
