use async_trait::async_trait;
use futures::TryStreamExt;
use futures::future::join_all;
use oasis_core::error::CoreError;
use oasis_core::{
    JS_KV_NODE_FACTS, JS_KV_NODE_HEARTBEAT, JS_KV_NODE_LABELS,
    types::{AgentFacts, AgentHeartbeat, AgentLabels},
};
use prost::Message;
use std::collections::HashMap;
use tokio::sync::OnceCell;

use crate::application::ports::repositories::NodeRepository;
use crate::domain::models::node::{Node, NodeFacts, NodeHeartbeat, NodeLabels};
use crate::infrastructure::persistence::utils as persist;

/// 节点仓储实现 - 基于NATS KV存储
pub struct NatsNodeRepository {
    jetstream: async_nats::jetstream::Context,
    heartbeat_ttl_sec: u64,
    kv_initialized: OnceCell<()>,
}

impl NatsNodeRepository {
    pub fn new(jetstream: async_nats::jetstream::Context, heartbeat_ttl_sec: u64) -> Self {
        Self {
            jetstream,
            heartbeat_ttl_sec,
            kv_initialized: OnceCell::const_new(),
        }
    }

    /// 确保KV存储存在
    async fn ensure_kv_stores(&self) -> Result<(), CoreError> {
        if self.kv_initialized.get().is_some() {
            return Ok(());
        }
        let _ = self.kv_initialized.get_or_init(|| async { () }).await;
        // 确保心跳KV存储存在
        if self
            .jetstream
            .get_key_value(JS_KV_NODE_HEARTBEAT)
            .await
            .is_err()
        {
            let cfg = async_nats::jetstream::kv::Config {
                bucket: JS_KV_NODE_HEARTBEAT.to_string(),
                description: "Agent heartbeat (TTL-based cleanup)".to_string(),
                max_age: std::time::Duration::from_secs(self.heartbeat_ttl_sec),
                history: 1,
                ..Default::default()
            };
            persist::ensure_kv_with_config(&self.jetstream, cfg).await?;
        }

        // 确保facts KV存储存在
        if self
            .jetstream
            .get_key_value(JS_KV_NODE_FACTS)
            .await
            .is_err()
        {
            let cfg = async_nats::jetstream::kv::Config {
                bucket: JS_KV_NODE_FACTS.to_string(),
                description: "Agent facts (versioned, no TTL)".to_string(),
                history: 50,
                max_value_size: 65536,
                ..Default::default()
            };
            persist::ensure_kv_with_config(&self.jetstream, cfg).await?;
        }

        // 确保labels KV存储存在
        if self
            .jetstream
            .get_key_value(JS_KV_NODE_LABELS)
            .await
            .is_err()
        {
            let cfg = async_nats::jetstream::kv::Config {
                bucket: JS_KV_NODE_LABELS.to_string(),
                description: "Agent labels (versioned, no TTL)".to_string(),
                history: 50,
                max_value_size: 65536,
                ..Default::default()
            };
            persist::ensure_kv_with_config(&self.jetstream, cfg).await?;
        }

        Ok(())
    }

    /// 获取心跳存储
    async fn get_heartbeat_store(&self) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        self.ensure_kv_stores().await?;
        self.jetstream
            .get_key_value(JS_KV_NODE_HEARTBEAT)
            .await
            .map_err(persist::map_nats_err)
    }

    /// 获取facts存储
    async fn get_facts_store(&self) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        self.ensure_kv_stores().await?;
        self.jetstream
            .get_key_value(JS_KV_NODE_FACTS)
            .await
            .map_err(persist::map_nats_err)
    }

    /// 获取labels存储
    async fn get_labels_store(&self) -> Result<async_nats::jetstream::kv::Store, CoreError> {
        self.ensure_kv_stores().await?;
        self.jetstream
            .get_key_value(JS_KV_NODE_LABELS)
            .await
            .map_err(persist::map_nats_err)
    }

    /// 批量获取心跳数据（私有方法）
    async fn get_heartbeats_batch(
        &self,
        agent_ids: &[String],
    ) -> Result<HashMap<String, crate::domain::models::node::NodeHeartbeat>, CoreError> {
        let heartbeat_store = self.get_heartbeat_store().await?;
        let mut heartbeats = HashMap::new();

        // 并行获取所有心跳数据
        let futures: Vec<_> = agent_ids
            .iter()
            .map(|id| {
                // Agent 发送的心跳键就是 agent_id 本身
                let key = id.to_string();
                let store = heartbeat_store.clone();
                async move {
                    let result = store.get(&key).await;
                    (id.clone(), result)
                }
            })
            .collect();

        let results = join_all(futures).await;
        for (agent_id, result) in results {
            if let Ok(Some(entry)) = result {
                if let Ok(proto) = oasis_core::proto::AgentHeartbeat::decode(entry.as_ref()) {
                    let core_heartbeat: AgentHeartbeat = (&proto).into();
                    heartbeats.insert(
                        agent_id,
                        crate::domain::models::node::NodeHeartbeat {
                            timestamp: core_heartbeat.last_seen,
                            version: format!("{}.0.0", core_heartbeat.sequence),
                        },
                    );
                }
            }
        }

        Ok(heartbeats)
    }

    /// 批量获取标签数据（私有方法）
    async fn get_labels_batch(
        &self,
        agent_ids: &[String],
    ) -> Result<HashMap<String, crate::domain::models::node::NodeLabels>, CoreError> {
        let labels_store = self.get_labels_store().await?;
        let mut labels_map = HashMap::new();

        let futures: Vec<_> = agent_ids
            .iter()
            .map(|id| {
                let key = oasis_core::kv_key_labels(id);
                let store = labels_store.clone();
                async move {
                    let result = store.get(&key).await;
                    (id.clone(), result)
                }
            })
            .collect();

        let results = join_all(futures).await;
        for (agent_id, result) in results {
            match result {
                Ok(Some(entry)) => {
                    if let Ok(proto) = oasis_core::proto::AgentLabels::decode(entry.as_ref()) {
                        let core_labels: AgentLabels = (&proto).into();
                        labels_map.insert(
                            agent_id,
                            crate::domain::models::node::NodeLabels {
                                labels: core_labels.labels,
                                version: 1,
                            },
                        );
                    }
                }
                _ => {
                    labels_map.insert(
                        agent_id,
                        crate::domain::models::node::NodeLabels {
                            labels: HashMap::new(),
                            version: 0,
                        },
                    );
                }
            }
        }

        Ok(labels_map)
    }

    /// 批量获取事实数据（私有方法）
    async fn get_facts_batch(
        &self,
        agent_ids: &[String],
    ) -> Result<HashMap<String, crate::domain::models::node::NodeFacts>, CoreError> {
        let facts_store = self.get_facts_store().await?;
        let mut facts_map = HashMap::new();

        let futures: Vec<_> = agent_ids
            .iter()
            .map(|id| {
                let key = oasis_core::kv_key_facts(id);
                let store = facts_store.clone();
                async move {
                    let result = store.get(&key).await;
                    (id.clone(), result)
                }
            })
            .collect();

        let results = join_all(futures).await;
        for (agent_id, result) in results {
            match result {
                Ok(Some(entry)) => {
                    if let Ok(proto) = oasis_core::proto::AgentFacts::decode(entry.as_ref()) {
                        let core_facts: AgentFacts = (&proto).into();
                        facts_map.insert(
                            agent_id,
                            crate::domain::models::node::NodeFacts {
                                facts: core_facts,
                                version: 1,
                            },
                        );
                    }
                }
                _ => {
                    facts_map.insert(
                        agent_id.clone(),
                        crate::domain::models::node::NodeFacts {
                            facts: AgentFacts {
                                agent_id: agent_id.clone().into(),
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
                            },
                            version: 0,
                        },
                    );
                }
            }
        }

        Ok(facts_map)
    }
}

#[async_trait]
impl NodeRepository for NatsNodeRepository {
    async fn get(&self, id: &str) -> Result<Node, CoreError> {
        // 获取心跳信息
        let heartbeat_store = self.get_heartbeat_store().await?;
        // Agent 发送的心跳键就是 agent_id 本身
        let heartbeat_key = id.to_string();

        let heartbeat = match heartbeat_store.get(&heartbeat_key).await {
            Ok(Some(entry)) => {
                let proto =
                    oasis_core::proto::AgentHeartbeat::decode(entry.as_ref()).map_err(|e| {
                        CoreError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let core_heartbeat: AgentHeartbeat = (&proto).into();

                NodeHeartbeat {
                    timestamp: core_heartbeat.last_seen,
                    version: format!("{}.0.0", core_heartbeat.sequence), // 使用序列号作为版本
                                                                         // 暂时使用默认环境，后续可从配置或标签获取
                }
            }
            Ok(None) | Err(_) => {
                return Err(CoreError::Agent {
                    agent_id: id.to_string().into(),
                    message: "Agent not found".to_string(),
                });
            }
        };

        // 获取标签信息
        let labels_store = self.get_labels_store().await?;
        let labels_key = oasis_core::kv_key_labels(id);

        let labels = match labels_store.get(&labels_key).await {
            Ok(Some(entry)) => {
                let proto =
                    oasis_core::proto::AgentLabels::decode(entry.as_ref()).map_err(|e| {
                        CoreError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let core_labels: AgentLabels = (&proto).into();

                NodeLabels {
                    labels: core_labels.labels,
                    version: 1,
                }
            }
            Ok(None) => NodeLabels {
                labels: HashMap::new(),
                version: 0,
            },
            Err(e) => {
                return Err(CoreError::Nats {
                    message: e.to_string(),
                });
            }
        };

        // 获取facts信息
        let facts_store = self.get_facts_store().await?;
        let facts_key = oasis_core::kv_key_facts(id);

        let facts = match facts_store.get(&facts_key).await {
            Ok(Some(entry)) => {
                let proto = oasis_core::proto::AgentFacts::decode(entry.as_ref()).map_err(|e| {
                    CoreError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                let core_facts: AgentFacts = (&proto).into();

                NodeFacts {
                    facts: core_facts,
                    version: 1, // 暂时使用固定版本
                }
            }
            Ok(None) => NodeFacts {
                facts: AgentFacts {
                    agent_id: id.to_string().into(),
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
                },
                version: 0,
            },
            Err(e) => {
                return Err(CoreError::Nats {
                    message: e.to_string(),
                });
            }
        };

        // 计算整体版本号（暂时使用固定值）
        let overall_version = 1;

        Ok(Node {
            id: id.to_string().into(),
            heartbeat,
            labels,
            facts,
            status: crate::domain::models::node::NodeStatus::Online,
            groups: Vec::new(),
            version: overall_version,
        })
    }

    async fn list_online(&self) -> Result<Vec<String>, CoreError> {
        let heartbeat_store = self.get_heartbeat_store().await?;
        let keys = heartbeat_store
            .keys()
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?;

        let mut online_agents = Vec::new();
        for key in keys {
            // 由于心跳 KV Store 有 TTL，存在即表示在线
            online_agents.push(key.to_string());
        }

        Ok(online_agents)
    }

    /// 批量获取节点详情，优化 N+1 查询问题
    async fn get_nodes_batch(&self, agent_ids: &[String]) -> Result<Vec<Node>, CoreError> {
        if agent_ids.is_empty() {
            return Ok(Vec::new());
        }

        // 并行获取心跳、标签、事实数据
        let (heartbeat_results, labels_results, facts_results) = tokio::try_join!(
            self.get_heartbeats_batch(agent_ids),
            self.get_labels_batch(agent_ids),
            self.get_facts_batch(agent_ids)
        )?;

        let mut nodes = Vec::new();
        for agent_id in agent_ids {
            let heartbeat = heartbeat_results.get(agent_id).cloned().unwrap_or_else(|| {
                crate::domain::models::node::NodeHeartbeat {
                    timestamp: 0,
                    version: "0.0.0".to_string(),
                }
            });
            let labels = labels_results.get(agent_id).cloned().unwrap_or_else(|| {
                crate::domain::models::node::NodeLabels {
                    labels: HashMap::new(),
                    version: 0,
                }
            });
            let facts = facts_results.get(agent_id).cloned().unwrap_or_else(|| {
                crate::domain::models::node::NodeFacts {
                    facts: AgentFacts {
                        agent_id: agent_id.clone().into(),
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
                    },
                    version: 0,
                }
            });

            nodes.push(Node {
                id: agent_id.clone().into(),
                heartbeat,
                labels,
                facts,
                status: crate::domain::models::node::NodeStatus::Online,
                groups: Vec::new(),
                version: 1,
            });
        }

        Ok(nodes)
    }

    async fn find_by_selector(&self, selector: &str) -> Result<Vec<Node>, CoreError> {
        // 1. Get all online agent IDs.
        let online_agent_ids = self.list_online().await?;
        if online_agent_ids.is_empty() {
            return Ok(Vec::new());
        }

        // 2. Load full node data for all online agents to ensure the selector has complete information.
        let all_nodes = self.get_nodes_batch(&online_agent_ids).await?;

        // 3. Compile the CEL selector once.
        let compiled_selector = oasis_core::selector::CelSelector::new(selector.to_string())
            .map_err(|e| CoreError::InvalidTask {
                reason: format!("Invalid selector: {}", e),
            })?;

        // 4. Filter nodes by matching the selector against their full attributes.
        let matched_nodes = all_nodes
            .into_iter()
            .filter(|node| {
                let attributes = node.to_attributes();
                compiled_selector.matches(&attributes).unwrap_or(false)
            })
            .collect();

        Ok(matched_nodes)
    }

    async fn update_labels(
        &self,
        id: &str,
        labels: HashMap<String, String>,
    ) -> Result<(), CoreError> {
        let labels_store = self.get_labels_store().await?;
        let key = oasis_core::kv_key_labels(id);

        let core_labels = AgentLabels {
            agent_id: id.to_string().into(),
            labels,
            updated_at: chrono::Utc::now().timestamp(),
            updated_by: "oasis-server".to_string(),
        };

        let proto: oasis_core::proto::AgentLabels = (&core_labels).into();
        let data = oasis_core::proto_impls::encoding::to_vec(&proto);

        labels_store
            .put(&key, data.into())
            .await
            .map_err(|e| CoreError::Nats {
                message: e.to_string(),
            })?;

        Ok(())
    }
}
