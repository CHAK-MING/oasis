//! 健康检查接口模块
//!
//! 这个模块提供了gRPC健康检查服务的实现，包括：
//! - 基本的健康检查端点
//! - 详细的健康状态信息
//! - 与基础设施层健康检查服务的集成

use async_nats::jetstream::Context;
use oasis_core::error::CoreError;
use oasis_core::selector::NodeAttributes;
use std::collections::HashMap;

use crate::application::ports::repositories::NodeRepository;

/// 健康检查服务
pub struct HealthService {
    nats_healthy: std::sync::Arc<tokio::sync::RwLock<bool>>,
    agents_healthy: std::sync::Arc<tokio::sync::RwLock<bool>>,
    online_agents: std::sync::Arc<tokio::sync::RwLock<HashMap<String, NodeAttributes>>>,
    node_repo: std::sync::Arc<dyn NodeRepository>,
}

impl HealthService {
    /// 创建新的健康检查服务
    pub fn new(node_repo: std::sync::Arc<dyn NodeRepository>) -> Self {
        Self {
            nats_healthy: std::sync::Arc::new(tokio::sync::RwLock::new(false)),
            agents_healthy: std::sync::Arc::new(tokio::sync::RwLock::new(false)),
            online_agents: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            node_repo,
        }
    }

    /// 获取 NATS 健康状态
    pub async fn is_nats_healthy(&self) -> bool {
        *self.nats_healthy.read().await
    }

    /// 获取 Agent 健康状态
    pub async fn is_agents_healthy(&self) -> bool {
        *self.agents_healthy.read().await
    }

    /// 获取在线 Agent 数量
    pub async fn get_online_agents_count(&self) -> usize {
        self.online_agents.read().await.len()
    }

    // 已移除：获取在线 Agent 列表（当前未对外提供）

    /// 更新 Agent 状态
    pub async fn update_agent_status(&self, agent_id: &str, is_online: bool) {
        let mut agents_guard = self.online_agents.write().await;
        if is_online {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                agents_guard.entry(agent_id.to_string())
            {
                // 只有当 Agent *确实* 不在线时，才去获取它的属性并插入
                match self.node_repo.get(agent_id).await {
                    Ok(node) => {
                        entry.insert(node.to_attributes());
                        tracing::info!(
                            "Agent {} came online. Total online: {}",
                            agent_id,
                            agents_guard.len()
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to get attributes for new agent {}: {}. It will be marked online with no attributes.",
                            agent_id,
                            e
                        );
                        // 即使获取属性失败，也插入一个空对象以标记其在线
                        entry.insert(NodeAttributes::new(agent_id.to_string()));
                    }
                }
            }
            // 如果 Agent 已经存在 (is Some)，则什么都不做，也不打印日志
        } else {
            // Agent 下线
            if agents_guard.remove(agent_id).is_some() {
                // 只有当 Agent *确实* 在线时，移除操作才会返回 Some
                tracing::info!(
                    "Agent {} went offline. Total online: {}",
                    agent_id,
                    agents_guard.len()
                );
            }
            // 如果 Agent 本来就不在线，则什么都不做
        }

        // 更新整体健康状态
        let mut agents_healthy = self.agents_healthy.write().await;
        *agents_healthy = !agents_guard.is_empty();
    }

    /// 获取总体健康状态
    pub async fn is_healthy(&self) -> bool {
        let nats_ok = self.is_nats_healthy().await;
        let agents_ok = self.is_agents_healthy().await;
        nats_ok && agents_ok
    }

    /// 启动健康检查监控
    pub async fn start_monitoring(
        &self,
        jetstream: Context,
        _node_repo: std::sync::Arc<dyn NodeRepository>,
        heartbeat_ttl_sec: u64,
    ) {
        let nats_healthy = self.nats_healthy.clone();
        let agents_healthy = self.agents_healthy.clone();
        let online_agents = self.online_agents.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

            loop {
                interval.tick().await;

                // 检查 NATS JetStream 健康状态
                match check_nats(&jetstream, heartbeat_ttl_sec).await {
                    Ok(_) => {
                        {
                            let mut status = nats_healthy.write().await;
                            *status = true;
                        }
                        tracing::debug!("NATS health check passed");
                    }
                    Err(e) => {
                        {
                            let mut status = nats_healthy.write().await;
                            *status = false;
                        }
                        tracing::warn!("NATS health check failed: {}", e);
                    }
                }

                // 这里只更新整体健康状态，基于当前内存中的 online_agents 状态
                {
                    let online_agents_guard = online_agents.read().await;
                    let mut status = agents_healthy.write().await;
                    *status = !online_agents_guard.is_empty();
                }
            }
        });

        tracing::info!("Health monitoring service started successfully");
    }
}

/// 检查 NATS JetStream 健康状态
async fn check_nats(jetstream: &Context, _heartbeat_ttl_sec: u64) -> Result<(), CoreError> {
    // 检查必需的 KV bucket 是否存在
    let _heartbeat_kv = jetstream
        .get_key_value(oasis_core::JS_KV_NODE_HEARTBEAT)
        .await
        .map_err(|e| CoreError::Nats {
            message: format!("Failed to get heartbeat KV store: {}", e),
        })?;

    // 检查 facts KV bucket
    let _facts_kv = jetstream
        .get_key_value(oasis_core::JS_KV_NODE_FACTS)
        .await
        .map_err(|e| CoreError::Nats {
            message: format!("Failed to get facts KV store: {}", e),
        })?;

    // 检查 labels KV bucket
    let _labels_kv = jetstream
        .get_key_value(oasis_core::JS_KV_NODE_LABELS)
        .await
        .map_err(|e| CoreError::Nats {
            message: format!("Failed to get labels KV store: {}", e),
        })?;

    // 简化：只检查 bucket 是否存在，不检查配置
    tracing::debug!("All required NATS KV buckets are accessible");

    Ok(())
}

// 已移除：自由函数 check_agents（当前走 use case 流程）
