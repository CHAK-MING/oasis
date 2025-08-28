//! 健康检查接口模块
//!
//! 这个模块提供了gRPC健康检查服务的实现，包括：
//! - 基本的健康检查端点
//! - 详细的健康状态信息
//! - 与基础设施层健康检查服务的集成

use async_nats::jetstream::Context;
use oasis_core::error::CoreError;
use oasis_core::proto::AgentInfo;
use std::collections::HashMap;

/// 健康检查服务接口
#[async_trait::async_trait]
pub trait HealthCheckService: Send + Sync {
    /// 获取在线 Agent 信息
    async fn get_online_agents(&self) -> Result<Vec<AgentInfo>, CoreError>;
}

/// 健康检查服务
pub struct HealthService {
    nats_healthy: std::sync::Arc<tokio::sync::RwLock<bool>>,
    agents_healthy: std::sync::Arc<tokio::sync::RwLock<bool>>,
    online_agents: std::sync::Arc<tokio::sync::RwLock<HashMap<String, AgentInfo>>>,
    health_check_service: std::sync::Arc<dyn HealthCheckService>,
    reporter: std::sync::Arc<tokio::sync::Mutex<Option<tonic_health::server::HealthReporter>>>,
    min_online_agents: std::sync::Arc<tokio::sync::RwLock<usize>>,
}

impl HealthService {
    /// 创建新的健康检查服务
    pub fn new(health_check_service: std::sync::Arc<dyn HealthCheckService>) -> Self {
        Self {
            nats_healthy: std::sync::Arc::new(tokio::sync::RwLock::new(false)),
            agents_healthy: std::sync::Arc::new(tokio::sync::RwLock::new(false)),
            online_agents: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            health_check_service,
            reporter: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            min_online_agents: std::sync::Arc::new(tokio::sync::RwLock::new(1)),
        }
    }

    /// 注册 tonic_health reporter 以便运行期动态更新标准健康状态
    pub async fn register_reporter(&self, reporter: tonic_health::server::HealthReporter) {
        let mut guard = self.reporter.lock().await;
        *guard = Some(reporter);
    }

    pub async fn set_serving_oasis(&self) {
        if let Some(ref mut rep) = *self.reporter.lock().await {
            let _ = rep
                .set_serving::<oasis_core::proto::oasis_service_server::OasisServiceServer<
                    crate::interface::grpc::server::OasisServer,
                >>()
                .await;
        }
    }

    pub async fn set_not_serving_oasis(&self) {
        if let Some(ref mut rep) = *self.reporter.lock().await {
            let _ = rep
                .set_not_serving::<oasis_core::proto::oasis_service_server::OasisServiceServer<
                    crate::interface::grpc::server::OasisServer,
                >>()
                .await;
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

    /// 配置在线 Agent 数阈值（默认 1）
    pub async fn set_min_online_agents(&self, min: usize) {
        let mut guard = self.min_online_agents.write().await;
        *guard = min.max(1);
    }

    /// 更新 Agent 状态
    pub async fn update_agent_status(&self, agent_id: &str, is_online: bool) {
        let mut agents_guard = self.online_agents.write().await;
        if is_online {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                agents_guard.entry(agent_id.to_string())
            {
                // 从健康检查服务获取 Agent 信息
                match self.health_check_service.get_online_agents().await {
                    Ok(agents) => {
                        if let Some(agent_info) = agents.into_iter().find(|a| {
                            a.agent_id
                                .as_ref()
                                .map(|x| x.value.as_str())
                                == Some(agent_id)
                        }) {
                            entry.insert(agent_info);
                        } else {
                            // 如果找不到，插入一个空的 AgentInfo
                            entry.insert(AgentInfo {
                                agent_id: Some(oasis_core::proto::AgentId {
                                    value: agent_id.to_string(),
                                }),
                                is_online: false,
                                facts: None,
                                labels: HashMap::new(),
                                groups: vec![],
                            });
                        }
                        tracing::info!(
                            "Agent {} came online. Total online: {}",
                            agent_id,
                            agents_guard.len()
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to get agent info for {}: {}. Using empty info.",
                            agent_id,
                            e
                        );
                        entry.insert(AgentInfo {
                            agent_id: Some(oasis_core::proto::AgentId {
                                value: agent_id.to_string(),
                            }),
                            is_online: false,
                            facts: None,
                            labels: HashMap::new(),
                            groups: vec![],
                        });
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

        // 更新整体健康状态（达到最小在线阈值）
        let required = *self.min_online_agents.read().await;
        let mut agents_healthy = self.agents_healthy.write().await;
        *agents_healthy = agents_guard.len() >= required;
    }

    /// 获取总体健康状态
    pub async fn is_healthy(&self) -> bool {
        let nats_ok = self.is_nats_healthy().await;
        let agents_ok = self.is_agents_healthy().await;
        nats_ok && agents_ok
    }

    /// 启动健康检查监控
    pub async fn start_monitoring(&self, jetstream: Context, heartbeat_ttl_sec: u64) {
        let nats_healthy = self.nats_healthy.clone();
        let agents_healthy = self.agents_healthy.clone();
        let online_agents = self.online_agents.clone();
        let reporter = self.reporter.clone();
        let min_online_agents = self.min_online_agents.clone();
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
                        // 更新标准健康状态为 Serving
                        if let Some(ref mut rep) = *reporter.lock().await {
                            let _ = rep
                                .set_serving::<oasis_core::proto::oasis_service_server::OasisServiceServer<
                                    crate::interface::grpc::server::OasisServer,
                                >>()
                                .await;
                        }
                        tracing::debug!("NATS health check passed");
                    }
                    Err(e) => {
                        {
                            let mut status = nats_healthy.write().await;
                            *status = false;
                        }
                        // 更新标准健康状态为 NotServing
                        if let Some(ref mut rep) = *reporter.lock().await {
                            let _ = rep
                                .set_not_serving::<oasis_core::proto::oasis_service_server::OasisServiceServer<
                                    crate::interface::grpc::server::OasisServer,
                                >>()
                                .await;
                        }
                        tracing::warn!("NATS health check failed: {}", e);
                    }
                }

                // 这里只更新整体健康状态，基于当前内存中的 online_agents 状态
                {
                    let online_agents_guard = online_agents.read().await;
                    let required = *min_online_agents.read().await;
                    let mut status = agents_healthy.write().await;
                    *status = online_agents_guard.len() >= required;
                }
            }
        });

        tracing::info!("Health monitoring service started successfully");
    }
}

/// 检查 NATS JetStream 健康状态
async fn check_nats(jetstream: &Context, _heartbeat_ttl_sec: u64) -> Result<(), CoreError> {
    // 简化：只检查心跳 bucket 可访问即可证明 JetStream 健康
    let _ = jetstream
                    .get_key_value(oasis_core::JS_KV_AGENT_HEARTBEAT)
        .await
        .map_err(|e| CoreError::Nats {
            message: format!("Failed to access heartbeat KV store: {}", e),
        })?;

    tracing::debug!("NATS heartbeat KV is accessible");
    Ok(())
}

// 已移除：自由函数 check_agents（当前走 use case 流程）
