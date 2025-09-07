pub mod selector_engine;

use crate::infrastructure::{
    monitor::{agent_info_monitor::AgentInfoMonitor, heartbeat_monitor::HeartbeatMonitor},
    services::agent_service::selector_engine::{QueryCacheConfig, QueryResult, SelectorEngine},
};
use async_nats::jetstream::Context;
use oasis_core::core_types::AgentId;
use oasis_core::error::{CoreError, ErrorSeverity, Result as CoreResult};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 选择器服务：封装表达式解析与索引查询
pub struct AgentService {
    jetstream: Arc<Context>,
    engine: Arc<SelectorEngine>,
}

impl AgentService {
    pub fn new(
        jetstream: Arc<Context>,
        heartbeat: Arc<HeartbeatMonitor>,
        agent_info: Arc<AgentInfoMonitor>,
        shutdown_token: CancellationToken,
    ) -> Self {
        let engine = Arc::new(
            SelectorEngine::new(heartbeat, agent_info, shutdown_token).with_cache_config(
                QueryCacheConfig {
                    max_entries: 2000, // 增大缓存容量
                    ttl_seconds: 600,  // 10分钟 TTL
                    enable_cache: true,
                    cleanup_interval_seconds: 120, // 2分钟清理间隔
                },
            ),
        );

        Self { jetstream, engine }
    }

    /// 解析表达式，返回所有匹配的 Agent（不做在线过滤）
    pub async fn query(&self, expression: &str) -> CoreResult<QueryResult> {
        self.engine.query(expression).await
    }

    // 只读：获取单个/批量 AgentInfo
    pub fn get_agent_info(&self, agent_id: &AgentId) -> Option<oasis_core::agent_types::AgentInfo> {
        self.engine.get_agent_info(agent_id)
    }

    pub fn spawn_cache_cleanup(&self) -> Option<JoinHandle<()>> {
        self.engine.clone().spawn_cache_cleanup()
    }

    pub fn list_agent_infos(
        &self,
        agent_ids: &[AgentId],
    ) -> Vec<oasis_core::agent_types::AgentInfo> {
        let mut out = Vec::with_capacity(agent_ids.len());
        for id in agent_ids {
            if let Some(info) = self.get_agent_info(id) {
                out.push(info);
            }
        }
        out
    }

    // 写：删除 Agent 相关数据（KV）
    pub async fn remove_agent(&self, agent_id: &AgentId) -> CoreResult<bool> {
        let mut removed = false;

        if let Ok(store) = self
            .jetstream
            .get_key_value(oasis_core::constants::JS_KV_AGENT_INFOS)
            .await
        {
            let key = oasis_core::constants::kv_key_facts(agent_id.as_str());
            if store.delete(&key).await.is_ok() {
                removed = true;
            }
        }

        if let Ok(store) = self
            .jetstream
            .get_key_value(oasis_core::constants::JS_KV_AGENT_HEARTBEAT)
            .await
        {
            let key = oasis_core::constants::kv_key_heartbeat(agent_id.as_str());
            if store.delete(&key).await.is_ok() {
                removed = true;
            }
        }

        if let Ok(store) = self
            .jetstream
            .get_key_value(oasis_core::constants::JS_KV_AGENT_LABELS)
            .await
        {
            let key = oasis_core::constants::kv_key_labels(agent_id.as_str());
            if store.delete(&key).await.is_ok() {
                removed = true;
            }
        }

        if !removed {
            return Err(CoreError::Internal {
                message: format!("Agent {} not found or failed to remove", agent_id),
                severity: ErrorSeverity::Error,
            });
        }

        Ok(true)
    }
}
