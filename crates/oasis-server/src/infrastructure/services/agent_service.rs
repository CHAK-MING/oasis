pub mod selector_engine;

use crate::infrastructure::{
    monitor::{agent_info_monitor::AgentInfoMonitor, heartbeat_monitor::HeartbeatMonitor},
    services::agent_service::selector_engine::{QueryCacheConfig, QueryResult, SelectorEngine},
};
use async_nats::jetstream::Context;
use oasis_core::error::{CoreError, ErrorSeverity, Result as CoreResult};
use oasis_core::{agent_types::AgentInfo, core_types::AgentId, kv_key_facts, proto};
use prost::Message;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

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
            if let Some(mut info) = self.get_agent_info(id) {
                // Merge heartbeat-derived status (source of truth) via SelectorEngine
                if let Some(hb_info) = self.engine.get_agent_heartbeat_info(id) {
                    info.status = hb_info.status;
                    info.last_heartbeat = hb_info.last_heartbeat;
                } else {
                    // No heartbeat entry: treat as Offline with last_heartbeat unchanged
                    info.status = oasis_core::agent_types::AgentStatus::Offline;
                }
                out.push(info);
            }
        }
        out
    }

    // 删除 Agent 相关数据（KV）
    pub async fn remove_agent(&self, agent_id: &AgentId) -> CoreResult<bool> {
        let mut removed = false;

        // 移除缓存
        self.engine.remove_agent(agent_id);

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

    pub async fn set_info_agent(
        &self,
        agent_id: &AgentId,
        info: &HashMap<String, String>,
    ) -> CoreResult<bool> {
        let mut current_agent_info = match self.get_agent_info(agent_id) {
            Some(existing_info) => existing_info,
            None => {
                // 如果Agent不存在，返回错误
                return Err(CoreError::Internal {
                    message: format!("Agent {} not found", agent_id),
                    severity: ErrorSeverity::Error,
                });
            }
        };

        // 2. 设置新的标签和分组信息
        // 保留系统标签（以 __system_ 开头），移除普通标签和分组，然后插入新的标签和分组
        let mut preserved_system_info = HashMap::new();

        // 保留系统标签
        for (key, value) in current_agent_info.info.iter() {
            if key.starts_with("__system_") {
                preserved_system_info.insert(key.clone(), value.clone());
            }
        }

        // 清空现有info，只保留系统标签
        current_agent_info.info = preserved_system_info;

        // 插入新的标签和分组信息
        for (key, value) in info.iter() {
            current_agent_info.info.insert(key.clone(), value.clone());
        }

        // 3. 更新到JetStream KV存储
        match self
            .update_agent_info_to_kv(agent_id, &current_agent_info)
            .await
        {
            Ok(_) => {
                info!(
                    "Successfully updated agent info in KV store for agent: {}",
                    agent_id
                );
            }
            Err(e) => {
                error!(
                    "Failed to update agent info in KV store for agent {}: {}",
                    agent_id, e
                );
                return Err(e);
            }
        }

        // 4. 通过AgentInfoMonitor更新缓存和索引
        // 由于AgentInfoMonitor会监听KV的变更，这里更新会被自动同步到缓存
        // 但为了确保立即生效，我们也直接更新缓存
        self.engine
            .update_agent_info(agent_id.clone(), current_agent_info.clone())
            .await;

        info!(
            "Successfully set agent info for {}: labels/groups updated",
            agent_id
        );

        Ok(true)
    }

    /// 将AgentInfo更新到JetStream KV存储
    async fn update_agent_info_to_kv(
        &self,
        agent_id: &AgentId,
        agent_info: &AgentInfo,
    ) -> CoreResult<()> {
        // 获取Agent Info KV存储
        let kv_store = self
            .jetstream
            .get_key_value(oasis_core::constants::JS_KV_AGENT_INFOS)
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to get agent infos KV store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        // 生成KV键名
        let key = kv_key_facts(agent_id.as_str());

        // 转换为Proto格式并序列化
        let proto_info: proto::AgentInfoMsg = agent_info.into();
        let data = proto_info.encode_to_vec();

        // 写入KV存储
        kv_store
            .put(&key, data.into())
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to put agent info to KV store: {}", e),
                severity: ErrorSeverity::Error,
            })?;

        Ok(())
    }
}
