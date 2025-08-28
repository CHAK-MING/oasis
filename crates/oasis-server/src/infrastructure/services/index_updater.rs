use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::application::selector::SelectorEngine;
use crate::domain::events::AgentEvent;
use oasis_core::types::AgentId;

/// 索引更新服务 - 监听 KV 事件并更新倒排索引
pub struct IndexUpdaterService {
    selector_engine: Arc<SelectorEngine>,
    event_receiver: broadcast::Receiver<AgentEvent>,
    shutdown_token: tokio_util::sync::CancellationToken,
}

impl IndexUpdaterService {
    pub fn new(
        selector_engine: Arc<SelectorEngine>,
        event_receiver: broadcast::Receiver<AgentEvent>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            selector_engine,
            event_receiver,
            shutdown_token,
        }
    }

    /// 启动索引更新循环
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting index updater service");

        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    info!("Shutdown signal received for index updater");
                    break;
                }
                result = self.event_receiver.recv() => {
                    match result {
                        Ok(event) => {
                            // 添加日志记录接收到的事件
                            info!("Index updater received event: {:?}", event);
                            
                            if let Err(e) = self.handle_event(event).await {
                                error!("Error handling event: {}", e);
                                // 继续处理下一个事件，不退出循环
                            }
                        }
                        Err(e) => {
                            error!("Error receiving event: {}", e);
                            // 如果是Lagged错误，继续运行
                            if matches!(e, broadcast::error::RecvError::Lagged(_)) {
                                warn!("Event receiver lagged, some events may have been dropped");
                                continue;
                            }
                            // 其他错误则退出
                            break;
                        }
                    }
                }
            }
        }

        info!("Index updater service stopped");
        Ok(())
    }

    /// 处理单个事件
    async fn handle_event(&self, event: AgentEvent) -> Result<()> {
        match event {
            AgentEvent::Online {
                agent_id,
                timestamp: _,
            } => {
                self.handle_online_event(agent_id.as_str()).await?;
            }
            AgentEvent::Offline {
                agent_id,
                timestamp: _,
            } => {
                self.handle_offline_event(agent_id.as_str()).await?;
            }
            AgentEvent::LabelsUpdated {
                agent_id,
                labels,
                timestamp: _,
            } => {
                self.handle_labels_updated_event(agent_id.as_str(), labels)
                    .await?;
            }
            AgentEvent::GroupsUpdated {
                agent_id,
                groups,
                timestamp: _,
            } => {
                info!("Processing GroupsUpdated event for agent: {}, groups: {:?}", agent_id, groups);
                self.handle_groups_updated_event(agent_id.as_str(), groups)
                    .await?;
            }
            AgentEvent::FactsUpdated {
                agent_id,
                facts,
                timestamp: _,
            } => {
                self.handle_facts_updated_event(agent_id.as_str(), facts)
                    .await?;
            }
            AgentEvent::HeartbeatUpdated {
                agent_id,
                timestamp: _,
            } => {
                // 心跳更新通常不需要更新索引
                debug!(agent_id = %agent_id, "Heartbeat updated, skipping index update");
            }
        }
        Ok(())
    }

    /// 处理节点上线事件
    async fn handle_online_event(&self, node_id: &str) -> Result<()> {
        info!(node_id = %node_id, "Handling online event");

        // 获取节点的当前数据（从快照中）
        let index = self.selector_engine.get_index();
        let snapshot = index.snapshot();
        if let Some(agent_data) = snapshot
            .agent_data
            .values()
            .find(|n| n.id.as_str() == node_id)
        {
            // 更新在线状态
            index.upsert_agent(
                agent_data.id.clone(),
                agent_data.labels.clone(),
                agent_data.groups.clone(),
                true, // 设置为在线
            )?;

            info!(node_id = %node_id, "Updated online status");
        } else {
            // 首次出现：插入最小快照，标记为在线
            index.upsert_agent(
                AgentId::from(node_id.to_string()),
                std::collections::HashMap::new(),
                Vec::new(),
                true,
            )?;
            info!(node_id = %node_id, "Inserted new agent as online");
        }

        Ok(())
    }

    /// 处理节点下线事件
    async fn handle_offline_event(&self, node_id: &str) -> Result<()> {
        info!(node_id = %node_id, "Handling offline event");

        // 获取节点的当前数据（从快照中）
        let index = self.selector_engine.get_index();
        let snapshot = index.snapshot();
        if let Some(agent_data) = snapshot
            .agent_data
            .values()
            .find(|n| n.id.as_str() == node_id)
        {
            // 更新在线状态
            index.upsert_agent(
                agent_data.id.clone(),
                agent_data.labels.clone(),
                agent_data.groups.clone(),
                false, // 设置为离线
            )?;

            info!(node_id = %node_id, "Updated offline status");
        }

        Ok(())
    }

    /// 处理标签更新事件
    async fn handle_labels_updated_event(
        &self,
        node_id: &str,
        labels: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        info!(node_id = %node_id, labels = ?labels, "Handling labels updated event");

        // 获取节点的当前数据（从快照中）
        let index = self.selector_engine.get_index();
        let snapshot = index.snapshot();
        if let Some(agent_data) = snapshot
            .agent_data
            .values()
            .find(|n| n.id.as_str() == node_id)
        {
            // 更新标签
            index.upsert_agent(
                agent_data.id.clone(),
                labels, // 使用新的标签
                agent_data.groups.clone(),
                agent_data.is_online,
            )?;

            info!(node_id = %node_id, "Updated labels");
        } else {
            // 首次出现：插入并带上标签
            index.upsert_agent(
                AgentId::from(node_id.to_string()),
                labels,
                Vec::new(),
                false,
            )?;
            info!(node_id = %node_id, "Inserted new agent with labels");
        }

        Ok(())
    }

    /// 处理事实更新事件
    async fn handle_facts_updated_event(
        &self,
        node_id: &str,
        _facts: oasis_core::types::AgentFacts,
    ) -> Result<()> {
        info!(node_id = %node_id, "Handling facts updated event");

        // 获取节点的当前数据（从快照中）
        let index = self.selector_engine.get_index();
        let snapshot = index.snapshot();
        if let Some(agent_data) = snapshot
            .agent_data
            .values()
            .find(|n| n.id.as_str() == node_id)
        {
            // 事实更新通常不影响选择器，但我们可以更新节点数据
            // 这里我们保持现有的标签和组，只更新在线状态
            index.upsert_agent(
                agent_data.id.clone(),
                agent_data.labels.clone(),
                agent_data.groups.clone(),
                agent_data.is_online,
            )?;

            info!(node_id = %node_id, "Updated facts");
        } else {
            // 首次出现：仅建立最小快照
            index.upsert_agent(
                AgentId::from(node_id.to_string()),
                std::collections::HashMap::new(),
                Vec::new(),
                false,
            )?;
            info!(node_id = %node_id, "Inserted new agent from facts update");
        }

        Ok(())
    }

    /// 处理组更新事件
    async fn handle_groups_updated_event(&self, node_id: &str, groups: Vec<String>) -> Result<()> {
        info!(node_id = %node_id, groups = ?groups, "Handling groups updated event");

        // 获取节点的当前数据（从快照中）
        let index = self.selector_engine.get_index();
        let snapshot = index.snapshot();
        if let Some(agent_data) = snapshot
            .agent_data
            .values()
            .find(|n| n.id.as_str() == node_id)
        {
            // 更新组
            index.upsert_agent(
                agent_data.id.clone(),
                agent_data.labels.clone(),
                groups, // 使用新的组
                agent_data.is_online,
            )?;

            info!(node_id = %node_id, "Updated groups");
        } else {
            // 首次出现：插入并带上组
            index.upsert_agent(
                AgentId::from(node_id.to_string()),
                std::collections::HashMap::new(),
                groups,
                false,
            )?;
            info!(node_id = %node_id, "Inserted new agent with groups");
        }

        Ok(())
    }
}
