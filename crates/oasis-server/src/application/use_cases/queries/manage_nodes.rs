use std::sync::Arc;

use oasis_core::error::CoreError;
use oasis_core::selector::NodeAttributes;
use oasis_core::types::AgentId;

use crate::application::ports::repositories::NodeRepository;
use crate::domain::models::node::{Node, NodeFacts};
use crate::domain::services::SelectorEngine;

/// 节点管理用例
pub struct ManageNodesUseCase {
    node_repo: Arc<dyn NodeRepository>,
    selector_engine: Arc<dyn SelectorEngine>,
}

impl ManageNodesUseCase {
    pub fn new(
        node_repo: Arc<dyn NodeRepository>,
        selector_engine: Arc<dyn SelectorEngine>,
    ) -> Self {
        Self {
            node_repo,
            selector_engine,
        }
    }

    /// 列出所有节点
    pub async fn list_nodes(&self, selector: Option<&str>) -> Result<Vec<Node>, CoreError> {
        let online_ids = self.node_repo.list_online().await?;

        // 使用批量获取方法优化性能
        let mut nodes = self.node_repo.get_nodes_batch(&online_ids).await?;

        // 如果提供了选择器，则过滤节点
        if let Some(selector_expr) = selector {
            if !selector_expr.trim().is_empty() {
                let node_attributes: Vec<NodeAttributes> =
                    nodes.iter().map(|node| node.to_attributes()).collect();

                let target_ids = self
                    .selector_engine
                    .resolve(selector_expr, &node_attributes)
                    .await?;
                let target_id_set: std::collections::HashSet<_> = target_ids.into_iter().collect();

                nodes.retain(|node| target_id_set.contains(node.id.as_str()));
            }
        }

        Ok(nodes)
    }

    pub async fn resolve_selector(&self, expression: &str) -> Result<Vec<String>, CoreError> {
        let online_ids = self.node_repo.list_online().await?;
        tracing::info!("Found {} online agents: {:?}", online_ids.len(), online_ids);

        // 使用批量获取方法优化性能
        let nodes = self.node_repo.get_nodes_batch(&online_ids).await?;
        tracing::info!("Retrieved {} nodes from repository", nodes.len());

        // 转换为 NodeAttributes
        let node_attributes: Vec<NodeAttributes> =
            nodes.iter().map(|node| node.to_attributes()).collect();
        tracing::info!("Converted {} nodes to attributes", node_attributes.len());

        let result = self
            .selector_engine
            .resolve(expression, &node_attributes)
            .await?;
        tracing::info!(
            "Selector engine resolved {} matching agents: {:?}",
            result.len(),
            result
        );

        Ok(result)
    }

    pub async fn check_agents(
        &self,
        agent_ids: Vec<String>,
    ) -> Result<Vec<oasis_core::proto::AgentStatus>, CoreError> {
        // 使用批量获取方法优化性能
        let nodes = self.node_repo.get_nodes_batch(&agent_ids).await?;

        // 创建节点ID到节点的映射，用于快速查找
        let node_map: std::collections::HashMap<_, _> =
            nodes.iter().map(|node| (node.id.clone(), node)).collect();

        let mut statuses = Vec::new();
        for id in agent_ids {
            if let Some(node) = node_map.get(&AgentId::from(id.clone())) {
                statuses.push(oasis_core::proto::AgentStatus {
                    agent_id: Some(oasis_core::proto::AgentId { value: id }),
                    online: node.is_online(30),
                });
            } else {
                // 节点不存在或不在线
                statuses.push(oasis_core::proto::AgentStatus {
                    agent_id: Some(oasis_core::proto::AgentId { value: id }),
                    online: false,
                });
            }
        }
        Ok(statuses)
    }

    pub async fn get_node_labels(
        &self,
        agent_id: &str,
    ) -> Result<std::collections::HashMap<String, String>, CoreError> {
        Ok(self.node_repo.get(agent_id).await?.labels.labels)
    }

    pub async fn get_node_facts(&self, agent_id: &str) -> Result<Option<NodeFacts>, CoreError> {
        Ok(Some(self.node_repo.get(agent_id).await?.facts))
    }
}
