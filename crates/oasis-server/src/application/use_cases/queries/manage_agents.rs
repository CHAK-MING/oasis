use std::sync::Arc;

use oasis_core::error::CoreError;
use oasis_core::types::AgentId;

use crate::application::ports::repositories::AgentRepository;
use crate::application::selector::SelectorEngine;
use crate::domain::models::agent::Agent;

/// Agent 管理用例
pub struct ManageAgentsUseCase {
    agent_repo: Arc<dyn AgentRepository>,
    selector_engine: Arc<SelectorEngine>,
}

impl ManageAgentsUseCase {
    pub fn new(agent_repo: Arc<dyn AgentRepository>, selector_engine: Arc<SelectorEngine>) -> Self {
        Self {
            agent_repo,
            selector_engine,
        }
    }

    /// 列出所有 Agent
    pub async fn list_agents(&self, selector: Option<&str>) -> Result<Vec<Agent>, CoreError> {
        let online_ids = self.agent_repo.list_online().await?;
        tracing::info!(count = online_ids.len(), ids = ?online_ids, "list_agents online_ids");

        // 使用批量获取方法优化性能
        let mut agents = self.agent_repo.get_agents_batch(&online_ids).await?;
        tracing::info!(count = agents.len(), "list_agents fetched agent details");

        // 如果提供了选择器，则过滤 Agent
        if let Some(selector_expr) = selector {
            if !selector_expr.trim().is_empty() {
                match self.selector_engine.execute(selector_expr) {
                    Ok(result) => {
                        let target_id_set: std::collections::HashSet<_> =
                            result.agent_ids.iter().map(|id| id.as_str()).collect();
                        agents.retain(|agent| target_id_set.contains(agent.id.as_str()));

                        tracing::debug!(
                            expression = %selector_expr,
                            result_count = result.agent_ids.len(),
                            execution_time_ms = result.execution_time_ms,
                            "Used selector"
                        );
                        tracing::info!(
                            after_filter = agents.len(),
                            "list_agents after selector filter"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            expression = %selector_expr,
                            error = %e,
                            "Selector execution failed"
                        );
                        // 如果选择器执行失败，返回空列表
                        agents.clear();
                    }
                }
            }
        }

        Ok(agents)
    }

    pub async fn resolve_selector(&self, expression: &str) -> Result<Vec<String>, CoreError> {
        match self.selector_engine.execute(expression) {
            Ok(result) => {
                let agent_ids: Vec<String> = result
                    .agent_ids
                    .iter()
                    .map(|id| id.as_str().to_string())
                    .collect();

                tracing::info!(
                    expression = %expression,
                    result_count = agent_ids.len(),
                    execution_time_ms = result.execution_time_ms,
                                            "Selector resolved agents"
                );

                Ok(agent_ids)
            }
            Err(e) => {
                tracing::warn!(
                    expression = %expression,
                    error = %e,
                    "Selector execution failed"
                );
                Err(CoreError::InvalidTask {
                    reason: format!("Failed to execute selector: {}", e),
                })
            }
        }
    }

    pub async fn check_agents(
        &self,
        agent_ids: Vec<String>,
    ) -> Result<Vec<oasis_core::proto::AgentStatus>, CoreError> {
        // 使用批量获取方法优化性能
        let agents = self.agent_repo.get_agents_batch(&agent_ids).await?;

        // 创建 Agent ID 到 Agent 的映射，用于快速查找
        let agent_map: std::collections::HashMap<_, _> = agents
            .iter()
            .map(|agent| (agent.id.clone(), agent))
            .collect();

        let mut statuses = Vec::new();
        for id in agent_ids {
            if let Some(agent) = agent_map.get(&AgentId::from(id.clone())) {
                statuses.push(oasis_core::proto::AgentStatus {
                    agent_id: Some(oasis_core::proto::AgentId { value: id }),
                    online: agent.is_online(30),
                });
            } else {
                // Agent 不存在或不在线
                statuses.push(oasis_core::proto::AgentStatus {
                    agent_id: Some(oasis_core::proto::AgentId { value: id }),
                    online: false,
                });
            }
        }

        Ok(statuses)
    }

    pub async fn get_agent_labels(
        &self,
        agent_id: &str,
    ) -> Result<std::collections::HashMap<String, String>, CoreError> {
        let agent = self.agent_repo.get(agent_id).await?;
        Ok(agent.labels)
    }

    pub async fn get_agent_facts(
        &self,
        agent_id: &str,
    ) -> Result<Option<oasis_core::agent::AgentFacts>, CoreError> {
        let agent = self.agent_repo.get(agent_id).await?;
        Ok(Some(agent.facts))
    }
}
