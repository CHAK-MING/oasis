use std::sync::Arc;

use oasis_core::{error::CoreError, task::TaskSpec};

use crate::application::ports::repositories::{NodeRepository, TaskRepository};
use crate::domain::models::task::Task;

/// 任务执行用例
pub struct ExecuteTaskUseCase {
    task_repo: Arc<dyn TaskRepository>,
    node_repo: Arc<dyn NodeRepository>,
}

impl ExecuteTaskUseCase {
    pub fn new(task_repo: Arc<dyn TaskRepository>, node_repo: Arc<dyn NodeRepository>) -> Self {
        Self {
            task_repo,
            node_repo,
        }
    }

    /// 执行任务的主要用例
    pub async fn execute(
        &self,
        targets: Vec<String>,
        command: String,
        args: Vec<String>,
        timeout_seconds: u32,
        env: Option<std::collections::HashMap<String, String>>,
    ) -> Result<String, CoreError> {
        // 1. 输入参数验证
        self.validate_input(&command, &args, timeout_seconds)?;

        // 2. 解析目标规格，获取实际的 agent 列表
        let target_agents = self.resolve_targets(&targets).await?;

        if target_agents.is_empty() {
            return Err(CoreError::Agent {
                agent_id: "no agents found".to_string(),
                message: "No agents found for the given selector".to_string(),
            });
        }

        // 3. 验证目标节点的在线状态
        let online_agents = self.validate_agents(&target_agents).await?;

        if online_agents.is_empty() {
            return Err(CoreError::Agent {
                agent_id: "no online agents".to_string(),
                message: "No online agents available".to_string(),
            });
        }

        // 4. 构造任务规格
        let agents_vec = online_agents.clone().into_iter().map(|s| s.into()).collect();
        let task_spec = TaskSpec::for_agents(uuid::Uuid::new_v4().to_string(), command, agents_vec)
        .with_args(args)
        .with_env(env.unwrap_or_default())
        .with_timeout(timeout_seconds);

        // 5. 创建任务对象
        let task = Task::from_spec(task_spec);

        // 5.1 可选：持久化任务元数据，便于后续排查
        let _ = self.task_repo.create(task.clone()).await;

        // 6. 发布任务到 JetStream
        let task_id = self.task_repo.publish(task).await?;

        // 7. 发布成功后，任务状态保持为 Pending，等待 Agent 接收并开始执行
        // Agent 接收到任务后会通过状态更新机制将状态改为 Running
        tracing::info!(
            task_id = %task_id,
            target_count = online_agents.len(),
            targets = ?online_agents,
            "Task created and published successfully, status remains Pending until Agent starts execution"
        );

        Ok(task_id)
    }

    /// 输入参数验证
    fn validate_input(
        &self,
        command: &str,
        _args: &[String],
        timeout_seconds: u32,
    ) -> Result<(), CoreError> {
        // 验证命令
        if command.trim().is_empty() {
            return Err(CoreError::InvalidTask {
                reason: "Command cannot be empty".to_string(),
                
            });
        }

        // 验证超时时间
        if timeout_seconds == 0 {
            return Err(CoreError::InvalidTask {
                reason: "Timeout must be greater than 0".to_string(),
                
            });
        }

        Ok(())
    }

    /// 解析目标规格，支持 agent:、default 格式
    async fn resolve_targets(&self, targets: &[String]) -> Result<Vec<String>, CoreError> {
        use std::collections::HashSet;
        let mut resolved_set: HashSet<String> = HashSet::new();

        // 处理明确指定的目标
        for target in targets {
            if target.starts_with("agent:") {
                // 单播：agent:id
                let agent_id =
                    target
                        .strip_prefix("agent:")
                        .ok_or_else(|| CoreError::InvalidTask {
                            reason: format!("Invalid target format: {}", target),
                            
                        })?;
                resolved_set.insert(agent_id.to_string());
            } else if target == "default" || target.is_empty() {
                // 默认：使用默认队列，这里简化为所有在线节点
                let online_agents = self.node_repo.list_online().await?;
                for agent_id in online_agents {
                    resolved_set.insert(agent_id);
                }
            } else {
                // 直接作为agent_id处理
                resolved_set.insert(target.clone());
            }
        }

        // 输出为 Vec
        let mut out: Vec<String> = resolved_set.into_iter().collect();
        out.sort();
        Ok(out)
    }

    /// 验证节点的在线状态
    async fn validate_agents(&self, agent_ids: &[String]) -> Result<Vec<String>, CoreError> {
        // 使用批量获取方法优化性能
        let nodes = self.node_repo.get_nodes_batch(agent_ids).await?;

        let mut online_agents = Vec::new();
        for node in nodes {
            // 使用更宽松的 TTL 来匹配健康检查服务的逻辑
            // 健康检查服务使用 90 秒的 TTL，这里使用相同的值
            if node.is_online(90) {
                online_agents.push(node.id);
            } else {
                tracing::warn!(agent_id = %node.id, "Agent is offline");
            }
        }

        Ok(online_agents)
    }

    /// 获取任务执行结果
    pub async fn get_task_result(
        &self,
        task_id: &str,
        agent_id: Option<&str>,
    ) -> Result<Option<crate::domain::models::task::TaskResult>, CoreError> {
        tracing::info!(
            "get_task_result called: task_id={}, agent_id={:?}",
            task_id,
            agent_id
        );
        if let Some(aid) = agent_id {
            // 如果指定了 agent_id，则直接获取该 agent 的结果
            self.task_repo.get_result(task_id, aid).await
        } else {
            // 否则，获取任务的所有结果并返回最新的一个
            let stream_results = self.task_repo.stream_results(task_id).await?;

            if stream_results.is_empty() {
                Ok(None)
            } else {
                // 返回最新的结果（按时间戳排序）
                let latest_result = stream_results
                    .into_iter()
                    .max_by_key(|result| result.timestamp);
                Ok(latest_result)
            }
        }
    }
}
