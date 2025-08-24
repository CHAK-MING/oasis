use async_trait::async_trait;
use oasis_core::error::CoreError;

use crate::domain::models::{
    file::{FileInfo, FileUploadResult},
    node::Node,
    rollout::Rollout,
    task::{Task, TaskResult},
};

/// 节点仓储接口
#[async_trait]
pub trait NodeRepository: Send + Sync {
    /// 根据ID获取节点
    async fn get(&self, id: &str) -> Result<Node, CoreError>;

    /// 列出所有在线节点
    async fn list_online(&self) -> Result<Vec<String>, CoreError>;

    /// 根据选择器查询节点
    async fn find_by_selector(&self, selector: &str) -> Result<Vec<Node>, CoreError>;

    /// 更新节点标签
    async fn update_labels(
        &self,
        id: &str,
        labels: std::collections::HashMap<String, String>,
    ) -> Result<(), CoreError>;

    /// 批量获取节点详情，优化 N+1 查询问题
    async fn get_nodes_batch(&self, agent_ids: &[String]) -> Result<Vec<Node>, CoreError>;
}

/// 任务仓储接口
#[async_trait]
pub trait TaskRepository: Send + Sync {
    /// 创建任务（可选：用于调试/审计）
    async fn create(&self, task: Task) -> Result<String, CoreError>;

    /// 获取任务结果
    async fn get_result(
        &self,
        task_id: &str,
        agent_id: &str,
    ) -> Result<Option<TaskResult>, CoreError>;

    /// 批量获取任务结果
    async fn get_results_batch(
        &self,
        task_agent_pairs: &[(String, String)],
    ) -> Result<Vec<Option<TaskResult>>, CoreError>;

    /// 流式获取任务结果
    async fn stream_results(&self, task_id: &str) -> Result<Vec<TaskResult>, CoreError>;

    /// 创建任务结果消费者
    async fn create_result_consumer(
        &self,
        task_id: &str,
    ) -> Result<Box<dyn ResultConsumer>, CoreError>;

    /// 发布任务到JetStream
    async fn publish(&self, task: Task) -> Result<String, CoreError>;
}

/// 结果消费者trait
#[async_trait]
pub trait ResultConsumer: Send + Sync {
    /// 获取下一个任务结果
    async fn next_result(&mut self) -> Result<Option<TaskResult>, CoreError>;
}

/// 灰度发布仓储接口
#[async_trait]
pub trait RolloutRepository: Send + Sync {
    /// 创建发布
    async fn create(&self, rollout: Rollout) -> Result<String, CoreError>;

    /// 根据ID获取发布
    async fn get(&self, id: &str) -> Result<Rollout, CoreError>;

    /// 更新发布状态
    async fn update(&self, rollout: Rollout) -> Result<(), CoreError>;

    /// 列出所有发布
    async fn list(&self) -> Result<Vec<Rollout>, CoreError>;

    /// 获取所有活动的灰度发布（需要自动化处理）
    async fn list_active(&self) -> Result<Vec<Rollout>, CoreError>;
}

/// 文件仓储接口（仅保留上传/下载/查询/删除/应用）
#[async_trait]
pub trait FileRepository: Send + Sync {
    /// 上传文件
    async fn upload(&self, name: &str, data: Vec<u8>) -> Result<FileUploadResult, CoreError>;

    /// 获取文件信息
    async fn get_info(&self, name: &str) -> Result<Option<FileInfo>, CoreError>;

    /// 清空对象存储
    async fn clear_all(&self) -> Result<u64, CoreError>;
}

/// Agent 配置仓储接口
#[async_trait]
pub trait AgentConfigRepository: Send + Sync {
    /// 批量应用配置（已扁平化 K/V）到多个 agent
    async fn apply_bulk(
        &self,
        agent_ids: &[String],
        flat_kv: &std::collections::HashMap<String, String>,
    ) -> Result<u64, CoreError>;

    /// 获取单个配置值
    async fn get(&self, agent_id: &str, key: &str) -> Result<Option<String>, CoreError>;

    /// 设置单个配置值
    async fn set(&self, agent_id: &str, key: &str, value: &str) -> Result<(), CoreError>;

    /// 删除单个配置值
    async fn del(&self, agent_id: &str, key: &str) -> Result<(), CoreError>;

    /// 列出 agent 的所有配置键
    async fn list_keys(
        &self,
        agent_id: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, CoreError>;

    /// 获取 agent 的所有配置
    async fn get_all(
        &self,
        agent_id: &str,
    ) -> Result<std::collections::HashMap<String, String>, CoreError>;

    // 批量清空能力由明确的用例/RPC驱动时再暴露
}
