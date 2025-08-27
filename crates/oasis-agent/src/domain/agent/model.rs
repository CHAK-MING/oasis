use super::status::AgentStatus;
use oasis_core::selector::NodeAttributes;
use oasis_core::types::{AgentId, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Agent 核心领域实体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub attributes: NodeAttributes,
    pub status: AgentStatus,
    // 已处理任务与已发送结果的去重记录（task_id -> timestamp）
    processed_tasks: HashMap<TaskId, i64>,
    sent_results: HashMap<TaskId, i64>,
}

impl Agent {
    pub fn new(id: AgentId, attributes: NodeAttributes) -> Self {
        Self {
            id,
            attributes,
            status: AgentStatus::Initializing,
            processed_tasks: HashMap::new(),
            sent_results: HashMap::new(),
        }
    }

    pub fn set_status(&mut self, status: AgentStatus) {
        self.status = status;
    }

    pub fn is_running(&self) -> bool {
        matches!(self.status, AgentStatus::Running)
    }

    pub fn can_accept_tasks(&self) -> bool {
        matches!(self.status, AgentStatus::Running)
    }

    pub fn update_attributes(&mut self, new_attributes: NodeAttributes) {
        self.attributes = new_attributes;
    }

    // ===== 幂等去重：任务处理与结果发送 =====

    /// 任务是否已处理过
    pub fn is_task_processed(&self, task_id: &TaskId) -> bool {
        self.processed_tasks.contains_key(task_id)
    }

    /// 标记任务为已处理
    pub fn mark_task_processed(&mut self, task_id: TaskId) {
        let now = chrono::Utc::now().timestamp();
        self.processed_tasks.insert(task_id, now);
    }

    /// 结果是否已发送过
    pub fn is_result_sent(&self, task_id: &TaskId) -> bool {
        self.sent_results.contains_key(task_id)
    }

    /// 标记结果为已发送
    pub fn mark_result_sent(&mut self, task_id: TaskId) {
        let now = chrono::Utc::now().timestamp();
        self.sent_results.insert(task_id, now);
    }

    /// 清理过期的去重记录，防止内存增长
    pub fn cleanup_old_records(&mut self) {
        // 记录保留时长（秒）。这里选择 24 小时，可按需调整/配置化。
        const RETENTION_SECS: i64 = 24 * 60 * 60;
        let now = chrono::Utc::now().timestamp();

        self.processed_tasks
            .retain(|_, ts| now.saturating_sub(*ts) <= RETENTION_SECS);
        self.sent_results
            .retain(|_, ts| now.saturating_sub(*ts) <= RETENTION_SECS);
    }
}
