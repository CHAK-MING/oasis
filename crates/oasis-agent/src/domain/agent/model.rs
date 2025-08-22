use super::status::AgentStatus;
use oasis_core::selector::NodeAttributes;

pub struct Agent {
    pub id: String,
    pub status: AgentStatus,
    pub attributes: NodeAttributes,
}

impl Agent {
    pub fn new(id: String, attributes: NodeAttributes) -> Self {
        Self {
            id,
            status: AgentStatus::Initializing,
            attributes,
        }
    }

    /// 将 Agent 状态转换为 Running
    pub fn start(&mut self) {
        self.status = AgentStatus::Running;
    }

    /// 将 Agent 状态转换为 Draining
    pub fn drain(&mut self) {
        self.status = AgentStatus::Draining;
    }

    /// 将 Agent 状态转换为 Shutdown
    pub fn shutdown(&mut self) {
        self.status = AgentStatus::Shutdown;
    }

    /// 检查 Agent 是否可以接受新任务
    pub fn can_accept_tasks(&self) -> bool {
        matches!(self.status, AgentStatus::Running)
    }

    /// 更新 attributes（整体替换）
    pub fn update_attributes(&mut self, new_attributes: NodeAttributes) {
        self.attributes = new_attributes;
    }
}
