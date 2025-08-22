use super::status::AgentStatus;
use oasis_core::selector::NodeAttributes;
use oasis_core::types::AgentId;
use serde::{Deserialize, Serialize};

/// Agent 核心领域实体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub attributes: NodeAttributes,
    pub status: AgentStatus,
}

impl Agent {
    pub fn new(id: AgentId, attributes: NodeAttributes) -> Self {
        Self {
            id,
            attributes,
            status: AgentStatus::Initializing,
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
}
