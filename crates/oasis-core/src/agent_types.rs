//! oasis-core/src/agent_types.rs - 统一的Agent相关类型

use crate::core_types::AgentId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub id: AgentId,
    pub status: AgentStatus,
    pub info: HashMap<String, String>, // 合并facts和labels和 groups
    pub last_heartbeat: i64,
    pub version: String,
    pub capabilities: Vec<String>,
}

impl AgentInfo {
    pub fn new(id: AgentId) -> Self {
        Self {
            id,
            status: AgentStatus::Offline,
            info: HashMap::new(),
            last_heartbeat: 0,
            version: String::new(),
            capabilities: Vec::new(),
        }
    }

    pub fn online(mut self) -> Self {
        self.status = AgentStatus::Online;
        self.last_heartbeat = chrono::Utc::now().timestamp();
        self
    }

    pub fn with_info(mut self, key: String, value: String) -> Self {
        self.info.insert(key, value);
        self
    }

    pub fn with_capability(mut self, capability: String) -> Self {
        self.capabilities.push(capability);
        self
    }
}

/// 强类型的Agent状态 - 替代所有String状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentStatus {
    Online,
    Offline,
    Removed,
}

impl AgentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Offline => "offline",
            Self::Removed => "removed",
        }
    }
}

impl From<String> for AgentStatus {
    fn from(s: String) -> Self {
        match s.to_lowercase().as_str() {
            "online" => Self::Online,
            "offline" => Self::Offline,
            "removed" => Self::Removed,
            _ => Self::Removed,
        }
    }
}

