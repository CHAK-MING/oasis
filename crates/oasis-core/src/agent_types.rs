//! oasis-core/src/agent_types.rs - 统一的Agent相关类型

use crate::core_types::AgentId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub agent_id: AgentId,
    pub status: AgentStatus,
    pub info: HashMap<String, String>, // 合并facts和labels和 groups
    pub last_heartbeat: i64,
    pub version: String,
    pub capabilities: Vec<String>,
}

impl AgentInfo {
    pub fn new(agent_id: AgentId) -> Self {
        Self {
            agent_id,
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

/// 强类型的Agent状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentStatus {
    Online,   // 在线（心跳正常）
    Degraded, // 降级（部分服务失败，但仍在运行）
    Offline,  // 离线（心跳超时）
    Removed,  // 移除（手动移除）
    Unknown,
}

impl AgentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Degraded => "degraded",
            Self::Offline => "offline",
            Self::Removed => "removed",
            Self::Unknown => "unknown",
        }
    }
}

impl From<String> for AgentStatus {
    fn from(s: String) -> Self {
        match s.to_lowercase().as_str() {
            "online" => Self::Online,
            "degraded" => Self::Degraded,
            "offline" => Self::Offline,
            "removed" => Self::Removed,
            _ => Self::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_info_new() {
        let agent_id = AgentId::new("test-agent");
        let info = AgentInfo::new(agent_id.clone());

        assert_eq!(info.agent_id.as_str(), "test-agent");
        assert_eq!(info.status, AgentStatus::Offline);
        assert!(info.info.is_empty());
        assert_eq!(info.last_heartbeat, 0);
        assert!(info.version.is_empty());
        assert!(info.capabilities.is_empty());
    }

    #[test]
    fn test_agent_info_online() {
        let agent_id = AgentId::new("test-agent");
        let info = AgentInfo::new(agent_id).online();

        assert_eq!(info.status, AgentStatus::Online);
        assert!(info.last_heartbeat > 0);
    }

    #[test]
    fn test_agent_info_with_info() {
        let agent_id = AgentId::new("test-agent");
        let info = AgentInfo::new(agent_id)
            .with_info("hostname".to_string(), "server1".to_string())
            .with_info("env".to_string(), "prod".to_string());

        assert_eq!(info.info.len(), 2);
        assert_eq!(info.info.get("hostname"), Some(&"server1".to_string()));
        assert_eq!(info.info.get("env"), Some(&"prod".to_string()));
    }

    #[test]
    fn test_agent_info_with_capability() {
        let agent_id = AgentId::new("test-agent");
        let info = AgentInfo::new(agent_id)
            .with_capability("exec".to_string())
            .with_capability("file".to_string());

        assert_eq!(info.capabilities.len(), 2);
        assert!(info.capabilities.contains(&"exec".to_string()));
        assert!(info.capabilities.contains(&"file".to_string()));
    }

    #[test]
    fn test_agent_info_builder_chain() {
        let agent_id = AgentId::new("test-agent");
        let info = AgentInfo::new(agent_id)
            .online()
            .with_info("role".to_string(), "web".to_string())
            .with_capability("exec".to_string());

        assert_eq!(info.status, AgentStatus::Online);
        assert_eq!(info.info.len(), 1);
        assert_eq!(info.capabilities.len(), 1);
    }

    #[test]
    fn test_agent_status_as_str() {
        assert_eq!(AgentStatus::Online.as_str(), "online");
        assert_eq!(AgentStatus::Degraded.as_str(), "degraded");
        assert_eq!(AgentStatus::Offline.as_str(), "offline");
        assert_eq!(AgentStatus::Removed.as_str(), "removed");
        assert_eq!(AgentStatus::Unknown.as_str(), "unknown");
    }

    #[test]
    fn test_agent_status_from_string() {
        assert_eq!(AgentStatus::from("online".to_string()), AgentStatus::Online);
        assert_eq!(AgentStatus::from("ONLINE".to_string()), AgentStatus::Online);
        assert_eq!(AgentStatus::from("Online".to_string()), AgentStatus::Online);
        assert_eq!(
            AgentStatus::from("degraded".to_string()),
            AgentStatus::Degraded
        );
        assert_eq!(
            AgentStatus::from("offline".to_string()),
            AgentStatus::Offline
        );
        assert_eq!(
            AgentStatus::from("removed".to_string()),
            AgentStatus::Removed
        );
        assert_eq!(
            AgentStatus::from("invalid".to_string()),
            AgentStatus::Unknown
        );
        assert_eq!(AgentStatus::from("".to_string()), AgentStatus::Unknown);
    }

    #[test]
    fn test_agent_status_equality() {
        assert_eq!(AgentStatus::Online, AgentStatus::Online);
        assert_ne!(AgentStatus::Online, AgentStatus::Offline);
    }

    #[test]
    fn test_agent_info_serialization() {
        let agent_id = AgentId::new("test-agent");
        let info = AgentInfo::new(agent_id)
            .online()
            .with_info("key".to_string(), "value".to_string());

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: AgentInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.agent_id.as_str(), "test-agent");
        assert_eq!(deserialized.status, AgentStatus::Online);
    }
}
