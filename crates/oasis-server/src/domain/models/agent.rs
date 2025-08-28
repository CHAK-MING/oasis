use oasis_core::types::AgentId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Agent 心跳信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeat {
    pub timestamp: i64,
    pub version: String,
}

/// 简化后的 Agent 标签与事实：直接持有数据

/// Agent 状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AgentStatus {
    Online,
    Offline,
    Maintenance,
}

/// Agent 聚合根 - 核心业务实体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub heartbeat: AgentHeartbeat,
    pub labels: HashMap<String, String>,
    pub facts: oasis_core::agent::AgentFacts,
    pub status: AgentStatus,
    pub groups: Vec<String>,
    pub version: u64, // 乐观锁版本号
}

impl Agent {
    /// 业务规则：检查是否在线
    pub fn is_online(&self, ttl_sec: u64) -> bool {
        let now = chrono::Utc::now().timestamp();
        now - self.heartbeat.timestamp <= ttl_sec as i64
    }

    // 已移除 to_agent_info，改为在调用处直接构造 proto::AgentInfo

    /// 更新标签
    pub fn update_labels(&mut self, new_labels: HashMap<String, String>) {
        self.labels = new_labels;
        self.version += 1;
    }

    /// 更新组
    pub fn update_groups(&mut self, new_groups: Vec<String>) {
        self.groups = new_groups;
        self.version += 1;
    }

    /// 添加标签
    pub fn add_label(&mut self, key: String, value: String) {
        self.labels.insert(key, value);
        self.version += 1;
    }

    /// 移除标签
    pub fn remove_label(&mut self, key: &str) {
        self.labels.remove(key);
        self.version += 1;
    }

    /// 添加组
    pub fn add_group(&mut self, group: String) {
        if !self.groups.contains(&group) {
            self.groups.push(group);
            self.version += 1;
        }
    }

    /// 移除组
    pub fn remove_group(&mut self, group: &str) {
        self.groups.retain(|g| g != group);
        self.version += 1;
    }
}
