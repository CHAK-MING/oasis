use oasis_core::selector::NodeAttributes;
use oasis_core::types::AgentId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 节点心跳信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub timestamp: i64,
    pub version: String,
}

/// 节点标签
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLabels {
    pub labels: HashMap<String, String>,
    pub version: u64,
}

/// 节点事实信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeFacts {
    pub facts: oasis_core::agent::AgentFacts,
    pub version: u64,
}

/// 节点状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Online,
    Offline,
    Maintenance,
}

/// 节点聚合根 - 核心业务实体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: AgentId,
    pub heartbeat: NodeHeartbeat,
    pub labels: NodeLabels,
    pub facts: NodeFacts,
    pub status: NodeStatus,
    pub groups: Vec<String>,
    pub version: u64, // 乐观锁版本号
}

impl Node {
    /// 业务规则：检查是否在线
    pub fn is_online(&self, ttl_sec: u64) -> bool {
        let now = chrono::Utc::now().timestamp();
        now - self.heartbeat.timestamp <= ttl_sec as i64
    }

    /// 将 Node 转换为选择器可用的属性结构
    pub fn to_attributes(&self) -> NodeAttributes {
        let groups: Vec<String> = self.groups.clone();

        // 将结构化 facts 的部分字段映射为可被选择器使用的自定义键
        let mut custom_facts: HashMap<String, String> = HashMap::new();
        custom_facts.insert("hostname".to_string(), self.facts.facts.hostname.clone());
        custom_facts.insert(
            "primary_ip".to_string(),
            self.facts.facts.primary_ip.clone(),
        );
        custom_facts.insert("os_name".to_string(), self.facts.facts.os_name.clone());
        custom_facts.insert(
            "os_version".to_string(),
            self.facts.facts.os_version.clone(),
        );

        NodeAttributes {
            id: self.id.clone(),
            labels: self.labels.labels.clone(),
            groups,
            version: self.heartbeat.version.clone(),
            custom: custom_facts,
        }
    }
}
