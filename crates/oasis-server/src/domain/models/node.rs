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
    pub facts: String, // JSON 格式的事实信息
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
        (now - self.heartbeat.timestamp) < ttl_sec as i64
    }

    /// 将 Node 转换为 NodeAttributes 以供选择器引擎使用。
    pub fn to_attributes(&self) -> NodeAttributes {
        // 统一：environment/region 仅存在于 labels；groups 独立字段
        let groups = self.groups.clone();

        // 自定义事实是 agent 报告的所有其他事实。
        let custom_facts: HashMap<String, String> =
            serde_json::from_str(&self.facts.facts).unwrap_or_default();

        NodeAttributes {
            id: self.id.clone(),
            labels: self.labels.labels.clone(),
            groups,
            version: self.heartbeat.version.clone(),
            custom: custom_facts,
        }
    }

    /// 从部分数据构造 NodeAttributes，适用于特定场景。
    pub fn attributes_from_parts(
        id: &AgentId,
        labels: Option<&HashMap<String, String>>,
        facts: Option<&HashMap<String, String>>,
        version: Option<&str>,
    ) -> NodeAttributes {
        let labels_map = labels.cloned().unwrap_or_default();
        let groups: Vec<String> = Vec::new();

        NodeAttributes {
            id: id.clone(),
            labels: labels_map,
            groups,
            version: version.unwrap_or("").to_string(),
            custom: facts.cloned().unwrap_or_default(),
        }
    }
}
