use chrono::{DateTime, Utc};
use oasis_core::agent::AgentFacts;
use serde::{Deserialize, Serialize};

/// 节点领域事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeEvent {
    /// 节点上线
    Online {
        node_id: String,
        timestamp: DateTime<Utc>,
    },
    /// 节点下线
    Offline {
        node_id: String,
        timestamp: DateTime<Utc>,
    },
    /// 节点心跳更新
    HeartbeatUpdated {
        node_id: String,
        timestamp: DateTime<Utc>,
    },
    /// 节点标签更新
    LabelsUpdated {
        node_id: String,
        labels: std::collections::HashMap<String, String>,
        timestamp: DateTime<Utc>,
    },
    /// 节点事实更新（结构化）
    FactsUpdated {
        node_id: String,
        facts: AgentFacts,
        timestamp: DateTime<Utc>,
    },
}
