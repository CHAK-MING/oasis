use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

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
    /// 节点事实更新
    FactsUpdated {
        node_id: String,
        facts: String,
        timestamp: DateTime<Utc>,
    },
}

