use chrono::{DateTime, Utc};
use oasis_core::{agent::AgentFacts, types::AgentId};
use serde::{Deserialize, Serialize};

/// Agent 领域事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentEvent {
    /// Agent 上线
    Online {
        agent_id: AgentId,
        timestamp: DateTime<Utc>,
    },
    /// Agent 下线
    Offline {
        agent_id: AgentId,
        timestamp: DateTime<Utc>,
    },
    /// Agent 心跳更新
    HeartbeatUpdated {
        agent_id: AgentId,
        timestamp: DateTime<Utc>,
    },
    /// Agent 标签更新
    LabelsUpdated {
        agent_id: AgentId,
        labels: std::collections::HashMap<String, String>,
        timestamp: DateTime<Utc>,
    },
    /// Agent 组更新
    GroupsUpdated {
        agent_id: AgentId,
        groups: Vec<String>,
        timestamp: DateTime<Utc>,
    },
    /// Agent 事实更新（结构化）
    FactsUpdated {
        agent_id: AgentId,
        facts: AgentFacts,
        timestamp: DateTime<Utc>,
    },
}
