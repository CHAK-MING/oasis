use crate::type_defs::AgentId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AgentStatus {
    Online,
    Offline,
    Busy,
}

/// Agent Facts - 系统信息（长期保存，版本化）
/// 只包含较稳定的字段，避免高频变化导致写放大
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentFacts {
    pub agent_id: AgentId,
    pub hostname: String,
    pub primary_ip: String, // 主要 IP 地址

    // 硬件信息
    pub cpu_arch: String,
    pub cpu_cores: u32,
    pub memory_total_bytes: u64,

    // 操作系统信息
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub boot_id: String,

    // 网络信息
    pub network_interfaces: Vec<NetworkInterface>,
    pub cidrs: Vec<String>,


    // Agent 版本信息
    pub agent_version: String,

    // 元数据
    pub collected_at: i64, // Unix 时间戳
}

impl AgentFacts {
    /// Basic sanity validation to prevent clearly invalid values from propagating
    pub fn validate_basic(&self) -> Result<(), crate::error::CoreError> {
        if self.cpu_cores == 0 {
            return Err(crate::error::CoreError::InvalidTask {
                reason: "cpu_cores must be greater than 0".to_string(),
            });
        }
        if self.memory_total_bytes == 0 {
            return Err(crate::error::CoreError::InvalidTask {
                reason: "memory_total_bytes must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

/// 网络接口信息
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkInterface {
    pub name: String,
    pub mac: Option<String>,
    pub ipv4: Vec<String>,
    pub ipv6: Vec<String>,
}

/// Agent Labels - 用户标签（长期保存，可由 Server/CLI 变更）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLabels {
    pub agent_id: AgentId,
    pub labels: HashMap<String, String>, // env=prod, role=worker 等
    pub updated_at: i64,                 // Unix 时间戳
    pub updated_by: String,              // 更新者标识
}

/// Agent Heartbeat - 心跳信息（短 TTL）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeat {
    pub agent_id: AgentId,
    pub status: AgentStatus,
    pub last_seen: i64, // Unix 时间戳
    pub sequence: u64,  // 心跳序列号，用于检测重复或丢失
}
