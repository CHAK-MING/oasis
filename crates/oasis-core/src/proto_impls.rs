use std::fmt;

// ===== 为 proto 强类型实现必要的 trait =====

impl fmt::Display for crate::proto::AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl fmt::Display for crate::proto::TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl fmt::Display for crate::proto::RolloutId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

// ===== 转换工具 =====

impl From<String> for crate::proto::AgentId {
    fn from(value: String) -> Self {
        Self { value }
    }
}

impl From<&str> for crate::proto::AgentId {
    fn from(value: &str) -> Self {
        Self {
            value: value.to_string(),
        }
    }
}

impl From<String> for crate::proto::TaskId {
    fn from(value: String) -> Self {
        Self { value }
    }
}

impl From<&str> for crate::proto::TaskId {
    fn from(value: &str) -> Self {
        Self {
            value: value.to_string(),
        }
    }
}

impl From<String> for crate::proto::RolloutId {
    fn from(value: String) -> Self {
        Self { value }
    }
}

impl From<&str> for crate::proto::RolloutId {
    fn from(value: &str) -> Self {
        Self {
            value: value.to_string(),
        }
    }
}

// ===== 解包工具 =====

impl crate::proto::AgentId {
    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn to_string(&self) -> String {
        self.value.clone()
    }
}

impl crate::proto::TaskId {
    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn to_string(&self) -> String {
        self.value.clone()
    }
}

impl crate::proto::RolloutId {
    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn to_string(&self) -> String {
        self.value.clone()
    }
}

// ===== AgentFacts/Heartbeat conversions =====

impl From<&crate::agent::NetworkInterface> for crate::proto::NetworkInterfaceMsg {
    fn from(n: &crate::agent::NetworkInterface) -> Self {
        Self {
            name: n.name.clone(),
            mac: n.mac.clone(),
            ipv4: n.ipv4.clone(),
            ipv6: n.ipv6.clone(),
        }
    }
}

impl From<&crate::agent::AgentFacts> for crate::proto::AgentFacts {
    fn from(f: &crate::agent::AgentFacts) -> Self {
        Self {
            agent_id: Some(crate::proto::AgentId {
                value: f.agent_id.to_string(),
            }),
            hostname: f.hostname.clone(),
            primary_ip: f.primary_ip.clone(),
            cpu_arch: f.cpu_arch.clone(),
            cpu_cores: f.cpu_cores,
            memory_total_bytes: f.memory_total_bytes,
            os_name: f.os_name.clone(),
            os_version: f.os_version.clone(),
            kernel_version: f.kernel_version.clone(),
            boot_id: f.boot_id.clone(),
            network_interfaces: f.network_interfaces.iter().map(Into::into).collect(),
            cidrs: f.cidrs.clone(),
            agent_version: f.agent_version.clone(),
            collected_at: f.collected_at,
        }
    }
}

impl From<&crate::proto::NetworkInterfaceMsg> for crate::agent::NetworkInterface {
    fn from(p: &crate::proto::NetworkInterfaceMsg) -> Self {
        Self {
            name: p.name.clone(),
            mac: p.mac.clone(),
            ipv4: p.ipv4.clone(),
            ipv6: p.ipv6.clone(),
        }
    }
}

impl From<&crate::proto::AgentFacts> for crate::agent::AgentFacts {
    fn from(p: &crate::proto::AgentFacts) -> Self {
        Self {
            agent_id: p
                .agent_id
                .as_ref()
                .map(|id| id.value.clone().into())
                .unwrap_or_else(|| "".into()),
            hostname: p.hostname.clone(),
            primary_ip: p.primary_ip.clone(),
            cpu_arch: p.cpu_arch.clone(),
            cpu_cores: p.cpu_cores,
            memory_total_bytes: p.memory_total_bytes,
            os_name: p.os_name.clone(),
            os_version: p.os_version.clone(),
            kernel_version: p.kernel_version.clone(),
            boot_id: p.boot_id.clone(),
            network_interfaces: p.network_interfaces.iter().map(Into::into).collect(),
            cidrs: p.cidrs.clone(),
            agent_version: p.agent_version.clone(),
            collected_at: p.collected_at,
        }
    }
}

impl From<&crate::agent::AgentHeartbeat> for crate::proto::AgentHeartbeat {
    fn from(h: &crate::agent::AgentHeartbeat) -> Self {
        let status = match h.status {
            crate::agent::AgentStatus::Online => crate::proto::AgentStatusEnum::AgentOnline as i32,
            crate::agent::AgentStatus::Offline => {
                crate::proto::AgentStatusEnum::AgentOffline as i32
            }
            crate::agent::AgentStatus::Busy => crate::proto::AgentStatusEnum::AgentBusy as i32,
        };
        Self {
            agent_id: Some(crate::proto::AgentId {
                value: h.agent_id.to_string(),
            }),
            status,
            last_seen: h.last_seen,
            sequence: h.sequence,
        }
    }
}

impl From<&crate::proto::AgentHeartbeat> for crate::agent::AgentHeartbeat {
    fn from(p: &crate::proto::AgentHeartbeat) -> Self {
        let status = match p.status {
            x if x == crate::proto::AgentStatusEnum::AgentOnline as i32 => {
                crate::agent::AgentStatus::Online
            }
            x if x == crate::proto::AgentStatusEnum::AgentOffline as i32 => {
                crate::agent::AgentStatus::Offline
            }
            _ => crate::agent::AgentStatus::Busy,
        };
        Self {
            agent_id: p
                .agent_id
                .as_ref()
                .map(|id| id.value.clone().into())
                .unwrap_or_else(|| "".into()),
            status,
            last_seen: p.last_seen,
            sequence: p.sequence,
        }
    }
}

pub mod encoding {
    use prost::Message;
    pub fn to_vec<M: Message>(m: &M) -> Vec<u8> {
        M::encode_to_vec(m)
    }
    pub fn from_slice<M: Message + Default>(buf: &[u8]) -> Result<M, prost::DecodeError> {
        M::decode(buf)
    }
}

// ===== TaskExecution conversions =====
impl From<&crate::task::TaskExecution> for crate::proto::TaskExecutionMsg {
    fn from(t: &crate::task::TaskExecution) -> Self {
        Self {
            task_id: Some(crate::proto::TaskId {
                value: t.task_id.to_string(),
            }),
            agent_id: Some(crate::proto::AgentId {
                value: t.agent_id.to_string(),
            }),
            stdout: t.stdout.clone(),
            stderr: t.stderr.clone(),
            exit_code: t.exit_code.unwrap_or(-1),
            timestamp: t.timestamp,
            duration_ms: t.duration_ms,
        }
    }
}

// ===== AgentLabels conversions =====
impl From<&crate::agent::AgentLabels> for crate::proto::AgentLabels {
    fn from(l: &crate::agent::AgentLabels) -> Self {
        Self {
            agent_id: Some(crate::proto::AgentId {
                value: l.agent_id.to_string(),
            }),
            labels: l.labels.clone(),
            updated_at: l.updated_at,
            updated_by: l.updated_by.clone(),
        }
    }
}

// ===== TaskSpec conversions =====
impl From<&crate::task::TaskSpec> for crate::proto::TaskSpecMsg {
    fn from(t: &crate::task::TaskSpec) -> Self {
        use crate::proto::{TaskTargetMsg, task_target_msg::Target as ProtoTaskTarget};
        let selector_str = match &t.target {
            crate::task::TaskTarget::Selector(s) => s.clone(),
            crate::task::TaskTarget::Agents(v) => {
                let list = v
                    .iter()
                    .map(|id| format!("\"{}\"", id))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("agent_id in [{}]", list)
            }
            crate::task::TaskTarget::AllAgents => "true".to_string(),
        };
        let target_msg = TaskTargetMsg {
            target: Some(ProtoTaskTarget::Selector(selector_str)),
        };
        crate::proto::TaskSpecMsg {
            id: Some(crate::proto::TaskId {
                value: t.id.to_string(),
            }),
            command: t.command.clone(),
            args: t.args.clone(),
            env: t.env.clone(),
            target: Some(target_msg),
            timeout_seconds: t.timeout_seconds,
        }
    }
}

impl From<&crate::proto::TaskSpecMsg> for crate::task::TaskSpec {
    fn from(p: &crate::proto::TaskSpecMsg) -> Self {
        let id =
            p.id.as_ref()
                .map(|x| x.value.clone().into())
                .unwrap_or_else(|| "".into());
        let target = match p.target.as_ref().and_then(|t| t.target.as_ref()) {
            Some(crate::proto::task_target_msg::Target::Selector(s)) => {
                crate::task::TaskTarget::Selector(s.clone())
            }
            None => crate::task::TaskTarget::Selector(String::new()),
        };
        crate::task::TaskSpec {
            id,
            command: p.command.clone(),
            args: p.args.clone(),
            env: p.env.clone(),
            target,
            timeout_seconds: p.timeout_seconds,
        }
    }
}

impl From<&crate::proto::AgentLabels> for crate::agent::AgentLabels {
    fn from(p: &crate::proto::AgentLabels) -> Self {
        Self {
            agent_id: p
                .agent_id
                .as_ref()
                .map(|id| id.value.clone().into())
                .unwrap_or_else(|| "".into()),
            labels: p.labels.clone(),
            updated_at: p.updated_at,
            updated_by: p.updated_by.clone(),
        }
    }
}

impl From<&crate::proto::TaskExecutionMsg> for crate::task::TaskExecution {
    fn from(p: &crate::proto::TaskExecutionMsg) -> Self {
        Self {
            task_id: p
                .task_id
                .as_ref()
                .map(|id| id.value.clone().into())
                .unwrap_or_else(|| "".into()),
            agent_id: p
                .agent_id
                .as_ref()
                .map(|id| id.value.clone().into())
                .unwrap_or_else(|| "".into()),
            stdout: p.stdout.clone(),
            stderr: p.stderr.clone(),
            exit_code: Some(p.exit_code),
            timestamp: p.timestamp,
            duration_ms: p.duration_ms,
        }
    }
}

// ===== DLQ conversions =====
impl From<&crate::dlq::DeadLetterEntry> for crate::proto::DeadLetterEntryMsg {
    fn from(e: &crate::dlq::DeadLetterEntry) -> Self {
        Self {
            task: Some((&e.task).into()),
            error: e.error.clone(),
            agent_id: Some(crate::proto::AgentId {
                value: e.agent_id.to_string(),
            }),
            retry_count: e.retry_count,
            timestamp: e.timestamp.unix_timestamp(),
        }
    }
}

impl From<&crate::proto::DeadLetterEntryMsg> for crate::dlq::DeadLetterEntry {
    fn from(p: &crate::proto::DeadLetterEntryMsg) -> Self {
        let task = crate::task::TaskSpec::from(p.task.as_ref().unwrap());
        Self {
            task,
            error: p.error.clone(),
            agent_id: p
                .agent_id
                .as_ref()
                .map(|id| id.value.clone().into())
                .unwrap_or_else(|| "".into()),
            retry_count: p.retry_count,
            timestamp: time::OffsetDateTime::from_unix_timestamp(p.timestamp)
                .unwrap_or_else(|_| time::OffsetDateTime::UNIX_EPOCH),
        }
    }
}
