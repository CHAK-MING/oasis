// JetStream 资源命名（避免使用不被支持的字符，如 '.' 和 '_'）
pub const JS_STREAM_TASKS: &str = "OASIS-TASKS"; // subjects: tasks.exec.>
pub const JS_STREAM_RESULTS: &str = "OASIS-RESULTS"; // subjects: results.>
pub const JS_STREAM_FILES: &str = "OASIS-FILES"; // subjects: files.>

// KV 存储结构重构 - 分离不同生命周期的数据
// 新架构：三个独立的 KV buckets，支持独立的 TTL 和版本管理
pub const JS_KV_AGENT_INFOS: &str = "OASIS-AGENT-INFOS"; // facts (版本化，非TTL)
pub const JS_KV_AGENT_HEARTBEAT: &str = "OASIS-AGENT-HB"; // heartbeat (TTL=2x心跳)
pub const JS_KV_AGENT_LABELS: &str = "OASIS-AGENT-LABELS"; // labels (Server/CLI可变更)


// Object Store（文件分发）
// 使用下划线命名以避免某些部署对连字符的限制
pub const JS_OBJ_ARTIFACTS: &str = "OASIS_ARTIFACTS";
/// 灰度发布 Rollouts KV 存储
pub const JS_KV_ROLLOUTS: &str = "OASIS-ROLLOUTS";

// ---------- Stream duplicate window 策略（统一管理） ----------
/// TASKS 流去重窗口（秒）
pub const DUPLICATE_WINDOW_TASKS_SECS: u64 = 30;
/// RESULTS 流去重窗口（秒）
pub const DUPLICATE_WINDOW_RESULTS_SECS: u64 = 60;

// 内置命令（由 Agent 内部处理，不通过外部进程）
pub const CMD_LABELS_UPDATE: &str = "oasis:labels-update"; // args: JSON or k=v pairs

// - 默认：tasks.exec.default（无明确目标时）
// - 单播：tasks.exec.agent.<agentId>

pub const TASKS_FILTER_SUBJECT: &str = "tasks.exec.>"; // Agent 侧各消费者使用更精确的 filter_subject
pub const TASKS_PUBLISH_SUBJECT: &str = "tasks.exec.default"; // Server 无目标时的默认 subject

// 结果主题前缀（最终形如：results.<taskId>.<agentId>）
pub const RESULTS_SUBJECT_PREFIX: &str = "results";

pub const FILES_SUBJECT_PREFIX: &str = "files";


// 统一管理的消费者命名常量
pub const DEFAULT_CONSUMER_NAME: &str = "oasis-workers-default-new";
pub const UNICAST_CONSUMER_PREFIX_VERSION: &str = "v2";
pub const UNICAST_CONSUMER_PREFIX: &str = "oasis-agent-"; // 最终名称将携带版本

use crate::core_types::{AgentId, TaskId};

/// 输入验证模块
pub mod validation {
    /// 验证Agent ID格式
    pub fn validate_agent_id(agent_id: &str) -> Result<(), String> {
        if agent_id.is_empty() {
            return Err("Agent ID cannot be empty".to_string());
        }

        if agent_id.len() > 255 {
            return Err(format!(
                "Agent ID too long ({} chars, max 255)",
                agent_id.len()
            ));
        }

        if !agent_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(
                "Agent ID contains invalid characters (only alphanumeric, -, _ allowed)"
                    .to_string(),
            );
        }

        if agent_id.starts_with('-') || agent_id.ends_with('-') {
            return Err("Agent ID cannot start or end with '-'".to_string());
        }

        Ok(())
    }

    /// 验证Task ID格式
    pub fn validate_task_id(task_id: &str) -> Result<(), String> {
        if task_id.is_empty() {
            return Err("Task ID cannot be empty".to_string());
        }

        if task_id.len() > 255 {
            return Err(format!(
                "Task ID too long ({} chars, max 255)",
                task_id.len()
            ));
        }

        // Task ID通常是UUID格式，但我们也允许其他格式
        if !task_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
        {
            return Err(
                "Task ID contains invalid characters (only alphanumeric, - allowed)".to_string(),
            );
        }

        Ok(())
    }

    /// 验证文件路径
    pub fn validate_file_path(path: &str) -> Result<(), String> {
        if path.is_empty() {
            return Err("File path cannot be empty".to_string());
        }

        if path.len() > 4096 {
            return Err(format!(
                "File path too long ({} chars, max 4096)",
                path.len()
            ));
        }

        Ok(())
    }

    /// 验证命令
    pub fn validate_command(command: &str) -> Result<(), String> {
        if command.is_empty() {
            return Err("Command cannot be empty".to_string());
        }

        if command.len() > 1024 {
            return Err(format!(
                "Command too long ({} chars, max 1024)",
                command.len()
            ));
        }

        // 检查危险命令
        let dangerous_commands = ["rm -rf", "sudo", "su ", "chmod 777", "dd if="];
        for dangerous in &dangerous_commands {
            if command.contains(dangerous) {
                return Err(format!(
                    "Command contains potentially dangerous pattern: {}",
                    dangerous
                ));
            }
        }

        Ok(())
    }
}

/// 生成任务结果 subject：results.<taskId>.<agentId>
/// 请优先使用 `result_subject_for_typed`，该函数仅作为兼容保留
pub fn result_subject_for(task_id: &TaskId, agent_id: &AgentId) -> String {
    result_subject_for_typed(task_id, agent_id)
}

/// 生成任务结果 subject：results.<taskId>.<agentId>（类型安全版本）
pub fn result_subject_for_typed(task_id: &TaskId, agent_id: &AgentId) -> String {
    format!(
        "{RESULTS_SUBJECT_PREFIX}.{}.{}",
        task_id.as_str(),
        agent_id.as_str()
    )
}

// 删除 DLQ 相关函数 - 对当前所有命令都是冗余的

// ---------- 任务主题 helpers ----------

/// 默认工作队列 subject
pub fn tasks_default_subject() -> &'static str {
    "tasks.exec.default"
}

/// 指定 agent 的单播 subject
pub fn tasks_unicast_subject(agent_id: &AgentId) -> String {
    format!("tasks.exec.agent.{agent_id}")
}

/// 生成默认工作队列消费者名称（共享）
/// 注意：默认消费者是共享的，在工作队列流上同一个 subject 只能有一个消费者
pub fn default_consumer_name() -> &'static str {
    DEFAULT_CONSUMER_NAME
}

/// 生成单播消费者名称（每个 Agent 独立）
pub fn unicast_consumer_name(agent_id: &AgentId) -> String {
    format!(
        "{}{}-{}",
        UNICAST_CONSUMER_PREFIX,
        UNICAST_CONSUMER_PREFIX_VERSION,
        agent_id.as_str()
    )
}

// ---------- KV 键名生成 helpers ----------

/// 规范化 agent_id 为 KV 安全的键名
/// 将不安全字符替换为 '-'，确保键名为单层且通配符安全
fn normalize_agent_id_for_kv(agent_id: &str) -> String {
    use regex::Regex;
    static RE: std::sync::LazyLock<Regex> =
        std::sync::LazyLock::new(|| Regex::new(r"[^a-zA-Z0-9_-]").unwrap());
    let normalized = RE.replace_all(agent_id, "-");
    normalized.trim_matches('-').to_string()
}

/// 生成 Agent facts 键名（单层，避免通配符问题）
pub fn kv_key_facts(agent_id: &str) -> String {
    normalize_agent_id_for_kv(agent_id)
}

/// 生成 Agent heartbeat 键名（单层，避免通配符问题）
pub fn kv_key_heartbeat(agent_id: &str) -> String {
    normalize_agent_id_for_kv(agent_id)
}

/// 生成 Agent labels 键名（单层，避免通配符问题）
pub fn kv_key_labels(agent_id: &str) -> String {
    normalize_agent_id_for_kv(agent_id)
}

/// 生成任务状态 KV 键：task:state:<agentId>:<taskId>
pub fn kv_key_task_state(agent_id: &str, task_id: &str) -> String {
    format!(
        "task:state:{}:{}",
        normalize_agent_id_for_kv(agent_id),
        task_id
    )
}

/// Agent 配置键的统一前缀
pub const KV_CONFIG_AGENT_KEY_PREFIX: &str = "agent.config";

/// 生成用于匹配某 Agent 所有配置项的前缀（例如："agent.config.my-agent-id."）
/// 返回的字符串末尾包含 '.'，用于 KV 的 `keys()` 操作进行前缀扫描。
pub fn kv_config_scan_prefix(agent_id: &str) -> String {
    format!("{}.{}.", KV_CONFIG_AGENT_KEY_PREFIX, agent_id)
}

/// 生成某个具体的 Agent 配置项的完整 Key（例如："agent.config.my-agent-id.log_level"）
pub fn kv_config_key(agent_id: &str, config_name: &str) -> String {
    format!(
        "{}.{}.{}",
        KV_CONFIG_AGENT_KEY_PREFIX,
        agent_id,
        config_name.replace('.', "_")
    )
}

/// 从 CLI/上层传入的目标标记生成 subject
/// 支持："agent:<id>"、"default"、""，其他值直接作为 agent_id 处理
pub fn tasks_subject_from_target(target: &AgentId) -> Result<String, String> {
    match target.as_str() {
        "" | "default" => Ok(tasks_default_subject().to_string()),
        s if s.starts_with("agent:") => {
            let rest = &s[6..]; // "agent:".len() == 6
            if rest.is_empty() {
                Err("agent target cannot be empty".to_string())
            } else {
                Ok(tasks_unicast_subject(&AgentId::from(rest.to_string())))
            }
        }
        _ => Ok(tasks_unicast_subject(target)),
    }
}

// ---------- 目标标记 helpers（供 CLI 构造 routing targets） ----------

pub fn target_spec_agent(agent_id: &str) -> String {
    format!("agent:{agent_id}")
}

pub fn target_spec_default() -> &'static str {
    "default"
}

// 消费者名称前缀（建议按 agentId 区分）
pub const TASK_CONSUMER_PREFIX: &str = UNICAST_CONSUMER_PREFIX; // e.g. oasis-agent-<agentId>

// 默认消费者参数
pub const DEFAULT_ACK_WAIT_SECS: u64 = 30;
pub const DEFAULT_MAX_ACK_PENDING: i64 = 128;
pub const DEFAULT_FETCH_MAX_MESSAGES: usize = 128;
// fetch 空轮询超时（毫秒），避免 0 导致忙轮询；建议 1000~3000ms
pub const DEFAULT_FETCH_EXPIRES_MS: u64 = 2000;
/// 失败重试次数上限
pub const DEFAULT_MAX_DELIVER: i64 = 5;
