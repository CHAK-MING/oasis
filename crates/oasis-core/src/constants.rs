// JetStream 资源命名（避免使用不被支持的字符，如 '.' 和 '_'）
pub const JS_STREAM_TASKS: &str = "OASIS-TASKS"; // subjects: tasks.exec.>
pub const JS_STREAM_RESULTS: &str = "OASIS-RESULTS"; // subjects: results.>
pub const JS_STREAM_TASKS_DLQ: &str = "OASIS-TASKS-DLQ"; // subjects: tasks.dlq.>

pub const JS_STREAM_CONFIG: &str = "OASIS-CONFIG"; // 配置更新Stream

// KV 存储结构重构 - 分离不同生命周期的数据
// 新架构：三个独立的 KV buckets，支持独立的 TTL 和版本管理
pub const JS_KV_NODE_FACTS: &str = "OASIS-NODE-FACTS"; // facts (版本化，非TTL)
pub const JS_KV_NODE_HEARTBEAT: &str = "OASIS-NODE-HB"; // heartbeat (TTL=2x心跳)
pub const JS_KV_NODE_LABELS: &str = "OASIS-NODE-LABELS"; // labels (Server/CLI可变更)
// 增量属性更新（JSON Patch）主题前缀
pub const NATS_ATTRIBUTES_PATCH_SUBJECT_PREFIX: &str = "agent.attributes.patch";

// 未来扩展的 KV buckets
pub const JS_KV_LEADER: &str = "OASIS-LEADER"; // 选主协调
pub const JS_KV_CONFIG: &str = "OASIS-CONFIG"; // 统一配置管理

// Object Store（文件分发）
pub const JS_OBJ_ARTIFACTS: &str = "OASIS-ARTIFACTS";

// 内置命令（由 Agent 内部处理，不通过外部进程）
pub const CMD_FILE_APPLY: &str = "oasis:file-apply";
pub const CMD_LABELS_UPDATE: &str = "oasis:labels-update"; // args: JSON or k=v pairs

// - 默认：tasks.exec.default（无明确目标时）
// - 单播：tasks.exec.agent.<agentId>

pub const TASKS_FILTER_SUBJECT: &str = "tasks.exec.>"; // Agent 侧各消费者使用更精确的 filter_subject
pub const TASKS_PUBLISH_SUBJECT: &str = "tasks.exec.default"; // Server 无目标时的默认 subject

// 结果主题前缀（最终形如：results.<taskId>.<agentId>）
pub const RESULTS_SUBJECT_PREFIX: &str = "results";
pub const TASKS_DLQ_SUBJECT_PREFIX: &str = "tasks.dlq";

// 统一管理的消费者命名常量（避免散落的硬编码）
pub const DEFAULT_CONSUMER_NAME: &str = "oasis-workers-default-new";
pub const UNICAST_CONSUMER_PREFIX_VERSION: &str = "v2";
pub const UNICAST_CONSUMER_PREFIX: &str = "oasis-agent-"; // 最终名称将携带版本

/// 生成任务结果 subject：results.<taskId>.<agentId>
/// 请优先使用 `result_subject_for_typed`，该函数仅作为兼容保留
pub fn result_subject_for(task_id: &str, agent_id: &str) -> String {
    result_subject_for_typed(
        &crate::type_defs::TaskId::from(task_id),
        &crate::type_defs::AgentId::from(agent_id),
    )
}

/// 生成任务结果 subject：results.<taskId>.<agentId>（类型安全版本）
pub fn result_subject_for_typed(
    task_id: &crate::type_defs::TaskId,
    agent_id: &crate::type_defs::AgentId,
) -> String {
    format!(
        "{RESULTS_SUBJECT_PREFIX}.{}.{}",
        task_id.as_str(),
        agent_id.as_str()
    )
}

/// 生成 DLQ subject：tasks.dlq.<key>
pub fn dlq_subject_for(key: &str) -> String {
    format!("{TASKS_DLQ_SUBJECT_PREFIX}.{key}")
}

// ---------- 任务主题 helpers ----------

/// 默认工作队列 subject
pub fn tasks_default_subject() -> &'static str {
    "tasks.exec.default"
}

/// 指定 agent 的单播 subject
pub fn tasks_unicast_subject(agent_id: &str) -> String {
    format!("tasks.exec.agent.{agent_id}")
}

/// 生成默认工作队列消费者名称（共享）
/// 注意：默认消费者是共享的，在工作队列流上同一个 subject 只能有一个消费者
/// 使用新名称以支持 DeliverPolicy 更新（从 All 改为 New）
pub fn default_consumer_name() -> &'static str {
    DEFAULT_CONSUMER_NAME
}

/// 生成单播消费者名称（每个 Agent 独立）
pub fn unicast_consumer_name(agent_id: &str) -> String {
    format!(
        "{}{}-{}",
        UNICAST_CONSUMER_PREFIX, UNICAST_CONSUMER_PREFIX_VERSION, agent_id
    ) // 统一版本位与前缀，避免散落硬编码
}

// ---------- KV 键名生成 helpers ----------

/// 规范化 agent_id 为 KV 安全的键名
/// 将不安全字符替换为 '-'，确保键名为单层且通配符安全
fn normalize_agent_id_for_kv(agent_id: &str) -> String {
    agent_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
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

/// Agent 配置键的统一前缀
pub const KV_CONFIG_AGENT_KEY_PREFIX: &str = "agent.config";

/// 生成用于匹配某 Agent 所有配置项的前缀（例如："agent.config.my-agent-id."）
/// 返回的字符串末尾包含 '.'，用于 KV 的 `keys()` 操作进行前缀扫描。
pub fn kv_config_scan_prefix(agent_id: &str) -> String {
    format!("{}.{}.", KV_CONFIG_AGENT_KEY_PREFIX, agent_id)
}

/// 生成某个具体的 Agent 配置项的完整 Key（例如："agent.config.my-agent-id.log_level"）
pub fn kv_config_key(agent_id: &str, config_name: &str) -> String {
    // 规范化配置名称，将点号替换为下划线以避免键名层级冲突
    let normalized_config_name = config_name.replace('.', "_");
    let key = format!(
        "{}.{}.{}",
        KV_CONFIG_AGENT_KEY_PREFIX, agent_id, normalized_config_name
    );
    tracing::debug!(
        "Generated config key: '{}' from agent_id: '{}', config_name: '{}'",
        key,
        agent_id,
        config_name
    );
    key
}

/// 从 CLI/上层传入的目标标记生成 subject
/// 支持："agent:<id>"、"default"、""，其他值直接作为 agent_id 处理
pub fn tasks_subject_from_target(target: &str) -> Result<String, String> {
    if let Some(rest) = target.strip_prefix("agent:") {
        if rest.is_empty() {
            return Err("agent target cannot be empty".to_string());
        }
        Ok(tasks_unicast_subject(rest))
    } else if target == "default" || target.is_empty() {
        Ok(tasks_default_subject().to_string())
    } else {
        // 直接作为 agent_id 处理
        Ok(tasks_unicast_subject(target))
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
/// 失败重试次数上限（达到后进入 DLQ）
pub const DEFAULT_MAX_DELIVER: i64 = 5;
