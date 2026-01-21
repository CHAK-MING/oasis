use crate::agent_types::AgentInfo;
use crate::agent_types::AgentStatus;
use crate::core_types::BatchId;
use crate::core_types::RolloutId;
use crate::core_types::SelectorExpression;
use crate::core_types::{AgentId, TaskId};
use crate::file_types::FileConfig;
use crate::file_types::FileOperationResult;
use crate::file_types::FileSpec;
use crate::file_types::{FileHistory, FileVersion};
use crate::proto;
use crate::proto::BatchMsg;
use crate::rollout_types::RolloutConfig;
use crate::rollout_types::RolloutStageStatus;
use crate::rollout_types::RolloutState;
use crate::rollout_types::RolloutStatus;
use crate::rollout_types::RolloutStrategy;
use crate::rollout_types::RolloutTaskType;
use crate::task_types::*;
use tracing::warn;

// ===== TaskId 转换 =====

impl From<&TaskId> for proto::TaskId {
    fn from(task_id: &TaskId) -> Self {
        Self {
            value: task_id.to_string(),
        }
    }
}

impl From<TaskId> for proto::TaskId {
    fn from(task_id: TaskId) -> Self {
        Self {
            value: task_id.into(),
        }
    }
}

impl From<proto::TaskId> for TaskId {
    fn from(proto: proto::TaskId) -> Self {
        TaskId::from(proto.value)
    }
}

impl From<&proto::TaskId> for TaskId {
    fn from(proto: &proto::TaskId) -> Self {
        TaskId::from(proto.value.clone())
    }
}

// ===== AgentId 转换 =====

impl From<&AgentId> for proto::AgentId {
    fn from(agent_id: &AgentId) -> Self {
        Self {
            value: agent_id.to_string(),
        }
    }
}

impl From<AgentId> for proto::AgentId {
    fn from(agent_id: AgentId) -> Self {
        Self {
            value: agent_id.into(),
        }
    }
}

impl From<proto::AgentId> for AgentId {
    fn from(proto: proto::AgentId) -> Self {
        AgentId::from(proto.value)
    }
}

impl From<&proto::AgentId> for AgentId {
    fn from(proto: &proto::AgentId) -> Self {
        AgentId::from(proto.value.clone())
    }
}

impl From<&BatchId> for proto::BatchId {
    fn from(batch_id: &BatchId) -> Self {
        Self {
            value: batch_id.to_string(),
        }
    }
}

impl From<BatchId> for proto::BatchId {
    fn from(batch_id: BatchId) -> Self {
        Self {
            value: batch_id.into(),
        }
    }
}

impl From<proto::BatchId> for BatchId {
    fn from(proto: proto::BatchId) -> Self {
        BatchId::from(proto.value)
    }
}

impl From<&proto::BatchId> for BatchId {
    fn from(proto: &proto::BatchId) -> Self {
        BatchId::from(proto.value.clone())
    }
}

impl From<&RolloutId> for proto::RolloutId {
    fn from(rollout_id: &RolloutId) -> Self {
        Self {
            value: rollout_id.to_string(),
        }
    }
}

impl From<RolloutId> for proto::RolloutId {
    fn from(rollout_id: RolloutId) -> Self {
        Self {
            value: rollout_id.into(),
        }
    }
}

impl From<proto::RolloutId> for RolloutId {
    fn from(proto: proto::RolloutId) -> Self {
        RolloutId::from(proto.value)
    }
}

impl From<&proto::RolloutId> for RolloutId {
    fn from(proto: &proto::RolloutId) -> Self {
        RolloutId::from(proto.value.clone())
    }
}

impl From<&SelectorExpression> for proto::SelectorExpression {
    fn from(selector_expression: &SelectorExpression) -> Self {
        Self {
            expression: selector_expression.to_string(),
        }
    }
}

impl From<SelectorExpression> for proto::SelectorExpression {
    fn from(selector_expression: SelectorExpression) -> Self {
        Self {
            expression: selector_expression.into(),
        }
    }
}

impl From<proto::SelectorExpression> for SelectorExpression {
    fn from(proto: proto::SelectorExpression) -> Self {
        SelectorExpression::from(proto.expression)
    }
}

impl From<&proto::SelectorExpression> for SelectorExpression {
    fn from(proto: &proto::SelectorExpression) -> Self {
        SelectorExpression::from(proto.expression.clone())
    }
}

// ===== TaskState 转换 =====

impl From<TaskState> for proto::TaskStateEnum {
    fn from(state: TaskState) -> Self {
        match state {
            TaskState::Created => proto::TaskStateEnum::TaskCreated,
            TaskState::Pending => proto::TaskStateEnum::TaskPending,
            TaskState::Running => proto::TaskStateEnum::TaskRunning,
            TaskState::Success => proto::TaskStateEnum::TaskSuccess,
            TaskState::Failed => proto::TaskStateEnum::TaskFailed,
            TaskState::Timeout => proto::TaskStateEnum::TaskTimeout,
            TaskState::Cancelled => proto::TaskStateEnum::TaskCancelled,
        }
    }
}

impl From<proto::TaskStateEnum> for TaskState {
    fn from(proto: proto::TaskStateEnum) -> Self {
        match proto {
            proto::TaskStateEnum::TaskCreated => TaskState::Created,
            proto::TaskStateEnum::TaskPending => TaskState::Pending,
            proto::TaskStateEnum::TaskRunning => TaskState::Running,
            proto::TaskStateEnum::TaskSuccess => TaskState::Success,
            proto::TaskStateEnum::TaskFailed => TaskState::Failed,
            proto::TaskStateEnum::TaskTimeout => TaskState::Timeout,
            proto::TaskStateEnum::TaskCancelled => TaskState::Cancelled,
        }
    }
}

impl From<i32> for TaskState {
    fn from(value: i32) -> Self {
        match proto::TaskStateEnum::try_from(value) {
            Ok(state) => state.into(),
            Err(_) => TaskState::Created, // 默认值
        }
    }
}

// ===== TaskRequest 转换 =====

impl From<&BatchRequest> for proto::BatchRequestMsg {
    fn from(request: &BatchRequest) -> Self {
        Self {
            command: request.command.clone(),
            args: request.args.clone(),
            target: Some(proto::SelectorExpression::from(&request.selector)),
            timeout_seconds: request.timeout_seconds,
        }
    }
}

impl From<BatchRequest> for proto::BatchRequestMsg {
    fn from(request: BatchRequest) -> Self {
        (&request).into()
    }
}

impl From<proto::BatchRequestMsg> for BatchRequest {
    fn from(proto_msg: proto::BatchRequestMsg) -> Self {
        let selector = match proto_msg.target {
            Some(se) => SelectorExpression::from(se),
            None => SelectorExpression::from(String::new()),
        };
        Self {
            command: proto_msg.command,
            args: proto_msg.args,
            selector,
            timeout_seconds: proto_msg.timeout_seconds,
        }
    }
}

impl From<&proto::BatchRequestMsg> for BatchRequest {
    fn from(proto_msg: &proto::BatchRequestMsg) -> Self {
        let selector = match proto_msg.target.as_ref() {
            Some(se) => SelectorExpression::from(se),
            None => SelectorExpression::from(String::new()),
        };
        Self {
            command: proto_msg.command.clone(),
            args: proto_msg.args.clone(),
            selector,
            timeout_seconds: proto_msg.timeout_seconds,
        }
    }
}

// ===== Task 转换 =====
impl From<&Task> for proto::TaskMsg {
    fn from(task: &Task) -> Self {
        Self {
            task_id: Some(proto::TaskId::from(&task.task_id)),
            batch_id: Some(proto::BatchId::from(&task.batch_id)),
            agent_id: Some(proto::AgentId::from(&task.agent_id)),
            command: task.command.clone(),
            args: task.args.clone(),
            timeout_seconds: task.timeout_seconds,
            state: proto::TaskStateEnum::from(task.state) as i32,
            created_at: task.created_at,
            updated_at: task.updated_at,
        }
    }
}

impl From<Task> for proto::TaskMsg {
    fn from(task: Task) -> Self {
        Self {
            task_id: Some(proto::TaskId::from(task.task_id)),
            batch_id: Some(proto::BatchId::from(task.batch_id)),
            agent_id: Some(proto::AgentId::from(task.agent_id)),
            command: task.command,
            args: task.args,
            timeout_seconds: task.timeout_seconds,
            state: proto::TaskStateEnum::from(task.state) as i32,
            created_at: task.created_at,
            updated_at: task.updated_at,
        }
    }
}

impl From<proto::TaskMsg> for Task {
    fn from(proto_msg: proto::TaskMsg) -> Self {
        Self {
            task_id: proto_msg
                .task_id
                .map(TaskId::from)
                .unwrap_or_else(TaskId::generate),
            batch_id: proto_msg
                .batch_id
                .map(BatchId::from)
                .unwrap_or_else(|| BatchId::from("unknown".to_string())),
            agent_id: proto_msg
                .agent_id
                .map(AgentId::from)
                .unwrap_or_else(|| AgentId::from("unknown".to_string())),
            command: proto_msg.command,
            args: proto_msg.args,
            timeout_seconds: proto_msg.timeout_seconds,
            state: TaskState::from(proto_msg.state),
            created_at: proto_msg.created_at,
            updated_at: proto_msg.updated_at,
        }
    }
}

impl From<Batch> for proto::BatchMsg {
    fn from(batch: Batch) -> Self {
        Self {
            batch_id: Some(proto::BatchId::from(batch.batch_id)),
            command: batch.command,
            args: batch.args,
            timeout_seconds: batch.timeout_seconds,
            created_at: batch.created_at,
        }
    }
}

impl From<proto::BatchMsg> for Batch {
    fn from(proto_msg: proto::BatchMsg) -> Self {
        Self {
            batch_id: proto_msg
                .batch_id
                .map(BatchId::from)
                .unwrap_or_else(BatchId::generate),
            command: proto_msg.command,
            args: proto_msg.args,
            timeout_seconds: proto_msg.timeout_seconds,
            created_at: proto_msg.created_at,
        }
    }
}

impl From<&Batch> for proto::BatchMsg {
    fn from(batch: &Batch) -> Self {
        Self {
            batch_id: Some(proto::BatchId::from(&batch.batch_id)),
            command: batch.command.clone(),
            args: batch.args.clone(),
            timeout_seconds: batch.timeout_seconds,
            created_at: batch.created_at,
        }
    }
}

// ===== TaskExecution 转换 =====

impl From<&TaskExecution> for proto::TaskExecutionMsg {
    fn from(execution: &TaskExecution) -> Self {
        Self {
            task_id: Some(proto::TaskId::from(&execution.task_id)),
            agent_id: Some(proto::AgentId::from(&execution.agent_id)),
            state: proto::TaskStateEnum::from(execution.state) as i32,
            exit_code: execution.exit_code,
            stdout: execution.stdout.clone(),
            stderr: execution.stderr.clone(),
            started_at: execution.started_at,
            finished_at: execution.finished_at,
            duration_ms: execution.duration_ms,
        }
    }
}

impl From<TaskExecution> for proto::TaskExecutionMsg {
    fn from(execution: TaskExecution) -> Self {
        Self {
            task_id: Some(proto::TaskId::from(execution.task_id)),
            agent_id: Some(proto::AgentId::from(execution.agent_id)),
            state: proto::TaskStateEnum::from(execution.state) as i32,
            exit_code: execution.exit_code,
            stdout: execution.stdout,
            stderr: execution.stderr,
            started_at: execution.started_at,
            finished_at: execution.finished_at,
            duration_ms: execution.duration_ms,
        }
    }
}

impl From<proto::TaskExecutionMsg> for TaskExecution {
    fn from(proto: proto::TaskExecutionMsg) -> Self {
        Self {
            task_id: proto
                .task_id
                .map(TaskId::from)
                .unwrap_or_else(TaskId::generate),
            agent_id: proto
                .agent_id
                .map(AgentId::from)
                .unwrap_or_else(|| AgentId::from("unknown".to_string())),
            state: TaskState::from(proto.state),
            exit_code: proto.exit_code,
            stdout: proto.stdout,
            stderr: proto.stderr,
            started_at: proto.started_at,
            finished_at: proto.finished_at,
            duration_ms: proto.duration_ms,
        }
    }
}

impl From<AgentStatus> for proto::AgentStatusEnum {
    fn from(status: AgentStatus) -> Self {
        match status {
            AgentStatus::Online => proto::AgentStatusEnum::AgentOnline,
            AgentStatus::Degraded => proto::AgentStatusEnum::AgentDegraded,
            AgentStatus::Offline => proto::AgentStatusEnum::AgentOffline,
            AgentStatus::Removed => proto::AgentStatusEnum::AgentRemoved,
            AgentStatus::Unknown => proto::AgentStatusEnum::AgentUnknown,
        }
    }
}

impl From<proto::AgentStatusEnum> for AgentStatus {
    fn from(proto: proto::AgentStatusEnum) -> Self {
        match proto {
            proto::AgentStatusEnum::AgentOffline => AgentStatus::Offline,
            proto::AgentStatusEnum::AgentOnline => AgentStatus::Online,
            proto::AgentStatusEnum::AgentDegraded => AgentStatus::Degraded,
            proto::AgentStatusEnum::AgentRemoved => AgentStatus::Removed,
            proto::AgentStatusEnum::AgentUnknown => AgentStatus::Unknown,
        }
    }
}

impl From<i32> for AgentStatus {
    fn from(value: i32) -> Self {
        match proto::AgentStatusEnum::try_from(value) {
            Ok(status) => status.into(),
            Err(_) => AgentStatus::Offline, // 默认值
        }
    }
}

impl From<&AgentInfo> for proto::AgentInfoMsg {
    fn from(info: &AgentInfo) -> Self {
        Self {
            agent_id: Some(proto::AgentId::from(&info.agent_id)),
            status: proto::AgentStatusEnum::from(info.status) as i32,
            info: info.info.clone(),
            last_heartbeat: info.last_heartbeat,
            version: info.version.clone(),
            capabilities: info.capabilities.clone(),
        }
    }
}

impl From<AgentInfo> for proto::AgentInfoMsg {
    fn from(info: AgentInfo) -> Self {
        Self {
            agent_id: Some(proto::AgentId::from(info.agent_id)),
            status: proto::AgentStatusEnum::from(info.status) as i32,
            info: info.info,
            last_heartbeat: info.last_heartbeat,
            version: info.version,
            capabilities: info.capabilities,
        }
    }
}

impl From<proto::AgentInfoMsg> for AgentInfo {
    fn from(proto: proto::AgentInfoMsg) -> Self {
        Self {
            agent_id: proto
                .agent_id
                .map(AgentId::from)
                .unwrap_or_else(|| AgentId::from("unknown".to_string())),
            status: AgentStatus::from(proto.status),
            info: proto.info,
            last_heartbeat: proto.last_heartbeat,
            version: proto.version,
            capabilities: proto.capabilities,
        }
    }
}

// ===== 请求/响应转换 =====

impl From<&proto::SubmitBatchRequest> for BatchRequest {
    fn from(proto: &proto::SubmitBatchRequest) -> Self {
        proto
            .batch_request
            .as_ref()
            .map(BatchRequest::from)
            .unwrap_or_default()
    }
}

impl From<proto::SubmitBatchRequest> for BatchRequest {
    fn from(proto: proto::SubmitBatchRequest) -> Self {
        (&proto).into()
    }
}

impl proto::TaskExecutionMsg {
    pub fn from_executions(executions: Vec<TaskExecution>) -> Vec<Self> {
        executions.into_iter().map(Self::from).collect()
    }
}

impl proto::AgentInfoMsg {
    pub fn from_agent_infos(infos: Vec<AgentInfo>) -> Vec<Self> {
        infos.into_iter().map(Self::from).collect()
    }
}

impl proto::SubmitBatchResponse {
    pub fn success(batch_id: BatchId, agent_nums: i64) -> Self {
        Self {
            batch_id: Some(proto::BatchId::from(batch_id)),
            agent_nums,
        }
    }
}

impl proto::CancelBatchResponse {
    pub fn success() -> Self {
        Self { success: true }
    }

    pub fn failure() -> Self {
        Self { success: false }
    }
}

impl proto::ListBatchesResponse {
    pub fn new(batches: Vec<BatchMsg>, total_count: u32) -> Self {
        let has_more = batches.len() < total_count as usize;
        Self {
            batches,
            total_count,
            has_more,
        }
    }
}

impl proto::BatchRequestMsg {
    pub fn validate(&self) -> Result<(), String> {
        if self.command.is_empty() {
            return Err("command cannot be empty".to_string());
        }

        if self.timeout_seconds == 0 {
            return Err("timeout_seconds must be greater than 0".to_string());
        }

        if self.target.is_none() {
            return Err("target is required".to_string());
        }

        Ok(())
    }
}

impl proto::TaskId {
    pub fn validate(&self) -> Result<(), String> {
        if self.value.is_empty() {
            return Err("task_id cannot be empty".to_string());
        }
        Ok(())
    }
}

impl proto::AgentId {
    pub fn validate(&self) -> Result<(), String> {
        if self.value.is_empty() {
            return Err("agent_id cannot be empty".to_string());
        }
        Ok(())
    }
}

impl proto::BatchId {
    pub fn validate(&self) -> Result<(), String> {
        if self.value.is_empty() {
            return Err("batch_id cannot be empty".to_string());
        }
        Ok(())
    }
}

impl proto::RolloutId {
    pub fn validate(&self) -> Result<(), String> {
        if self.value.is_empty() {
            return Err("rollout_id cannot be empty".to_string());
        }
        Ok(())
    }
}

impl From<&FileConfig> for crate::proto::FileConfigMsg {
    fn from(config: &FileConfig) -> Self {
        Self {
            source_path: config.source_path.clone(),
            destination_path: config.destination_path.clone(),
            revision: config.revision,
            owner: config.owner.clone().unwrap_or_default(),
            mode: config.mode.clone().unwrap_or_default(),
            target: config.target.as_ref().map(|t| t.into()),
        }
    }
}

impl From<FileConfig> for crate::proto::FileConfigMsg {
    fn from(config: FileConfig) -> Self {
        Self {
            source_path: config.source_path,
            destination_path: config.destination_path,
            revision: config.revision,
            owner: config.owner.unwrap_or_default(),
            mode: config.mode.unwrap_or_default(),
            target: config.target.map(|t| t.into()),
        }
    }
}

impl TryFrom<crate::proto::FileConfigMsg> for FileConfig {
    type Error = anyhow::Error;

    fn try_from(proto: crate::proto::FileConfigMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            source_path: proto.source_path,
            destination_path: proto.destination_path,
            revision: proto.revision,
            owner: if proto.owner.is_empty() {
                None
            } else {
                Some(proto.owner)
            },
            mode: if proto.mode.is_empty() {
                None
            } else {
                Some(proto.mode)
            },
            target: proto.target.map(SelectorExpression::from),
        })
    }
}

impl TryFrom<&crate::proto::FileConfigMsg> for FileConfig {
    type Error = anyhow::Error;

    fn try_from(proto: &crate::proto::FileConfigMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            source_path: proto.source_path.clone(),
            destination_path: proto.destination_path.clone(),
            revision: proto.revision,
            owner: if proto.owner.is_empty() {
                None
            } else {
                Some(proto.owner.clone())
            },
            mode: if proto.mode.is_empty() {
                None
            } else {
                Some(proto.mode.clone())
            },
            target: proto.target.as_ref().map(SelectorExpression::from),
        })
    }
}

// ===== FileSpec 转换 =====

impl From<&FileSpec> for crate::proto::FileSpecMsg {
    fn from(spec: &FileSpec) -> Self {
        Self {
            source_path: spec.source_path.clone(),
            size: spec.size,
            checksum: spec.checksum.clone(),
            content_type: spec.content_type.clone(),
            created_at: spec.created_at,
        }
    }
}

impl From<FileSpec> for crate::proto::FileSpecMsg {
    fn from(spec: FileSpec) -> Self {
        Self {
            source_path: spec.source_path,
            size: spec.size,
            checksum: spec.checksum,
            content_type: spec.content_type,
            created_at: spec.created_at,
        }
    }
}

impl TryFrom<crate::proto::FileSpecMsg> for FileSpec {
    type Error = anyhow::Error;

    fn try_from(proto: crate::proto::FileSpecMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            source_path: proto.source_path,
            size: proto.size,
            checksum: proto.checksum,
            content_type: proto.content_type,
            created_at: proto.created_at,
        })
    }
}

impl TryFrom<&crate::proto::FileSpecMsg> for FileSpec {
    type Error = anyhow::Error;

    fn try_from(proto: &crate::proto::FileSpecMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            source_path: proto.source_path.clone(),
            size: proto.size,
            checksum: proto.checksum.clone(),
            content_type: proto.content_type.clone(),
            created_at: proto.created_at,
        })
    }
}

// ===== FileOperationResult 转换 =====

impl From<&FileOperationResult> for crate::proto::FileOperationResult {
    fn from(result: &FileOperationResult) -> Self {
        Self {
            success: result.success,
            message: result.message.clone(),
            revision: result.revision,
        }
    }
}

impl From<FileOperationResult> for crate::proto::FileOperationResult {
    fn from(result: FileOperationResult) -> Self {
        Self {
            success: result.success,
            message: result.message,
            revision: result.revision,
        }
    }
}

impl From<crate::proto::FileOperationResult> for FileOperationResult {
    fn from(proto: crate::proto::FileOperationResult) -> Self {
        Self {
            success: proto.success,
            message: proto.message,
            revision: proto.revision,
        }
    }
}

impl From<&crate::proto::FileOperationResult> for FileOperationResult {
    fn from(proto: &crate::proto::FileOperationResult) -> Self {
        Self {
            success: proto.success,
            message: proto.message.clone(),
            revision: proto.revision,
        }
    }
}

// ===== RolloutStatus 转换 =====

impl From<&RolloutStatus> for proto::RolloutStatusMsg {
    fn from(status: &RolloutStatus) -> Self {
        let config = proto::RolloutConfigMsg::from(&status.config);
        let stages = status
            .stages
            .iter()
            .map(proto::RolloutStageStatusMsg::from)
            .collect();

        proto::RolloutStatusMsg {
            config: Some(config),
            state: proto::RolloutStateEnum::from(status.state) as i32,
            current_stage_idx: status.current_stage_idx,
            stages,
            all_target_agents: status
                .all_target_agents
                .iter()
                .map(|id| proto::AgentId {
                    value: id.to_string(),
                })
                .collect(),
            updated_at: status.updated_at,
            error_message: status.error_message.clone(),
            current_action: Some(status.current_action.clone()),
        }
    }
}

impl From<RolloutStatus> for proto::RolloutStatusMsg {
    fn from(status: RolloutStatus) -> Self {
        let config = proto::RolloutConfigMsg::from(status.config);
        let stages = status
            .stages
            .into_iter()
            .map(proto::RolloutStageStatusMsg::from)
            .collect();

        proto::RolloutStatusMsg {
            config: Some(config),
            state: proto::RolloutStateEnum::from(status.state) as i32,
            current_stage_idx: status.current_stage_idx,
            stages,
            all_target_agents: status
                .all_target_agents
                .into_iter()
                .map(|id| proto::AgentId { value: id.into() })
                .collect(),
            updated_at: status.updated_at,
            error_message: status.error_message,
            current_action: Some(status.current_action),
        }
    }
}

impl From<&proto::RolloutStatusMsg> for RolloutStatus {
    fn from(msg: &proto::RolloutStatusMsg) -> Self {
        let cfg = msg.config.as_ref().cloned().unwrap_or_default();
        let strategy = match cfg.strategy.and_then(|s| s.strategy) {
            Some(crate::proto::rollout_strategy_msg::Strategy::Percentage(p)) => {
                RolloutStrategy::Percentage {
                    stages: p.stages.into_iter().map(|v| v as u8).collect(),
                }
            }
            Some(crate::proto::rollout_strategy_msg::Strategy::Count(c)) => {
                RolloutStrategy::Count { stages: c.stages }
            }
            None => RolloutStrategy::default(),
        };
        let fallback_file_config = || crate::file_types::FileConfig {
            source_path: String::new(),
            destination_path: String::new(),
            revision: 0,
            owner: None,
            mode: None,
            target: None,
        };

        let task_type = match cfg.task_type.and_then(|t| t.task_type) {
            Some(crate::proto::rollout_task_type_msg::TaskType::Command(cmd)) => {
                RolloutTaskType::Command {
                    command: cmd.command,
                    args: cmd.args,
                    timeout_seconds: cmd.timeout_seconds,
                }
            }
            Some(crate::proto::rollout_task_type_msg::TaskType::FileDeployment(file)) => {
                let fc = match file.config {
                    Some(config) => match crate::file_types::FileConfig::try_from(config) {
                        Ok(cfg) => cfg,
                        Err(e) => {
                            warn!("Invalid rollout file config: {}", e);
                            fallback_file_config()
                        }
                    },
                    None => {
                        warn!("Missing rollout file config in task type");
                        fallback_file_config()
                    }
                };
                RolloutTaskType::FileDeployment { config: fc }
            }
            None => RolloutTaskType::Command {
                command: String::new(),
                args: vec![],
                timeout_seconds: 60,
            },
        };
        let rollout_id = cfg
            .rollout_id
            .as_ref()
            .map(|id| RolloutId::from(id.value.clone()))
            .unwrap_or_else(|| {
                warn!("Missing rollout_id in RolloutConfigMsg, using 'unknown'");
                RolloutId::from("unknown".to_string())
            });
        let target = cfg
            .target
            .clone()
            .map(SelectorExpression::from)
            .unwrap_or_else(|| {
                warn!("Missing rollout target selector, using empty selector");
                SelectorExpression::from(String::new())
            });

        let rollout_config = RolloutConfig {
            rollout_id,
            name: cfg.name,
            target,
            strategy,
            task_type,
            auto_advance: cfg.auto_advance,
            advance_interval_seconds: cfg.advance_interval_seconds,
            created_at: cfg.created_at,
        };

        let mut stages: Vec<RolloutStageStatus> = Vec::new();
        for s in &msg.stages {
            let target_agents: Vec<AgentId> = s
                .target_agents
                .iter()
                .map(|id| AgentId::from(id.value.clone()))
                .collect();
            let failed_execs: Vec<TaskExecution> = s
                .failed_executions
                .iter()
                .cloned()
                .map(TaskExecution::from)
                .collect();
            stages.push(RolloutStageStatus {
                stage_name: s.stage_name.clone(),
                target_agents,
                batch_id: s.batch_id.as_ref().map(|b| BatchId::from(b.value.clone())),
                started_count: s.started_count,
                completed_count: s.completed_count,
                failed_count: s.failed_count,
                started_at: s.started_at,
                completed_at: s.completed_at,
                failed_executions: failed_execs,
                version_snapshot: None,
            });
        }

        let all_target_agents: Vec<AgentId> = msg
            .all_target_agents
            .iter()
            .map(|id| AgentId::from(id.value.clone()))
            .collect();

        RolloutStatus {
            config: rollout_config,
            state: RolloutState::from(msg.state),
            current_stage_idx: msg.current_stage_idx,
            stages,
            all_target_agents,
            updated_at: msg.updated_at,
            error_message: msg.error_message.clone(),
            current_action: msg.current_action.clone().unwrap_or_default(),
        }
    }
}

// ===== RolloutConfig 转换 =====

impl From<&RolloutConfig> for proto::RolloutConfigMsg {
    fn from(config: &RolloutConfig) -> Self {
        let strategy = proto::RolloutStrategyMsg::from(&config.strategy);
        let task_type = proto::RolloutTaskTypeMsg::from(&config.task_type);

        proto::RolloutConfigMsg {
            rollout_id: Some(proto::RolloutId {
                value: config.rollout_id.to_string(),
            }),
            name: config.name.clone(),
            target: Some(proto::SelectorExpression {
                expression: config.target.to_string(),
            }),
            strategy: Some(strategy),
            task_type: Some(task_type),
            auto_advance: config.auto_advance,
            advance_interval_seconds: config.advance_interval_seconds,
            created_at: config.created_at,
        }
    }
}

impl From<RolloutConfig> for proto::RolloutConfigMsg {
    fn from(config: RolloutConfig) -> Self {
        let strategy = proto::RolloutStrategyMsg::from(config.strategy);
        let task_type = proto::RolloutTaskTypeMsg::from(config.task_type);

        proto::RolloutConfigMsg {
            rollout_id: Some(proto::RolloutId {
                value: config.rollout_id.into(),
            }),
            name: config.name,
            target: Some(proto::SelectorExpression {
                expression: config.target.into(),
            }),
            strategy: Some(strategy),
            task_type: Some(task_type),
            auto_advance: config.auto_advance,
            advance_interval_seconds: config.advance_interval_seconds,
            created_at: config.created_at,
        }
    }
}

// ===== RolloutStrategy 转换 =====

impl From<&RolloutStrategy> for proto::RolloutStrategyMsg {
    fn from(strategy: &RolloutStrategy) -> Self {
        match strategy {
            RolloutStrategy::Percentage { stages } => proto::RolloutStrategyMsg {
                strategy: Some(proto::rollout_strategy_msg::Strategy::Percentage(
                    proto::PercentageStrategy {
                        stages: stages.iter().map(|&s| s as u32).collect(),
                    },
                )),
            },
            RolloutStrategy::Count { stages } => proto::RolloutStrategyMsg {
                strategy: Some(proto::rollout_strategy_msg::Strategy::Count(
                    proto::CountStrategy {
                        stages: stages.clone(),
                    },
                )),
            },
        }
    }
}

impl From<RolloutStrategy> for proto::RolloutStrategyMsg {
    fn from(strategy: RolloutStrategy) -> Self {
        match strategy {
            RolloutStrategy::Percentage { stages } => proto::RolloutStrategyMsg {
                strategy: Some(proto::rollout_strategy_msg::Strategy::Percentage(
                    proto::PercentageStrategy {
                        stages: stages.into_iter().map(|s| s as u32).collect(),
                    },
                )),
            },
            RolloutStrategy::Count { stages } => proto::RolloutStrategyMsg {
                strategy: Some(proto::rollout_strategy_msg::Strategy::Count(
                    proto::CountStrategy { stages },
                )),
            },
        }
    }
}

// ===== RolloutTaskType 转换 =====

impl From<&RolloutTaskType> for proto::RolloutTaskTypeMsg {
    fn from(task_type: &RolloutTaskType) -> Self {
        match task_type {
            RolloutTaskType::Command {
                command,
                args,
                timeout_seconds,
            } => proto::RolloutTaskTypeMsg {
                task_type: Some(proto::rollout_task_type_msg::TaskType::Command(
                    proto::CommandTask {
                        command: command.clone(),
                        args: args.clone(),
                        timeout_seconds: *timeout_seconds,
                    },
                )),
            },
            RolloutTaskType::FileDeployment { config } => proto::RolloutTaskTypeMsg {
                task_type: Some(proto::rollout_task_type_msg::TaskType::FileDeployment(
                    proto::FileDeploymentTask {
                        config: Some(config.into()),
                    },
                )),
            },
        }
    }
}

impl From<RolloutTaskType> for proto::RolloutTaskTypeMsg {
    fn from(task_type: RolloutTaskType) -> Self {
        match task_type {
            RolloutTaskType::Command {
                command,
                args,
                timeout_seconds,
            } => proto::RolloutTaskTypeMsg {
                task_type: Some(proto::rollout_task_type_msg::TaskType::Command(
                    proto::CommandTask {
                        command,
                        args,
                        timeout_seconds,
                    },
                )),
            },
            RolloutTaskType::FileDeployment { config } => proto::RolloutTaskTypeMsg {
                task_type: Some(proto::rollout_task_type_msg::TaskType::FileDeployment(
                    proto::FileDeploymentTask {
                        config: Some(config.into()),
                    },
                )),
            },
        }
    }
}

// ===== RolloutStageStatus 转换 =====

impl From<&RolloutStageStatus> for proto::RolloutStageStatusMsg {
    fn from(stage: &RolloutStageStatus) -> Self {
        proto::RolloutStageStatusMsg {
            stage_name: stage.stage_name.clone(),
            target_agents: stage
                .target_agents
                .iter()
                .map(|id| proto::AgentId {
                    value: id.to_string(),
                })
                .collect(),
            batch_id: stage.batch_id.as_ref().map(|id| proto::BatchId {
                value: id.to_string(),
            }),
            started_count: stage.started_count,
            completed_count: stage.completed_count,
            failed_count: stage.failed_count,
            started_at: stage.started_at,
            completed_at: stage.completed_at,
            failed_executions: stage
                .failed_executions
                .iter()
                .map(proto::TaskExecutionMsg::from)
                .collect(),
        }
    }
}

impl From<RolloutStageStatus> for proto::RolloutStageStatusMsg {
    fn from(stage: RolloutStageStatus) -> Self {
        proto::RolloutStageStatusMsg {
            stage_name: stage.stage_name,
            target_agents: stage
                .target_agents
                .into_iter()
                .map(|id| proto::AgentId { value: id.into() })
                .collect(),
            batch_id: stage.batch_id.map(|id| proto::BatchId { value: id.into() }),
            started_count: stage.started_count,
            completed_count: stage.completed_count,
            failed_count: stage.failed_count,
            started_at: stage.started_at,
            completed_at: stage.completed_at,
            failed_executions: stage
                .failed_executions
                .into_iter()
                .map(proto::TaskExecutionMsg::from)
                .collect(),
        }
    }
}

// ===== RolloutState 转换 =====

impl From<RolloutState> for proto::RolloutStateEnum {
    fn from(state: RolloutState) -> Self {
        match state {
            RolloutState::Created => proto::RolloutStateEnum::RolloutCreated,
            RolloutState::Running => proto::RolloutStateEnum::RolloutRunning,
            RolloutState::Completed => proto::RolloutStateEnum::RolloutCompleted,
            RolloutState::Failed => proto::RolloutStateEnum::RolloutFailed,
            RolloutState::RollingBack => proto::RolloutStateEnum::RolloutRollingback,
            RolloutState::RollbackFailed => proto::RolloutStateEnum::RolloutRollbackfailed,
            RolloutState::RolledBack => proto::RolloutStateEnum::RolloutRolledback,
        }
    }
}

impl From<proto::RolloutStateEnum> for RolloutState {
    fn from(state: proto::RolloutStateEnum) -> Self {
        match state {
            proto::RolloutStateEnum::RolloutCreated => RolloutState::Created,
            proto::RolloutStateEnum::RolloutRunning => RolloutState::Running,
            proto::RolloutStateEnum::RolloutCompleted => RolloutState::Completed,
            proto::RolloutStateEnum::RolloutFailed => RolloutState::Failed,
            proto::RolloutStateEnum::RolloutRollingback => RolloutState::RollingBack,
            proto::RolloutStateEnum::RolloutRollbackfailed => RolloutState::RollbackFailed,
            proto::RolloutStateEnum::RolloutRolledback => RolloutState::RolledBack,
        }
    }
}

impl From<i32> for RolloutState {
    fn from(value: i32) -> Self {
        match proto::RolloutStateEnum::try_from(value) {
            Ok(state) => state.into(),
            Err(_) => RolloutState::Created, // 默认值
        }
    }
}

// FileVersion 转换
impl From<FileVersion> for proto::FileVersionMsg {
    fn from(version: FileVersion) -> Self {
        proto::FileVersionMsg {
            name: version.name,
            revision: version.revision,
            size: version.size,
            checksum: version.checksum,
            created_at: version.created_at,
            is_current: version.is_current,
        }
    }
}

impl From<proto::FileVersionMsg> for FileVersion {
    fn from(msg: proto::FileVersionMsg) -> Self {
        FileVersion {
            name: msg.name,
            revision: msg.revision,
            size: msg.size,
            checksum: msg.checksum,
            created_at: msg.created_at,
            is_current: msg.is_current,
        }
    }
}

// FileHistory 转换
impl From<FileHistory> for proto::FileHistoryMsg {
    fn from(history: FileHistory) -> Self {
        proto::FileHistoryMsg {
            name: history.name,
            versions: history.versions.into_iter().map(|v| v.into()).collect(),
            current_version: history.current_version,
        }
    }
}

impl From<proto::FileHistoryMsg> for FileHistory {
    fn from(msg: proto::FileHistoryMsg) -> Self {
        FileHistory {
            name: msg.name,
            versions: msg.versions.into_iter().map(|v| v.into()).collect(),
            current_version: msg.current_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== ID 转换测试 =====

    #[test]
    fn test_task_id_conversion() {
        let task_id = TaskId::generate();
        let proto_id: proto::TaskId = (&task_id).into();
        let back: TaskId = proto_id.into();
        assert_eq!(task_id.to_string(), back.to_string());
    }

    #[test]
    fn test_agent_id_conversion() {
        let agent_id = AgentId::new("test-agent-123");
        let proto_id: proto::AgentId = (&agent_id).into();
        let back: AgentId = proto_id.into();
        assert_eq!(agent_id.as_str(), back.as_str());
    }

    #[test]
    fn test_batch_id_conversion() {
        let batch_id = BatchId::generate();
        let proto_id: proto::BatchId = (&batch_id).into();
        let back: BatchId = proto_id.into();
        assert_eq!(batch_id.to_string(), back.to_string());
    }

    #[test]
    fn test_rollout_id_conversion() {
        let rollout_id = RolloutId::generate();
        let proto_id: proto::RolloutId = (&rollout_id).into();
        let back: RolloutId = proto_id.into();
        assert_eq!(rollout_id.to_string(), back.to_string());
    }

    #[test]
    fn test_selector_expression_conversion() {
        let expr = SelectorExpression::from("labels[\"env\"] == \"prod\"".to_string());
        let proto_expr: proto::SelectorExpression = (&expr).into();
        let back: SelectorExpression = proto_expr.into();
        assert_eq!(expr.to_string(), back.to_string());
    }

    // ===== TaskState 转换测试 =====

    #[test]
    fn test_task_state_conversion_all_variants() {
        let states = vec![
            TaskState::Created,
            TaskState::Pending,
            TaskState::Running,
            TaskState::Success,
            TaskState::Failed,
            TaskState::Timeout,
            TaskState::Cancelled,
        ];

        for state in states {
            let proto_state: proto::TaskStateEnum = state.into();
            let back: TaskState = proto_state.into();
            assert_eq!(state, back);
        }
    }

    #[test]
    fn test_task_state_from_invalid_i32() {
        let state: TaskState = 999.into();
        assert_eq!(state, TaskState::Created); // 默认值
    }

    // ===== AgentStatus 转换测试 =====

    #[test]
    fn test_agent_status_conversion_all_variants() {
        let statuses = vec![
            AgentStatus::Online,
            AgentStatus::Degraded,
            AgentStatus::Offline,
            AgentStatus::Removed,
            AgentStatus::Unknown,
        ];

        for status in statuses {
            let proto_status: proto::AgentStatusEnum = status.into();
            let back: AgentStatus = proto_status.into();
            assert_eq!(status, back);
        }
    }

    #[test]
    fn test_agent_status_from_invalid_i32() {
        let status: AgentStatus = 999.into();
        assert_eq!(status, AgentStatus::Offline); // 默认值
    }

    // ===== RolloutState 转换测试 =====

    #[test]
    fn test_rollout_state_conversion_all_variants() {
        let states = vec![
            RolloutState::Created,
            RolloutState::Running,
            RolloutState::Completed,
            RolloutState::Failed,
            RolloutState::RollingBack,
            RolloutState::RollbackFailed,
            RolloutState::RolledBack,
        ];

        for state in states {
            let proto_state: proto::RolloutStateEnum = state.into();
            let back: RolloutState = proto_state.into();
            assert_eq!(state, back);
        }
    }

    #[test]
    fn test_rollout_state_from_invalid_i32() {
        let state: RolloutState = 999.into();
        assert_eq!(state, RolloutState::Created); // 默认值
    }

    // ===== BatchRequest 转换测试 =====

    #[test]
    fn test_batch_request_conversion() {
        let request = BatchRequest {
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            selector: SelectorExpression::from("all".to_string()),
            timeout_seconds: 60,
        };

        let proto_msg: proto::BatchRequestMsg = (&request).into();
        let back: BatchRequest = proto_msg.into();

        assert_eq!(request.command, back.command);
        assert_eq!(request.args, back.args);
        assert_eq!(request.timeout_seconds, back.timeout_seconds);
    }

    #[test]
    fn test_batch_request_msg_validate_empty_command() {
        let msg = proto::BatchRequestMsg {
            command: String::new(),
            args: vec![],
            target: Some(proto::SelectorExpression {
                expression: "all".to_string(),
            }),
            timeout_seconds: 60,
        };
        assert!(msg.validate().is_err());
    }

    #[test]
    fn test_batch_request_msg_validate_zero_timeout() {
        let msg = proto::BatchRequestMsg {
            command: "echo".to_string(),
            args: vec![],
            target: Some(proto::SelectorExpression {
                expression: "all".to_string(),
            }),
            timeout_seconds: 0,
        };
        assert!(msg.validate().is_err());
    }

    #[test]
    fn test_batch_request_msg_validate_missing_target() {
        let msg = proto::BatchRequestMsg {
            command: "echo".to_string(),
            args: vec![],
            target: None,
            timeout_seconds: 60,
        };
        assert!(msg.validate().is_err());
    }

    #[test]
    fn test_batch_request_msg_validate_success() {
        let msg = proto::BatchRequestMsg {
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            target: Some(proto::SelectorExpression {
                expression: "all".to_string(),
            }),
            timeout_seconds: 60,
        };
        assert!(msg.validate().is_ok());
    }

    // ===== Task 转换测试 =====

    #[test]
    fn test_task_conversion() {
        let task = Task {
            task_id: TaskId::generate(),
            batch_id: BatchId::generate(),
            agent_id: AgentId::new("agent-1"),
            command: "uptime".to_string(),
            args: vec![],
            timeout_seconds: 30,
            state: TaskState::Running,
            created_at: 1000,
            updated_at: 1001,
        };

        let proto_msg: proto::TaskMsg = (&task).into();
        let back: Task = proto_msg.into();

        assert_eq!(task.command, back.command);
        assert_eq!(task.state, back.state);
        assert_eq!(task.timeout_seconds, back.timeout_seconds);
    }

    // ===== ID Validation 测试 =====

    #[test]
    fn test_task_id_validate_empty() {
        let id = proto::TaskId {
            value: String::new(),
        };
        assert!(id.validate().is_err());
    }

    #[test]
    fn test_task_id_validate_success() {
        let id = proto::TaskId {
            value: "task-123".to_string(),
        };
        assert!(id.validate().is_ok());
    }

    #[test]
    fn test_agent_id_validate_empty() {
        let id = proto::AgentId {
            value: String::new(),
        };
        assert!(id.validate().is_err());
    }

    #[test]
    fn test_agent_id_validate_success() {
        let id = proto::AgentId {
            value: "agent-123".to_string(),
        };
        assert!(id.validate().is_ok());
    }

    #[test]
    fn test_batch_id_validate_empty() {
        let id = proto::BatchId {
            value: String::new(),
        };
        assert!(id.validate().is_err());
    }

    #[test]
    fn test_rollout_id_validate_empty() {
        let id = proto::RolloutId {
            value: String::new(),
        };
        assert!(id.validate().is_err());
    }

    // ===== AgentInfo 转换测试 =====

    #[test]
    fn test_agent_info_conversion() {
        let info = AgentInfo {
            agent_id: AgentId::new("test-agent"),
            status: AgentStatus::Online,
            info: std::collections::HashMap::from([(
                "hostname".to_string(),
                "server1".to_string(),
            )]),
            last_heartbeat: 1234567890,
            version: "1.0.0".to_string(),
            capabilities: vec!["exec".to_string()],
        };

        let proto_msg: proto::AgentInfoMsg = (&info).into();
        let back: AgentInfo = proto_msg.into();

        assert_eq!(info.agent_id.as_str(), back.agent_id.as_str());
        assert_eq!(info.status, back.status);
        assert_eq!(info.version, back.version);
    }

    // ===== FileVersion/FileHistory 转换测试 =====

    #[test]
    fn test_file_version_conversion() {
        let version = FileVersion {
            name: "config.yaml".to_string(),
            revision: 3,
            size: 1024,
            checksum: "abc123".to_string(),
            created_at: 1000,
            is_current: true,
        };

        let proto_msg: proto::FileVersionMsg = version.clone().into();
        let back: FileVersion = proto_msg.into();

        assert_eq!(version.name, back.name);
        assert_eq!(version.revision, back.revision);
        assert_eq!(version.is_current, back.is_current);
    }

    #[test]
    fn test_file_history_conversion() {
        let history = FileHistory {
            name: "app.conf".to_string(),
            versions: vec![
                FileVersion {
                    name: "app.conf".to_string(),
                    revision: 1,
                    size: 512,
                    checksum: "v1hash".to_string(),
                    created_at: 1000,
                    is_current: false,
                },
                FileVersion {
                    name: "app.conf".to_string(),
                    revision: 2,
                    size: 600,
                    checksum: "v2hash".to_string(),
                    created_at: 2000,
                    is_current: true,
                },
            ],
            current_version: 2,
        };

        let proto_msg: proto::FileHistoryMsg = history.clone().into();
        let back: FileHistory = proto_msg.into();

        assert_eq!(history.name, back.name);
        assert_eq!(history.versions.len(), back.versions.len());
        assert_eq!(history.current_version, back.current_version);
    }

    // ===== Response 构造测试 =====

    #[test]
    fn test_submit_batch_response_success() {
        let batch_id = BatchId::generate();
        let response = proto::SubmitBatchResponse::success(batch_id.clone(), 5);

        assert!(response.batch_id.is_some());
        assert_eq!(response.agent_nums, 5);
    }

    #[test]
    fn test_cancel_batch_response() {
        let success = proto::CancelBatchResponse::success();
        assert!(success.success);

        let failure = proto::CancelBatchResponse::failure();
        assert!(!failure.success);
    }

    #[test]
    fn test_list_batches_response() {
        let batches = vec![proto::BatchMsg {
            batch_id: Some(proto::BatchId {
                value: "b1".to_string(),
            }),
            command: "echo".to_string(),
            args: vec![],
            timeout_seconds: 30,
            created_at: 1000,
        }];

        let response = proto::ListBatchesResponse::new(batches, 10);
        assert_eq!(response.batches.len(), 1);
        assert_eq!(response.total_count, 10);
        assert!(response.has_more);
    }
}
