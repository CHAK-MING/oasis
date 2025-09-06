use crate::agent_types::AgentInfo;
use crate::agent_types::AgentStatus;
use crate::core_types::BatchId;
use crate::core_types::SelectorExpression;
use crate::core_types::{AgentId, TaskId};
use crate::file_types::FileApplyConfig;
use crate::file_types::FileOperationResult;
use crate::file_types::FileSpec;
use crate::proto;
use crate::proto::BatchMsg;
use crate::task_types::*;

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
            value: task_id.to_string(),
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
            value: agent_id.to_string(),
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
            value: batch_id.to_string(),
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
            expression: selector_expression.to_string(),
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
        (&task).into()
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
        (&batch).into()
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
        (&execution).into()
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
            AgentStatus::Offline => proto::AgentStatusEnum::AgentOffline,
            AgentStatus::Removed => proto::AgentStatusEnum::AgentRemoved,
        }
    }
}

impl From<proto::AgentStatusEnum> for AgentStatus {
    fn from(proto: proto::AgentStatusEnum) -> Self {
        match proto {
            proto::AgentStatusEnum::AgentOffline => AgentStatus::Offline,
            proto::AgentStatusEnum::AgentOnline => AgentStatus::Online,
            proto::AgentStatusEnum::AgentRemoved => AgentStatus::Removed,
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
            id: Some(proto::AgentId::from(&info.id)),
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
        (&info).into()
    }
}

impl From<proto::AgentInfoMsg> for AgentInfo {
    fn from(proto: proto::AgentInfoMsg) -> Self {
        Self {
            id: proto
                .id
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

impl From<&FileApplyConfig> for crate::proto::FileApplyConfigMsg {
    fn from(config: &FileApplyConfig) -> Self {
        Self {
            name: config.name.clone(),
            destination_path: config.destination_path.clone(),
            owner: config.owner.clone().unwrap_or_default(),
            mode: config.mode.clone().unwrap_or_default(),
            target: config.target.as_ref().map(|t| t.into()),
        }
    }
}

impl From<FileApplyConfig> for crate::proto::FileApplyConfigMsg {
    fn from(config: FileApplyConfig) -> Self {
        (&config).into()
    }
}

impl TryFrom<crate::proto::FileApplyConfigMsg> for FileApplyConfig {
    type Error = anyhow::Error;

    fn try_from(proto: crate::proto::FileApplyConfigMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            name: proto.name,
            destination_path: proto.destination_path,
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
            atomic: false, // proto 中没有这个字段，默认 false
            target: proto.target.map(SelectorExpression::from),
        })
    }
}

impl TryFrom<&crate::proto::FileApplyConfigMsg> for FileApplyConfig {
    type Error = anyhow::Error;

    fn try_from(proto: &crate::proto::FileApplyConfigMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            name: proto.name.clone(),
            destination_path: proto.destination_path.clone(),
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
            atomic: false,
            target: proto.target.as_ref().map(SelectorExpression::from),
        })
    }
}

// ===== FileSpec 转换 =====

impl From<&FileSpec> for crate::proto::FileSpecMsg {
    fn from(spec: &FileSpec) -> Self {
        Self {
            name: spec.name.clone(),
            size: spec.size,
            checksum: spec.checksum.clone(),
            content_type: spec.content_type.clone(),
            created_at: spec.created_at,
        }
    }
}

impl From<FileSpec> for crate::proto::FileSpecMsg {
    fn from(spec: FileSpec) -> Self {
        (&spec).into()
    }
}

impl TryFrom<crate::proto::FileSpecMsg> for FileSpec {
    type Error = anyhow::Error;

    fn try_from(proto: crate::proto::FileSpecMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            name: proto.name,
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
            name: proto.name.clone(),
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
        }
    }
}

impl From<FileOperationResult> for crate::proto::FileOperationResult {
    fn from(result: FileOperationResult) -> Self {
        (&result).into()
    }
}

impl From<crate::proto::FileOperationResult> for FileOperationResult {
    fn from(proto: crate::proto::FileOperationResult) -> Self {
        Self {
            success: proto.success,
            message: proto.message,
        }
    }
}

impl From<&crate::proto::FileOperationResult> for FileOperationResult {
    fn from(proto: &crate::proto::FileOperationResult) -> Self {
        Self {
            success: proto.success,
            message: proto.message.clone(),
        }
    }
}
