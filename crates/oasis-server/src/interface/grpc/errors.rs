use oasis_core::error::CoreError;
use tonic::Status;

/// 构造统一的中文错误消息: [错误码] 描述（排查建议：...）
fn build_msg(code: &str, desc: impl AsRef<str>, hint: &str) -> String {
    format!("[{}] {}（排查建议：{}）", code, desc.as_ref(), hint)
}

/// 将 CoreError 映射为 tonic::Status（结构化中文信息）
pub fn map_core_error(error: CoreError) -> Status {
    match error {
        CoreError::InvalidTask {
            reason,
            severity: _,
        } => Status::invalid_argument(build_msg(
            "E_INVALID_ARG",
            format!("无效请求: {}", reason),
            "检查参数格式与取值是否正确",
        )),
        CoreError::TaskExecutionFailed {
            task_id,
            reason,
            severity: _,
            ..
        } => Status::internal(build_msg(
            "E_TASK_EXEC",
            format!("任务执行失败 {}: {}", task_id, reason),
            "查看节点日志与命令输出，确认命令/脚本是否可执行",
        )),
        CoreError::TaskTimeout {
            task_id,
            severity: _,
        } => Status::deadline_exceeded(build_msg(
            "E_TASK_TIMEOUT",
            format!("任务超时: {}", task_id),
            "提高超时时间，或排查节点资源与网络延迟",
        )),
        CoreError::Agent {
            agent_id,
            message,
            severity: _,
        } => {
            if message.contains("not found") {
                Status::not_found(build_msg(
                    "E_AGENT_NOT_FOUND",
                    format!("Agent 未找到: {}", agent_id),
                    "确认 Agent 是否已注册、ID 是否填写正确",
                ))
            } else if message.contains("offline") {
                Status::unavailable(build_msg(
                    "E_AGENT_OFFLINE",
                    format!("Agent 离线: {}", agent_id),
                    "检查 Agent 进程与网络连通性，确认 NATS/gRPC 是否可达",
                ))
            } else {
                Status::internal(build_msg(
                    "E_AGENT_INTERNAL",
                    format!("Agent 错误 {}: {}", agent_id, message),
                    "查看 Agent 日志，必要时重启 Agent",
                ))
            }
        }
        CoreError::Batch {
            batch_id,
            message,
            severity: _,
        } => Status::internal(build_msg(
            "E_BATCH",
            format!("批次错误 {}: {}", batch_id, message),
            "确认批次是否存在以及状态是否允许该操作",
        )),
        CoreError::File {
            path,
            message,
            severity: _,
        } => {
            if message.contains("not found") {
                Status::not_found(build_msg(
                    "E_FILE_NOT_FOUND",
                    format!("文件未找到: {}", path),
                    "确认路径是否正确，或文件是否已上传",
                ))
            } else if message.contains("access denied") {
                Status::permission_denied(build_msg(
                    "E_FILE_PERMISSION",
                    format!("文件访问被拒绝: {}", path),
                    "检查远端文件权限与属主设置",
                ))
            } else {
                Status::internal(build_msg(
                    "E_FILE",
                    format!("文件错误 {}: {}", path, message),
                    "检查文件路径、权限与磁盘空间",
                ))
            }
        }
        CoreError::Config {
            message,
            severity: _,
        } => Status::invalid_argument(build_msg(
            "E_CONFIG",
            format!("配置错误: {}", message),
            "检查配置文件与环境变量是否正确",
        )),
        CoreError::Nats {
            message,
            severity: _,
        } => Status::internal(build_msg(
            "E_NATS",
            format!("NATS 错误: {}", message),
            "检查 NATS 连接、JetStream 状态与权限配置",
        )),
        CoreError::Network {
            message,
            severity: _,
        } => Status::unavailable(build_msg(
            "E_NETWORK",
            format!("网络错误: {}", message),
            "检查网络连通性、防火墙与负载均衡配置",
        )),
        CoreError::Connection {
            endpoint,
            severity: _,
        } => Status::unavailable(build_msg(
            "E_CONNECT",
            format!("连接失败: {}", endpoint),
            "确认服务监听地址与端口是否正确开放",
        )),
        CoreError::Internal {
            message,
            severity: _,
        } => Status::internal(build_msg(
            "E_INTERNAL",
            format!("服务器内部错误: {}", message),
            "查看服务器日志并上报问题",
        )),
        CoreError::ServiceUnavailable {
            service,
            severity: _,
        } => Status::unavailable(build_msg(
            "E_SERVICE_UNAVAILABLE",
            format!("服务不可用: {}", service),
            "确认目标服务健康状态与依赖是否可用",
        )),
        CoreError::PermissionDenied {
            operation,
            severity: _,
        } => Status::permission_denied(build_msg(
            "E_PERMISSION",
            format!("权限不足: {}", operation),
            "检查角色/凭据是否具备操作权限",
        )),
        CoreError::Serialization {
            message,
            severity: _,
        } => Status::internal(build_msg(
            "E_SERIALIZATION",
            format!("序列化错误: {}", message),
            "检查数据格式与兼容性（可能是协议版本不匹配）",
        )),
        CoreError::NotFound {
            entity_type,
            entity_id,
            severity: _,
        } => Status::not_found(build_msg(
            "E_NOT_FOUND",
            format!("{} 未找到: {}", entity_type, entity_id),
            "确认资源是否存在，或 ID 是否正确",
        )),
        CoreError::VersionConflict {
            entity_type,
            entity_id,
            expected_version,
            actual_version,
            severity: _,
        } => Status::aborted(build_msg(
            "E_VERSION_CONFLICT",
            format!(
                "版本冲突: {} {} (期望 {}, 实际 {})",
                entity_type, entity_id, expected_version, actual_version
            ),
            "请刷新后重试，或在最新版本基础上重新提交",
        )),
    }
}
