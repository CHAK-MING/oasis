use crate::{core_types::SelectorExpression, error::CoreError};
use serde::{Deserialize, Serialize};

/// 统一的文件规格类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSpec {
    /// 源文件路径
    pub source_path: String,
    /// 文件大小（字节）
    pub size: u64,
    /// SHA256校验和
    pub checksum: String,
    /// MIME类型
    pub content_type: String,
    /// 创建时间戳
    pub created_at: i64,
}

/// 文件应用配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileConfig {
    /// 源文件路径
    pub source_path: String,
    /// 目标路径
    pub destination_path: String,
    /// nats 原生 revision
    pub revision: u64,
    /// 所有者（user:group）
    pub owner: Option<String>,
    /// 权限模式（0644）
    pub mode: Option<String>,
    /// 目标选择器
    pub target: Option<SelectorExpression>,
}

/// 文件版本信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileVersion {
    /// 文件名
    pub name: String,
    /// revision
    pub revision: u64,
    /// 文件大小
    pub size: u64,
    /// SHA256 校验和
    pub checksum: String,
    /// 创建时间戳
    pub created_at: i64,
    /// 是否为当前版本
    pub is_current: bool,
}

/// 文件历史信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHistory {
    /// 文件名
    pub name: String,
    /// 所有版本
    pub versions: Vec<FileVersion>,
    /// 当前版本号
    pub current_version: u64,
}

/// 统一的文件操作结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOperationResult {
    /// 操作是否成功
    pub success: bool,
    /// 消息
    pub message: String,
    // revision
    pub revision: u64,
}

impl FileConfig {
    /// 验证部署配置
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.destination_path.trim().is_empty() {
            return Err(CoreError::InvalidTask {
                reason: "destination path cannot be empty".to_string(),
                severity: crate::error::ErrorSeverity::Error,
            });
        }

        if self.destination_path.contains("..") {
            return Err(CoreError::InvalidTask {
                reason: "destination path contains invalid characters".to_string(),
                severity: crate::error::ErrorSeverity::Error,
            });
        }

        if self.target.is_none() {
            return Err(CoreError::InvalidTask {
                reason: "target cannot be empty".to_string(),
                severity: crate::error::ErrorSeverity::Error,
            });
        }

        // 验证权限模式
        if let Some(ref mode) = self.mode
            && (!mode.starts_with('0')
                || mode.len() != 4
                || !mode[1..].chars().all(|c| c.is_digit(8)))
        {
            return Err(CoreError::InvalidTask {
                reason: "invalid mode format (expected octal like '0644')".to_string(),
                severity: crate::error::ErrorSeverity::Error,
            });
        }

        // 验证所有者格式
        if let Some(owner) = &self.owner
            && let Some((user, group)) = owner.split_once(':')
            && (user.is_empty() || group.is_empty())
        {
            return Err(CoreError::InvalidTask {
                reason: "invalid owner format (expected 'user:group')".to_string(),
                severity: crate::error::ErrorSeverity::Error,
            });
        }

        Ok(())
    }

    /// 获取解析的权限模式
    pub fn get_parsed_mode(&self) -> u32 {
        self.mode
            .as_ref()
            .and_then(|m| u32::from_str_radix(&m[1..], 8).ok())
            .unwrap_or(0o644)
    }

    /// 获取解析的所有者信息
    pub fn get_parsed_owner(&self) -> (Option<String>, Option<String>) {
        match &self.owner {
            Some(owner) => {
                if let Some((user, group)) = owner.split_once(':') {
                    (Some(user.to_string()), Some(group.to_string()))
                } else {
                    (Some(owner.clone()), None)
                }
            }
            None => (None, None),
        }
    }
}

impl FileVersion {
    /// 创建新的文件版本
    pub fn new(
        name: String,
        revision: u64,
        size: u64,
        checksum: String,
        created_at: i64,
        is_current: bool,
    ) -> Self {
        Self {
            name,
            revision,
            size,
            checksum,
            created_at,
            is_current,
        }
    }
}

impl FileHistory {
    /// 创建新的文件历史
    pub fn new(name: String, versions: Vec<FileVersion>) -> Self {
        let current_version = versions
            .iter()
            .filter(|v| v.is_current)
            .map(|v| v.revision)
            .next()
            .unwrap_or(0);

        Self {
            name,
            current_version,
            versions,
        }
    }

    /// 获取当前版本
    pub fn get_current_version(&self) -> Option<&FileVersion> {
        self.versions.iter().find(|v| v.is_current)
    }

    /// 按 revision 排序版本
    pub fn sort_versions(&mut self) {
        self.versions.sort_by_key(|v| v.revision);
    }
}

impl FileOperationResult {
    /// 创建成功结果
    pub fn success(message: String, revision: u64) -> Self {
        Self {
            success: true,
            message,
            revision,
        }
    }

    /// 创建失败结果
    pub fn failure(message: String, revision: u64) -> Self {
        Self {
            success: false,
            message,
            revision,
        }
    }
}
