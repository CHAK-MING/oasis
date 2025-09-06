use crate::{
    core_types::{ SelectorExpression},
    error::CoreError,
};
use serde::{Deserialize, Serialize};

/// 统一的文件规格类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSpec {
    /// 对象名称/文件名
    pub name: String,
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
pub struct FileApplyConfig {
    /// 文件名
    pub name: String,
    /// 目标路径
    pub destination_path: String,
    /// 所有者（user:group）
    pub owner: Option<String>,
    /// 权限模式（0644）
    pub mode: Option<String>,
    /// 原子操作
    pub atomic: bool,
    /// 目标选择器
    pub target: Option<SelectorExpression>,
}

/// 统一的文件操作结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOperationResult {
    /// 操作是否成功
    pub success: bool,
    /// 消息
    pub message: String,
}

impl FileApplyConfig {
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
        if let Some(ref mode) = self.mode {
            if !mode.starts_with('0')
                || mode.len() != 4
                || !mode[1..].chars().all(|c| c.is_digit(8))
            {
                return Err(CoreError::InvalidTask {
                    reason: "invalid mode format (expected octal like '0644')".to_string(),
                    severity: crate::error::ErrorSeverity::Error,
                });
            }
        }

        // 验证所有者格式
        if let Some(owner) = &self.owner {
            if let Some((user, group)) = owner.split_once(':') {
                if user.is_empty() || group.is_empty() {
                    return Err(CoreError::InvalidTask {
                        reason: "invalid owner format (expected 'user:group')".to_string(),
                        severity: crate::error::ErrorSeverity::Error,
                    });
                }
            }
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

impl FileOperationResult {
    /// 创建成功结果
    pub fn success(message: String) -> Self {
        Self {
            success: true,
            message,
        }
    }

    /// 创建失败结果
    pub fn failure(message: String) -> Self {
        Self {
            success: false,
            message,
        }
    }
}
