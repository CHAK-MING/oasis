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
        if let Some(mode) = self.mode.as_ref() {
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
    pub fn parsed_mode(&self) -> u32 {
        self.mode
            .as_ref()
            .and_then(|m| u32::from_str_radix(&m[1..], 8).ok())
            .unwrap_or(0o644)
    }

    /// 获取解析的所有者信息
    pub fn parsed_owner(&self) -> (Option<String>, Option<String>) {
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
    pub fn current_version(&self) -> Option<&FileVersion> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_config_validate_empty_destination() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "".to_string(),
            revision: 1,
            owner: None,
            mode: None,
            target: Some(SelectorExpression::from("all".to_string())),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_file_config_validate_path_traversal() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/../passwd".to_string(),
            revision: 1,
            owner: None,
            mode: None,
            target: Some(SelectorExpression::from("all".to_string())),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_file_config_validate_missing_target() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: None,
            mode: None,
            target: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_file_config_validate_invalid_mode() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: None,
            mode: Some("644".to_string()), // missing leading 0
            target: Some(SelectorExpression::from("all".to_string())),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_file_config_validate_invalid_mode_non_octal() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: None,
            mode: Some("0999".to_string()), // invalid octal
            target: Some(SelectorExpression::from("all".to_string())),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_file_config_validate_valid_mode() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: None,
            mode: Some("0644".to_string()),
            target: Some(SelectorExpression::from("all".to_string())),
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_file_config_validate_invalid_owner() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: Some(":group".to_string()), // empty user
            mode: None,
            target: Some(SelectorExpression::from("all".to_string())),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_file_config_parsed_mode() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: None,
            mode: Some("0755".to_string()),
            target: None,
        };
        assert_eq!(config.parsed_mode(), 0o755);
    }

    #[test]
    fn test_file_config_parsed_mode_default() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: None,
            mode: None,
            target: None,
        };
        assert_eq!(config.parsed_mode(), 0o644);
    }

    #[test]
    fn test_file_config_parsed_owner() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: Some("root:wheel".to_string()),
            mode: None,
            target: None,
        };
        let (user, group) = config.parsed_owner();
        assert_eq!(user, Some("root".to_string()));
        assert_eq!(group, Some("wheel".to_string()));
    }

    #[test]
    fn test_file_config_parsed_owner_no_group() {
        let config = FileConfig {
            source_path: "test.conf".to_string(),
            destination_path: "/etc/app.conf".to_string(),
            revision: 1,
            owner: Some("root".to_string()),
            mode: None,
            target: None,
        };
        let (user, group) = config.parsed_owner();
        assert_eq!(user, Some("root".to_string()));
        assert_eq!(group, None);
    }

    #[test]
    fn test_file_version_new() {
        let version = FileVersion::new(
            "config.yaml".to_string(),
            3,
            1024,
            "sha256:abc123".to_string(),
            1234567890,
            true,
        );
        assert_eq!(version.name, "config.yaml");
        assert_eq!(version.revision, 3);
        assert_eq!(version.size, 1024);
        assert!(version.is_current);
    }

    #[test]
    fn test_file_history_new() {
        let versions = vec![
            FileVersion::new(
                "app.conf".to_string(),
                1,
                512,
                "v1".to_string(),
                1000,
                false,
            ),
            FileVersion::new("app.conf".to_string(), 2, 600, "v2".to_string(), 2000, true),
        ];
        let history = FileHistory::new("app.conf".to_string(), versions);

        assert_eq!(history.name, "app.conf");
        assert_eq!(history.current_version, 2);
        assert_eq!(history.versions.len(), 2);
    }

    #[test]
    fn test_file_history_current_version() {
        let versions = vec![
            FileVersion::new(
                "app.conf".to_string(),
                1,
                512,
                "v1".to_string(),
                1000,
                false,
            ),
            FileVersion::new("app.conf".to_string(), 2, 600, "v2".to_string(), 2000, true),
        ];
        let history = FileHistory::new("app.conf".to_string(), versions);

        let current = history.current_version();
        assert!(current.is_some());
        assert_eq!(current.unwrap().revision, 2);
    }

    #[test]
    fn test_file_history_sort_versions() {
        let versions = vec![
            FileVersion::new("app.conf".to_string(), 3, 700, "v3".to_string(), 3000, true),
            FileVersion::new(
                "app.conf".to_string(),
                1,
                512,
                "v1".to_string(),
                1000,
                false,
            ),
            FileVersion::new(
                "app.conf".to_string(),
                2,
                600,
                "v2".to_string(),
                2000,
                false,
            ),
        ];
        let mut history = FileHistory::new("app.conf".to_string(), versions);
        history.sort_versions();

        assert_eq!(history.versions[0].revision, 1);
        assert_eq!(history.versions[1].revision, 2);
        assert_eq!(history.versions[2].revision, 3);
    }

    #[test]
    fn test_file_operation_result_success() {
        let result = FileOperationResult::success("File deployed".to_string(), 5);
        assert!(result.success);
        assert_eq!(result.message, "File deployed");
        assert_eq!(result.revision, 5);
    }

    #[test]
    fn test_file_operation_result_failure() {
        let result = FileOperationResult::failure("Failed to deploy".to_string(), 0);
        assert!(!result.success);
        assert_eq!(result.message, "Failed to deploy");
    }

    #[test]
    fn test_file_spec_creation() {
        let spec = FileSpec {
            source_path: "config.yaml".to_string(),
            size: 2048,
            checksum: "sha256:xyz789".to_string(),
            content_type: "application/yaml".to_string(),
            created_at: 1234567890,
        };
        assert_eq!(spec.size, 2048);
        assert!(!spec.checksum.is_empty());
    }
}
