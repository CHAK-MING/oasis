use oasis_core::error::CoreError;
use serde::{Deserialize, Serialize};

/// File metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub size: u64,
    pub checksum: String,
    pub content_type: String,
}

impl FileInfo {
    pub fn validate_sha256(&self, expected: &str) -> Result<(), CoreError> {
        let exp = expected.to_lowercase();
        if self.checksum.to_lowercase() != exp {
            return Err(CoreError::File {
                path: "unknown".to_string(),
                message: format!("Checksum mismatch: expected {exp}, got {}", self.checksum),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUploadResult {
    pub object_name: String,
    pub size: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileApplyConfig {
    pub object_name: String,
    pub destination_path: String,
    pub expected_sha256: Option<String>,
    pub owner: Option<String>,
    pub mode: Option<String>,
    pub atomic: bool,
}

impl FileApplyConfig {
    pub fn new(object_name: String, destination_path: String) -> Self {
        Self {
            object_name,
            destination_path,
            expected_sha256: None,
            owner: None,
            mode: None,
            atomic: false,
        }
    }
    pub fn with_sha256(mut self, sha256: String) -> Self {
        self.expected_sha256 = Some(sha256);
        self
    }
    pub fn with_owner(mut self, owner: String) -> Self {
        self.owner = Some(owner);
        self
    }
    pub fn with_mode(mut self, mode: String) -> Self {
        self.mode = Some(mode);
        self
    }
    pub fn with_atomic(mut self, atomic: bool) -> Self {
        self.atomic = atomic;
        self
    }
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.object_name.is_empty() {
            return Err(CoreError::InvalidTask {
                reason: "object_name cannot be empty".into(),
            });
        }
        if self.destination_path.is_empty() {
            return Err(CoreError::InvalidTask {
                reason: "destination_path cannot be empty".into(),
            });
        }
        Ok(())
    }
}
