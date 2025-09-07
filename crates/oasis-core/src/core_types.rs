use derive_more::{AsRef, Display, From, Into};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Display, From, Into, AsRef, Serialize, Deserialize, Default,
)]
pub struct AgentId(String);

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Display, From, Into, AsRef, Serialize, Deserialize, Default,
)]
pub struct TaskId(String);

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Display, From, Into, AsRef, Serialize, Deserialize, Default,
)]
pub struct BatchId(String);
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Display, From, Into, AsRef, Serialize, Deserialize, Default,
)]
pub struct RolloutId(String);

impl AgentId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl TaskId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl BatchId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl RolloutId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Display, From, Into, AsRef, Serialize, Deserialize, Default,
)]
pub struct SelectorExpression(String);

impl SelectorExpression {
    pub fn new(expr: impl Into<String>) -> Self {
        Self(expr.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
