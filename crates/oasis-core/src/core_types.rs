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
        Self(uuid::Uuid::now_v7().to_string())
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
        Self(uuid::Uuid::now_v7().to_string())
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
        Self(uuid::Uuid::now_v7().to_string())
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
        Self(uuid::Uuid::now_v7().to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    mod agent_id_tests {
        use super::*;

        #[test]
        fn test_new_from_str() {
            let id = AgentId::new("agent-001");
            assert_eq!(id.as_str(), "agent-001");
        }

        #[test]
        fn test_new_from_string() {
            let id = AgentId::new(String::from("agent-002"));
            assert_eq!(id.as_str(), "agent-002");
        }

        #[test]
        fn test_generate_is_unique() {
            let id1 = AgentId::generate();
            let id2 = AgentId::generate();
            assert_ne!(id1, id2);
        }

        #[test]
        fn test_generate_is_valid_uuid() {
            let id = AgentId::generate();
            assert!(uuid::Uuid::parse_str(id.as_str()).is_ok());
        }

        #[test]
        fn test_display() {
            let id = AgentId::new("test-agent");
            assert_eq!(format!("{}", id), "test-agent");
        }

        #[test]
        fn test_equality() {
            let id1 = AgentId::new("same");
            let id2 = AgentId::new("same");
            let id3 = AgentId::new("different");
            assert_eq!(id1, id2);
            assert_ne!(id1, id3);
        }

        #[test]
        fn test_hash() {
            use std::collections::HashSet;
            let mut set = HashSet::new();
            set.insert(AgentId::new("agent-1"));
            set.insert(AgentId::new("agent-2"));
            set.insert(AgentId::new("agent-1"));
            assert_eq!(set.len(), 2);
        }

        #[test]
        fn test_clone() {
            let id1 = AgentId::new("original");
            let id2 = id1.clone();
            assert_eq!(id1, id2);
        }

        #[test]
        fn test_default() {
            let id = AgentId::default();
            assert_eq!(id.as_str(), "");
        }

        #[test]
        fn test_serde_roundtrip() {
            let id = AgentId::new("serde-test");
            let json = serde_json::to_string(&id).unwrap();
            let deserialized: AgentId = serde_json::from_str(&json).unwrap();
            assert_eq!(id, deserialized);
        }
    }

    mod task_id_tests {
        use super::*;

        #[test]
        fn test_new_and_as_str() {
            let id = TaskId::new("task-123");
            assert_eq!(id.as_str(), "task-123");
        }

        #[test]
        fn test_generate_is_valid_uuid() {
            let id = TaskId::generate();
            assert!(uuid::Uuid::parse_str(id.as_str()).is_ok());
        }
    }

    mod batch_id_tests {
        use super::*;

        #[test]
        fn test_new_and_as_str() {
            let id = BatchId::new("batch-456");
            assert_eq!(id.as_str(), "batch-456");
        }

        #[test]
        fn test_generate_is_valid_uuid() {
            let id = BatchId::generate();
            assert!(uuid::Uuid::parse_str(id.as_str()).is_ok());
        }
    }

    mod rollout_id_tests {
        use super::*;

        #[test]
        fn test_new_and_as_str() {
            let id = RolloutId::new("rollout-789");
            assert_eq!(id.as_str(), "rollout-789");
        }

        #[test]
        fn test_generate_is_valid_uuid() {
            let id = RolloutId::generate();
            assert!(uuid::Uuid::parse_str(id.as_str()).is_ok());
        }
    }

    mod selector_expression_tests {
        use super::*;

        #[test]
        fn test_new_from_str() {
            let expr = SelectorExpression::new("env=production");
            assert_eq!(expr.as_str(), "env=production");
        }

        #[test]
        fn test_wildcard() {
            let expr = SelectorExpression::new("*");
            assert_eq!(expr.as_str(), "*");
        }

        #[test]
        fn test_complex_expression() {
            let expr = SelectorExpression::new("env=prod,region=us-east-1,tier=web");
            assert_eq!(expr.as_str(), "env=prod,region=us-east-1,tier=web");
        }

        #[test]
        fn test_default() {
            let expr = SelectorExpression::default();
            assert_eq!(expr.as_str(), "");
        }

        #[test]
        fn test_equality() {
            let expr1 = SelectorExpression::new("test");
            let expr2 = SelectorExpression::new("test");
            assert_eq!(expr1, expr2);
        }
    }
}
