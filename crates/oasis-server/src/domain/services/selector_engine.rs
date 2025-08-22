use async_trait::async_trait;
use oasis_core::{
    error::CoreError,
    selector::{CelSelector, NodeAttributes},
};

/// 选择器引擎 - 领域服务（精简版）
#[async_trait]
pub trait SelectorEngine: Send + Sync {
    async fn resolve(
        &self,
        expression: &str,
        nodes: &[NodeAttributes],
    ) -> Result<Vec<String>, CoreError>;
}

/// CEL 选择器引擎（无额外缓存，直接使用 core 的实现与缓存）
#[derive(Default)]
pub struct CelSelectorEngine;

impl CelSelectorEngine {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SelectorEngine for CelSelectorEngine {
    async fn resolve(
        &self,
        expression: &str,
        nodes: &[NodeAttributes],
    ) -> Result<Vec<String>, CoreError> {
        let selector =
            CelSelector::new(expression.to_string()).map_err(|e| CoreError::InvalidTask {
                reason: format!("Failed to compile CEL expression: {}", e),

            })?;

        let mut matched_ids = Vec::new();
        for node in nodes {
            match selector.matches(node) {
                Ok(true) => matched_ids.push(node.id.clone()),
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(expression = %expression, node_id = %node.id, error = %e, "Selector evaluation failed");
                }
            }
        }
        Ok(matched_ids)
    }
}
