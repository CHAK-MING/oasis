//! 查询规划器
//!
//! 将 AST 转换为优化的位图操作计划

// use anyhow::Result;
use std::sync::Arc;

use crate::application::selector::index::{BitmapOperation, InvertedIndex};
use crate::application::selector::parser::AstNode;

#[derive(Clone)]
pub struct QueryPlan {
    pub root: BitmapOperation,
}

pub struct QueryPlanner {
    // 未直接读取，但保留以便未来基于索引统计选择更优计划
    _index: Arc<InvertedIndex>,
}

impl QueryPlanner {
    pub fn new(index: Arc<InvertedIndex>) -> Self {
        Self { _index: index }
    }

    pub fn build_plan(&self, query: &str) -> QueryPlan {
        let ast =
            crate::application::selector::parser::TargetParser::parse(query).expect("parse query");
        let op = self.ast_to_operation(&ast);
        let op = self.optimize_operation(op);
        QueryPlan { root: op }
    }

    fn ast_to_operation(&self, ast: &AstNode) -> BitmapOperation {
        match ast {
            AstNode::Bool(true) => BitmapOperation::Universe,
            AstNode::Bool(false) => BitmapOperation::Not(Box::new(BitmapOperation::Universe)),
            AstNode::IdList(ids) => {
                let converted = ids
                    .iter()
                    .cloned()
                    .map(oasis_core::types::AgentId::from)
                    .collect();
                BitmapOperation::IdList(converted)
            }
            AstNode::And(lhs, rhs) => BitmapOperation::And(
                Box::new(self.ast_to_operation(lhs)),
                Box::new(self.ast_to_operation(rhs)),
            ),
            AstNode::Or(lhs, rhs) => BitmapOperation::Or(
                Box::new(self.ast_to_operation(lhs)),
                Box::new(self.ast_to_operation(rhs)),
            ),
            AstNode::Not(inner) => BitmapOperation::Not(Box::new(self.ast_to_operation(inner))),
            AstNode::Equal(left, right) => {
                // Expect form: labels['k'] == 'v'
                let (key, value) = match (&**left, &**right) {
                    (AstNode::LabelAccess(k), AstNode::String(v)) => (k.clone(), v.clone()),
                    (AstNode::String(v), AstNode::LabelAccess(k)) => (k.clone(), v.clone()),
                    _ => ("".to_string(), "".to_string()),
                };
                self.build_equality_operation(key, value)
            }
            AstNode::GroupContains(group) => BitmapOperation::GroupContains(group.clone()),
            _ => BitmapOperation::Universe,
        }
    }

    fn build_equality_operation(&self, key: String, value: String) -> BitmapOperation {
        BitmapOperation::LabelEquals(key, value)
    }

    fn optimize_operation(&self, op: BitmapOperation) -> BitmapOperation {
        op
    }

    /// 提取查询中引用的标签键
    pub fn extract_label_keys(ast: &AstNode) -> std::collections::HashSet<String> {
        crate::application::selector::parser::TargetParser::extract_label_keys(ast)
    }

    /// 提取查询中引用的组名
    pub fn extract_group_names(ast: &AstNode) -> std::collections::HashSet<String> {
        crate::application::selector::parser::TargetParser::extract_group_names(ast)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::selector::parser::TargetParser;

    #[test]
    fn test_plan_simple_label_query() {
        let ast = TargetParser::parse("labels[\"role\"] == \"web\"").unwrap();
        let planner = QueryPlanner::new(Arc::new(InvertedIndex::new()));
        let _plan = planner.build_plan("labels[\"role\"] == \"web\"");
    }

    #[test]
    fn test_plan_complex_query() {
        let ast =
            TargetParser::parse("labels[\"role\"] == \"web\" && \"frontend\" in groups").unwrap();
        let planner = QueryPlanner::new(Arc::new(InvertedIndex::new()));
        let _plan = planner.build_plan("labels[\"role\"] == \"web\" && \"frontend\" in groups");
    }

    #[test]
    fn test_plan_id_list() {
        let ast = TargetParser::parse("agent1,agent2,agent3").unwrap();
        let planner = QueryPlanner::new(Arc::new(InvertedIndex::new()));
        let _plan = planner.build_plan("agent1,agent2,agent3");
    }

    #[test]
    fn test_plan_true() {
        let ast = TargetParser::parse("true").unwrap();
        let planner = QueryPlanner::new(Arc::new(InvertedIndex::new()));
        let _plan = planner.build_plan("true");
    }

    #[test]
    fn test_extract_label_keys() {
        let ast = TargetParser::parse("labels[\"role\"] == \"web\" && labels[\"env\"] == \"prod\"")
            .unwrap();
        let keys = QueryPlanner::extract_label_keys(&ast);

        assert_eq!(keys.len(), 2);
        assert!(keys.contains("role"));
        assert!(keys.contains("env"));
    }

    #[test]
    fn test_extract_group_names() {
        let ast = TargetParser::parse("'frontend' in groups && 'backend' in groups").unwrap();
        let groups = QueryPlanner::extract_group_names(&ast);

        assert_eq!(groups.len(), 2);
        assert!(groups.contains("frontend"));
        assert!(groups.contains("backend"));
    }
}
