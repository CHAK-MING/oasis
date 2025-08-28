//! 目标选择器解析器
//!
//! 支持新语法：
//! - `labels['role'] == 'web' && 'frontend' in groups`
//! - `true` (所有 agents)
//! - `agent1,agent2` (ID 列表)

use anyhow::{Result, anyhow};
use pest::{Parser, iterators::Pair};
use pest_derive::Parser;
use std::collections::HashSet;

#[derive(Parser)]
#[grammar = "src/application/selector/grammar.pest"]
struct TargetGrammar;

/// 解析后的 AST 节点
#[derive(Debug, Clone)]
pub enum AstNode {
    /// 布尔字面量
    Bool(bool),
    /// 字符串字面量
    String(String),
    /// 标签访问 labels['key']
    LabelAccess(String),
    /// 组包含检查 'group' in groups
    GroupContains(String),
    /// 等值比较 ==
    Equal(Box<AstNode>, Box<AstNode>),
    /// 逻辑与 &&
    And(Box<AstNode>, Box<AstNode>),
    /// 逻辑或 ||
    Or(Box<AstNode>, Box<AstNode>),
    /// 逻辑非 !
    Not(Box<AstNode>),
    /// ID 列表
    IdList(Vec<String>),
}

/// 目标选择器解析器
pub struct TargetParser;

impl TargetParser {
    /// 解析目标选择器字符串为 AST
    pub fn parse(input: &str) -> Result<AstNode> {
        let input = input.trim();

        if input.is_empty() {
            return Err(anyhow!("目标选择器不能为空"));
        }

        // 快速路径：ID 列表
        if Self::is_id_list(input) {
            return Ok(Self::parse_id_list(input));
        }

        // 快速路径：true
        if input == "true" {
            return Ok(AstNode::Bool(true));
        }

        // 解析复杂表达式
        let mut parsed = TargetGrammar::parse(Rule::expression, input)
            .map_err(|e| anyhow!("解析失败: {}", e))?;
        let pair = parsed.next().ok_or_else(|| anyhow!("解析失败: 空表达式"))?;
        let mut inner = pair.into_inner().peekable();
        Self::parse_expression(&mut inner)
    }

    /// 检测是否为 ID 列表
    fn is_id_list(input: &str) -> bool {
        input.contains(',')
            && !input.contains("==")
            && !input.contains("!=")
            && !input.contains("&&")
            && !input.contains("||")
            && !input.contains("!")
            && !input.contains("labels")
            && !input.contains("groups")
            && !input.contains("in")
    }

    /// 解析 ID 列表
    fn parse_id_list(input: &str) -> AstNode {
        let ids: Vec<String> = input
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        AstNode::IdList(ids)
    }

    /// 去掉首尾引号（支持 ' 或 ")
    fn unquote(s: &str) -> String {
        let bytes = s.as_bytes();
        if bytes.len() >= 2 {
            let first = bytes[0];
            let last = bytes[bytes.len() - 1];
            if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
                return s[1..s.len() - 1].to_string();
            }
        }
        s.to_string()
    }

    /// 解析表达式
    fn parse_expression(
        pairs: &mut std::iter::Peekable<pest::iterators::Pairs<Rule>>,
    ) -> Result<AstNode> {
        let mut left = Self::parse_term(pairs)?;

        while let Some(pair) = pairs.peek() {
            match pair.as_rule() {
                Rule::and_op => {
                    pairs.next(); // 消费 &&
                    let right = Self::parse_term(pairs)?;
                    left = AstNode::And(Box::new(left), Box::new(right));
                }
                Rule::or_op => {
                    pairs.next(); // 消费 ||
                    let right = Self::parse_term(pairs)?;
                    left = AstNode::Or(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }

        Ok(left)
    }

    /// 解析项
    fn parse_term(
        pairs: &mut std::iter::Peekable<pest::iterators::Pairs<Rule>>,
    ) -> Result<AstNode> {
        let pair = pairs.next().ok_or_else(|| anyhow!("意外的输入结束"))?;

        match pair.as_rule() {
            Rule::not_op => {
                let operand = Self::parse_term(pairs)?;
                Ok(AstNode::Not(Box::new(operand)))
            }
            Rule::paren_expr => {
                let mut inner_pairs = pair.into_inner().peekable();
                Self::parse_expression(&mut inner_pairs)
            }
            Rule::label_access => Self::parse_label_access(pair),
            Rule::group_contains => Self::parse_group_contains(pair),
            Rule::comparison => Self::parse_comparison(pair),
            Rule::string_literal => {
                let s = Self::unquote(pair.as_str());
                Ok(AstNode::String(s))
            }
            Rule::bool_literal => {
                let b = pair.as_str() == "true";
                Ok(AstNode::Bool(b))
            }
            _ => Err(anyhow!("不支持的语法: {:?}", pair.as_rule())),
        }
    }

    /// 解析标签访问
    fn parse_label_access(pair: Pair<Rule>) -> Result<AstNode> {
        let key = pair
            .into_inner()
            .next()
            .ok_or_else(|| anyhow!("标签访问缺少键"))?
            .as_str();
        let key = Self::unquote(key);
        Ok(AstNode::LabelAccess(key))
    }

    /// 解析组包含
    fn parse_group_contains(pair: Pair<Rule>) -> Result<AstNode> {
        let group = pair
            .into_inner()
            .next()
            .ok_or_else(|| anyhow!("组包含缺少组名"))?
            .as_str();
        let group = Self::unquote(group);
        Ok(AstNode::GroupContains(group))
    }

    /// 解析比较操作
    fn parse_comparison(pair: Pair<Rule>) -> Result<AstNode> {
        let mut inner = pair.into_inner();
        let left = Self::parse_comparison_operand(
            inner
                .next()
                .ok_or_else(|| anyhow!("比较操作缺少左操作数"))?,
        )?;
        let op = inner
            .next()
            .ok_or_else(|| anyhow!("比较操作缺少操作符"))?
            .as_str();
        let right = Self::parse_comparison_operand(
            inner
                .next()
                .ok_or_else(|| anyhow!("比较操作缺少右操作数"))?,
        )?;

        match op {
            "==" => Ok(AstNode::Equal(Box::new(left), Box::new(right))),
            _ => Err(anyhow!("不支持的操作符: {}", op)),
        }
    }

    /// 解析比较操作的操作数
    fn parse_comparison_operand(pair: Pair<Rule>) -> Result<AstNode> {
        match pair.as_rule() {
            Rule::label_access => Self::parse_label_access(pair),
            Rule::string_literal => {
                let s = Self::unquote(pair.as_str());
                Ok(AstNode::String(s))
            }
            Rule::comparison_operand => {
                // 递归解析比较操作数
                let mut inner = pair.into_inner();
                let operand = inner.next().ok_or_else(|| anyhow!("比较操作数为空"))?;
                Self::parse_comparison_operand(operand)
            }
            _ => Err(anyhow!("不支持的比较操作数: {:?}", pair.as_rule())),
        }
    }

    /// 提取 AST 中引用的标签键
    pub fn extract_label_keys(node: &AstNode) -> HashSet<String> {
        let mut keys = HashSet::new();
        Self::collect_label_keys(node, &mut keys);
        keys
    }

    /// 收集标签键
    fn collect_label_keys(node: &AstNode, keys: &mut HashSet<String>) {
        match node {
            AstNode::LabelAccess(key) => {
                keys.insert(key.clone());
            }
            AstNode::Equal(left, right) => {
                Self::collect_label_keys(left, keys);
                Self::collect_label_keys(right, keys);
            }
            AstNode::And(left, right) | AstNode::Or(left, right) => {
                Self::collect_label_keys(left, keys);
                Self::collect_label_keys(right, keys);
            }
            AstNode::Not(operand) => {
                Self::collect_label_keys(operand, keys);
            }
            _ => {}
        }
    }

    /// 提取 AST 中引用的组名
    pub fn extract_group_names(node: &AstNode) -> HashSet<String> {
        let mut groups = HashSet::new();
        Self::collect_group_names(node, &mut groups);
        groups
    }

    /// 收集组名
    fn collect_group_names(node: &AstNode, groups: &mut HashSet<String>) {
        match node {
            AstNode::GroupContains(group) => {
                groups.insert(group.clone());
            }
            AstNode::Equal(left, right) => {
                Self::collect_group_names(left, groups);
                Self::collect_group_names(right, groups);
            }
            AstNode::And(left, right) | AstNode::Or(left, right) => {
                Self::collect_group_names(left, groups);
                Self::collect_group_names(right, groups);
            }
            AstNode::Not(operand) => {
                Self::collect_group_names(operand, groups);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_id_list() {
        let result = TargetParser::parse("agent1,agent2,agent3").unwrap();
        match result {
            AstNode::IdList(ids) => {
                assert_eq!(ids, vec!["agent1", "agent2", "agent3"]);
            }
            _ => panic!("Expected IdList"),
        }
    }

    #[test]
    fn test_parse_true() {
        let result = TargetParser::parse("true").unwrap();
        match result {
            AstNode::Bool(true) => {}
            _ => panic!("Expected Bool(true)"),
        }
    }

    #[test]
    fn test_parse_label_equality() {
        // 双引号写法
        let result = TargetParser::parse("labels[\"role\"] == \"web\"").unwrap();
        match result {
            AstNode::Equal(left, right) => {
                match *left {
                    AstNode::LabelAccess(key) => assert_eq!(key, "role"),
                    _ => panic!("Expected LabelAccess"),
                }
                match *right {
                    AstNode::String(value) => assert_eq!(value, "web"),
                    _ => panic!("Expected String"),
                }
            }
            _ => panic!("Expected Equal"),
        }
    }

    #[test]
    fn test_parse_group_contains() {
        let result = TargetParser::parse("'frontend' in groups").unwrap();
        match result {
            AstNode::GroupContains(group) => {
                assert_eq!(group, "frontend");
            }
            _ => panic!("Expected GroupContains"),
        }
    }

    #[test]
    fn test_parse_complex_expression() {
        let result =
            TargetParser::parse("labels[\"role\"] == \"web\" && \"frontend\" in groups").unwrap();
        match result {
            AstNode::And(left, right) => {
                // 验证左操作数
                match *left {
                    AstNode::Equal(_, _) => {}
                    _ => panic!("Expected Equal as left operand"),
                }
                // 验证右操作数
                match *right {
                    AstNode::GroupContains(_) => {}
                    _ => panic!("Expected GroupContains as right operand"),
                }
            }
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn test_extract_label_keys() {
        let ast = TargetParser::parse("labels[\"role\"] == \"web\" && labels[\"env\"] == \"prod\"")
            .unwrap();
        let keys = TargetParser::extract_label_keys(&ast);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains("role"));
        assert!(keys.contains("env"));
    }

    #[test]
    fn test_extract_group_names() {
        let ast = TargetParser::parse("'frontend' in groups && 'backend' in groups").unwrap();
        let groups = TargetParser::extract_group_names(&ast);
        assert_eq!(groups.len(), 2);
        assert!(groups.contains("frontend"));
        assert!(groups.contains("backend"));
    }
}
