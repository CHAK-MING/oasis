/// 统一的目标选择器 - 消除双重参数设计
///
/// 这个模块实现了智能解析逻辑，将用户输入统一转换为CEL选择器表达式

#[derive(Debug, Clone)]
pub struct TargetSelector {
    pub expression: String,
}

impl TargetSelector {
    /// 智能解析用户输入为CEL选择器表达式
    ///
    /// # 解析规则
    /// - "agent1,agent2,agent3" -> agent_id in ["agent1", "agent2", "agent3"]  
    /// - "agent1" -> agent_id == "agent1"
    /// - "true" | "all" -> true (所有agents)
    /// - "labels.role == 'web'" -> labels.role == 'web' (CEL表达式)
    pub fn parse(input: &str) -> Self {
        let input = input.trim();

        if input.is_empty() {
            panic!("Target cannot be empty");
        }

        let expression = if input.contains(',') {
            // 逗号分隔的Agent列表 -> agent_id in [...]
            let agents: Vec<&str> = input.split(',').map(|s| s.trim()).collect();
            let agent_list = agents
                .iter()
                .filter(|a| !a.is_empty())
                .map(|a| format!("\"{}\"", a))
                .collect::<Vec<_>>()
                .join(", ");
            format!("agent_id in [{}]", agent_list)
        } else if input == "true" || input == "all" {
            // 所有agents
            "true".to_string()
        } else if Self::is_cel_expression(input) {
            // 已经是CEL表达式，直接使用
            input.to_string()
        } else {
            // 单个agent ID
            format!("agent_id == \"{}\"", input)
        };

        Self { expression }
    }

    /// 检测是否为CEL表达式
    ///
    /// 简单的启发式检测：包含CEL关键字
    fn is_cel_expression(input: &str) -> bool {
        input.contains("==")
            || input.contains("!=")
            || input.contains(" in ")
            || input.contains("labels.")
            || input.contains("facts.")
            || input.contains("&&")
            || input.contains("||")
    }

    /// 获取CEL表达式
    pub fn expression(&self) -> &str {
        &self.expression
    }
}
