//! 灰度发布相关类型定义

use crate::core_types::{AgentId, BatchId, RolloutId, SelectorExpression};
use crate::file_types::FileConfig;
use crate::task_types::TaskExecution;
use chrono;
use serde::{Deserialize, Serialize};

/// 灰度发布状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RolloutState {
    Created,        // 已创建
    Running,        // 执行中
    Completed,      // 已完成
    Failed,         // 失败
    RollingBack,    // 正在回滚
    RollbackFailed, // 回滚失败
    RolledBack,     // 回滚完成
}

impl RolloutState {
    /// 检查是否为终端状态
    pub fn is_terminal(&self) -> bool {
        matches!(self, RolloutState::Completed | RolloutState::Failed)
    }

    /// 检查是否可以推进
    pub fn can_advance(&self) -> bool {
        matches!(self, RolloutState::Created | RolloutState::Running)
    }

    /// 检查是否可以回滚
    pub fn can_rollback(&self) -> bool {
        !matches!(self, RolloutState::Running | RolloutState::Failed)
    }

    pub fn is_busy(&self) -> bool {
        matches!(self, RolloutState::Running | RolloutState::RollingBack)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::RollingBack => "rolling_back",
            Self::RollbackFailed => "rollback_failed",
            Self::RolledBack => "rolled_back",
        }
    }
}

impl Default for RolloutState {
    fn default() -> Self {
        RolloutState::Created
    }
}

/// 灰度发布策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RolloutStrategy {
    /// 按比例分阶段
    Percentage { stages: Vec<u8> }, // [10, 30, 60, 100]
    /// 按Agent数量分阶段
    Count { stages: Vec<u32> }, // [2, 5, 10, 0] (0表示剩余全部)
    /// 按Agent分组
    Groups { groups: Vec<String> }, // ["canary", "staging", "prod"]
}

impl Default for RolloutStrategy {
    fn default() -> Self {
        RolloutStrategy::Percentage {
            stages: vec![10, 30, 60, 100],
        }
    }
}

impl RolloutStrategy {
    /// 解析策略字符串
    pub fn parse(strategy_str: &str) -> Result<Self, String> {
        if let Some(percentage_part) = strategy_str.strip_prefix("percentage:") {
            let stages: Result<Vec<u8>, _> = percentage_part
                .split(',')
                .map(|s| s.trim().parse::<u8>())
                .collect();

            match stages {
                Ok(stages) => {
                    if stages.is_empty() {
                        return Err("百分比策略至少需要一个阶段".to_string());
                    }
                    if stages.iter().any(|&p| p == 0 || p > 100) {
                        return Err("百分比必须在1-100之间".to_string());
                    }
                    if stages.last() != Some(&100) {
                        return Err("最后一个阶段必须是100%".to_string());
                    }
                    Ok(RolloutStrategy::Percentage { stages })
                }
                Err(_) => Err("无效的百分比格式".to_string()),
            }
        } else if let Some(count_part) = strategy_str.strip_prefix("count:") {
            let stages: Result<Vec<u32>, _> = count_part
                .split(',')
                .map(|s| s.trim().parse::<u32>())
                .collect();

            match stages {
                Ok(stages) => {
                    if stages.is_empty() {
                        return Err("计数策略至少需要一个阶段".to_string());
                    }
                    Ok(RolloutStrategy::Count { stages })
                }
                Err(_) => Err("无效的计数格式".to_string()),
            }
        } else if let Some(groups_part) = strategy_str.strip_prefix("groups:") {
            let groups: Vec<String> = groups_part
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if groups.is_empty() {
                return Err("分组策略至少需要一个分组".to_string());
            }
            Ok(RolloutStrategy::Groups { groups })
        } else {
            Err(
                "策略格式无效，支持: percentage:10,30,100 或 count:2,5,0 或 groups:canary,prod"
                    .to_string(),
            )
        }
    }
}

/// 灰度发布任务类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RolloutTaskType {
    /// 执行命令
    Command {
        command: String,
        args: Vec<String>,
        timeout_seconds: u32,
    },
    /// 文件部署
    FileDeployment { config: FileConfig },
}

/// 灰度发布配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutConfig {
    /// 发布ID
    pub rollout_id: RolloutId,
    /// 名称/描述
    pub name: String,
    /// 目标选择器
    pub target: SelectorExpression,
    /// 发布策略
    pub strategy: RolloutStrategy,
    /// 任务类型
    pub task_type: RolloutTaskType,
    /// 自动推进（默认false，需要手动推进）
    pub auto_advance: bool,
    /// 推进间隔（秒，仅在auto_advance=true时有效）
    pub advance_interval_seconds: u32,
    /// 创建时间
    pub created_at: i64,
}

/// 灰度发布阶段状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutStageStatus {
    /// 阶段索引
    pub stage_index: u32,
    /// 阶段名称/描述
    pub stage_name: String,
    /// 目标Agent列表
    pub target_agents: Vec<AgentId>,
    /// 关联的BatchId（用于命令执行）
    pub batch_id: Option<BatchId>,
    /// 已开始的Agent数量
    pub started_count: u32,
    /// 成功完成的Agent数量
    pub completed_count: u32,
    /// 失败的Agent数量
    pub failed_count: u32,
    /// 阶段状态
    pub state: RolloutState,
    /// 开始时间
    pub started_at: Option<i64>,
    /// 完成时间
    pub completed_at: Option<i64>,
    /// 失败的任务执行详情
    pub failed_executions: Vec<TaskExecution>,
    /// 版本快照信息
    pub version_snapshot: Option<VersionSnapshot>,
}

/// 版本快照信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionSnapshot {
    pub created_at: i64,
    pub snapshot_data: SnapshotData,
}

/// 快照数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotData {
    FileSnapshot {
        file_config: FileConfig,
        previous_revision: Option<u64>, // 推进前的文件版本
    },
    CommandSnapshot {
        // 命令执行的快照，当前主要记录执行状态
        executed_at: i64,
    },
}

impl VersionSnapshot {
    pub fn new_file_snapshot(file_config: FileConfig, previous_revision: Option<u64>) -> Self {
        Self {
            created_at: chrono::Utc::now().timestamp(),
            snapshot_data: SnapshotData::FileSnapshot {
                file_config,
                previous_revision,
            },
        }
    }

    pub fn new_command_snapshot() -> Self {
        Self {
            created_at: chrono::Utc::now().timestamp(),
            snapshot_data: SnapshotData::CommandSnapshot {
                executed_at: chrono::Utc::now().timestamp(),
            },
        }
    }
}

/// 灰度发布状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutStatus {
    /// 发布配置
    pub config: RolloutConfig,
    /// 当前状态
    pub state: RolloutState,
    /// 当前阶段索引
    pub current_stage: u32,
    /// 所有阶段状态
    pub stages: Vec<RolloutStageStatus>,
    /// 解析的所有目标Agent
    pub all_target_agents: Vec<AgentId>,
    /// 更新时间
    pub updated_at: i64,
    /// 错误信息（如果有）
    pub error_message: Option<String>,
    /// 当前动作（如回滚命令）
    pub current_action: Option<String>,
}

impl RolloutStatus {
    pub fn new(config: RolloutConfig, all_target_agents: Vec<AgentId>) -> Self {
        let stages = Self::create_initial_stages(&config, &all_target_agents);

        Self {
            config,
            state: RolloutState::Created,
            current_stage: 0,
            stages,
            all_target_agents,
            updated_at: chrono::Utc::now().timestamp(),
            error_message: None,
            current_action: None,
        }
    }

    /// 根据策略创建初始阶段
    fn create_initial_stages(
        config: &RolloutConfig,
        all_agents: &[AgentId],
    ) -> Vec<RolloutStageStatus> {
        let mut stages = Vec::new();
        let total_agents = all_agents.len();

        match &config.strategy {
            RolloutStrategy::Percentage {
                stages: percentages,
            } => {
                let mut remaining_agents: Vec<_> = all_agents.iter().cloned().collect();

                for (i, &percentage) in percentages.iter().enumerate() {
                    let target_count = if percentage == 100 {
                        remaining_agents.len()
                    } else {
                        (total_agents * percentage as usize / 100).max(1)
                    };

                    let stage_agents: Vec<_> = remaining_agents
                        .drain(..target_count.min(remaining_agents.len()))
                        .collect();

                    stages.push(RolloutStageStatus {
                        stage_index: i as u32,
                        stage_name: format!("阶段{} ({}%)", i + 1, percentage),
                        target_agents: stage_agents,
                        batch_id: None,
                        started_count: 0,
                        completed_count: 0,
                        failed_count: 0,
                        state: RolloutState::Created,
                        started_at: None,
                        completed_at: None,
                        failed_executions: Vec::new(),
                        version_snapshot: None,
                    });

                    if remaining_agents.is_empty() {
                        break;
                    }
                }
            }
            RolloutStrategy::Count { stages: counts } => {
                let mut remaining_agents: Vec<_> = all_agents.iter().cloned().collect();

                for (i, &count) in counts.iter().enumerate() {
                    let target_count = if count == 0 {
                        remaining_agents.len()
                    } else {
                        count as usize
                    };

                    let stage_agents: Vec<_> = remaining_agents
                        .drain(..target_count.min(remaining_agents.len()))
                        .collect();

                    stages.push(RolloutStageStatus {
                        stage_index: i as u32,
                        stage_name: format!("阶段{} ({}个节点)", i + 1, stage_agents.len()),
                        target_agents: stage_agents,
                        batch_id: None,
                        started_count: 0,
                        completed_count: 0,
                        failed_count: 0,
                        state: RolloutState::Created,
                        started_at: None,
                        completed_at: None,
                        failed_executions: Vec::new(),
                        version_snapshot: None,
                    });

                    if remaining_agents.is_empty() {
                        break;
                    }
                }
            }
            RolloutStrategy::Groups { groups } => {
                // 基于分组的策略，按照agent数量平均分组
                let agents_per_group = (total_agents + groups.len() - 1) / groups.len();
                let mut remaining_agents: Vec<_> = all_agents.iter().cloned().collect();

                for (i, group_name) in groups.iter().enumerate() {
                    let stage_agents: Vec<_> = remaining_agents
                        .drain(..agents_per_group.min(remaining_agents.len()))
                        .collect();

                    stages.push(RolloutStageStatus {
                        stage_index: i as u32,
                        stage_name: format!("组: {}", group_name),
                        target_agents: stage_agents,
                        batch_id: None,
                        started_count: 0,
                        completed_count: 0,
                        failed_count: 0,
                        state: RolloutState::Created,
                        started_at: None,
                        completed_at: None,
                        failed_executions: Vec::new(),
                        version_snapshot: None,
                    });

                    if remaining_agents.is_empty() {
                        break;
                    }
                }
            }
        }

        stages
    }

    /// 获取当前阶段状态
    pub fn current_stage_status(&self) -> Option<&RolloutStageStatus> {
        self.stages.get(self.current_stage as usize)
    }

    /// 获取当前阶段状态（可变）
    pub fn current_stage_status_mut(&mut self) -> Option<&mut RolloutStageStatus> {
        self.stages.get_mut(self.current_stage as usize)
    }

    /// 检查是否可以推进到下一阶段
    pub fn can_advance(&self) -> bool {
        if !self.state.can_advance() {
            return false;
        }

        if let Some(current) = self.current_stage_status() {
            // 如果当前阶段是 Created 状态，可以推进（开始第一个阶段）
            // 如果当前阶段是 Completed 状态，可以推进到下一阶段
            matches!(
                current.state,
                RolloutState::Created | RolloutState::Completed
            )
        } else {
            false
        }
    }

    /// 检查整个发布是否完成
    pub fn is_completed(&self) -> bool {
        self.current_stage as usize >= self.stages.len()
            && self
                .stages
                .iter()
                .all(|s| s.state == RolloutState::Completed)
    }

    /// 计算总体进度百分比
    pub fn overall_progress(&self) -> f64 {
        let total_agents = self.all_target_agents.len() as f64;
        if total_agents == 0.0 {
            return 100.0;
        }

        let completed_agents: u32 = self.stages.iter().map(|s| s.completed_count).sum();

        (completed_agents as f64 / total_agents) * 100.0
    }
}

/// 灰度发布请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRolloutRequest {
    pub name: String,
    pub target: SelectorExpression,
    pub strategy: RolloutStrategy,
    pub task_type: RolloutTaskType,
    pub auto_advance: bool,
    pub advance_interval_seconds: u32,
}

impl CreateRolloutRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("名称不能为空".to_string());
        }

        match &self.strategy {
            RolloutStrategy::Percentage { stages } => {
                if stages.is_empty() {
                    return Err("百分比策略至少需要一个阶段".to_string());
                }
                if stages.iter().any(|&p| p == 0 || p > 100) {
                    return Err("百分比必须在1-100之间".to_string());
                }
                if stages.last() != Some(&100) {
                    return Err("最后一个阶段必须是100%".to_string());
                }
            }
            RolloutStrategy::Count { stages } => {
                if stages.is_empty() {
                    return Err("计数策略至少需要一个阶段".to_string());
                }
            }
            RolloutStrategy::Groups { groups } => {
                if groups.is_empty() {
                    return Err("分组策略至少需要一个分组".to_string());
                }
            }
        }

        match &self.task_type {
            RolloutTaskType::Command {
                command,
                timeout_seconds,
                ..
            } => {
                if command.trim().is_empty() {
                    return Err("命令不能为空".to_string());
                }
                if *timeout_seconds == 0 {
                    return Err("超时时间必须大于0".to_string());
                }
            }
            RolloutTaskType::FileDeployment { config, .. } => {
                config
                    .validate()
                    .map_err(|e| format!("文件配置无效: {}", e))?;
            }
        }

        Ok(())
    }
}
