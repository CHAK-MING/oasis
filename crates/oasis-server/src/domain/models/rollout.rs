use oasis_core::{error::CoreError, types::TaskSpec};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// 灰度发布策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RolloutStrategy {
    /// 金丝雀发布：先部署一小部分节点
    Canary {
        percentage: f64,
        observation_duration_secs: u64,
    },
    /// 滚动发布：分批部署
    Rolling {
        batch_size: BatchSize,
        batch_delay_secs: u64,
        max_failures: usize,
    },
    /// 蓝绿发布：全量切换
    BlueGreen {
        switch_percentage: f64,
        warmup_secs: u64,
    },
}

/// 批次大小配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BatchSize {
    Percentage(f64), // 0-100
    Count(usize),
}

impl BatchSize {
    pub fn compute(&self, total_nodes: usize) -> usize {
        match self {
            BatchSize::Percentage(pct) => {
                let pct = if *pct <= 0.0 { 0.0 } else { pct.min(100.0) };
                let size = ((total_nodes as f64) * pct / 100.0).ceil() as usize;
                size.max(1)
            }
            BatchSize::Count(n) => (*n).max(1),
        }
    }
}

/// 灰度发布状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RolloutState {
    Created,
    /// 正在执行当前批次
    RunningBatch {
        current_batch: usize,
    },
    /// 批次间等待
    WaitingForNextBatch {
        batch_completed_at: i64,
    },
    Paused {
        reason: String,
    },
    Succeeded,
    Failed {
        error: String,
    },
    Aborted {
        reason: String,
    },
    RollingBack {
        reason: String,
    },
}

/// 发布进度
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutProgress {
    pub total_agents: usize,
    pub processed_agents: usize,
    pub successful_agents: usize,
    pub failed_agents: usize,
    pub completion_rate: f64,
    pub current_batch: Option<usize>,
    pub total_batches: usize,
}

/// 批次结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResult {
    pub batch_index: usize,
    pub agent_count: usize,
    pub successful_count: usize,
    pub failed_count: usize,
    pub duration_secs: u64,
    pub completed_at: i64,
}

/// 灰度发布配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutConfig {
    pub strategy: RolloutStrategy,
    pub timeout_seconds: u64,
    pub auto_advance: bool,
    pub labels: HashMap<String, String>,
}

/// 灰度发布聚合根
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rollout {
    pub id: String,
    pub name: String,
    pub task: TaskSpec,
    pub target_selector: String,
    pub config: RolloutConfig,
    pub state: RolloutState,
    pub progress: RolloutProgress,
    pub batch_results: Vec<BatchResult>,
    /// 已处理的节点集合
    pub processed_agents: HashSet<String>,
    /// 当前批次正在运行的任务 (node_id -> task_id)
    pub current_batch_tasks: HashMap<String, String>,
    /// 当前批次开始时间（用于计算批次耗时）
    pub current_batch_started_at: Option<i64>,
    /// 缓存的目标节点列表（避免重复解析选择器）
    pub cached_target_agents: Option<Vec<String>>,
    pub created_at: i64,
    pub updated_at: i64,
    pub version: u64,
}

impl Rollout {
    pub fn new(
        id: String,
        name: String,
        task: TaskSpec,
        target_selector: String,
        config: RolloutConfig,
    ) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id,
            name,
            task,
            target_selector,
            config,
            state: RolloutState::Created,
            progress: RolloutProgress {
                total_agents: 0,
                processed_agents: 0,
                successful_agents: 0,
                failed_agents: 0,
                completion_rate: 0.0,
                current_batch: None,
                total_batches: 0,
            },
            batch_results: Vec::new(),
            processed_agents: HashSet::new(),
            current_batch_tasks: HashMap::new(),
            current_batch_started_at: None,
            cached_target_agents: None,
            created_at: now,
            updated_at: now,
            version: 1,
        }
    }

    pub fn start(&mut self) -> Result<(), CoreError> {
        match self.state {
            RolloutState::Created => {
                self.state = RolloutState::RunningBatch { current_batch: 0 };
                self.updated_at = chrono::Utc::now().timestamp();
                self.version += 1;
                Ok(())
            }
            _ => Err(CoreError::InvalidTask {
                reason: "Rollout is not in created state".to_string(),
            }),
        }
    }

    pub fn pause(&mut self, reason: String) -> Result<(), CoreError> {
        match self.state {
            RolloutState::RunningBatch { .. } | RolloutState::WaitingForNextBatch { .. } => {
                self.state = RolloutState::Paused { reason };
                self.updated_at = chrono::Utc::now().timestamp();
                self.version += 1;
                Ok(())
            }
            _ => Err(CoreError::InvalidTask {
                reason: "Rollout is not running".to_string(),
            }),
        }
    }

    pub fn resume(&mut self) -> Result<(), CoreError> {
        match self.state {
            RolloutState::Paused { .. } => {
                let current_batch = self.progress.current_batch.unwrap_or(0);
                self.state = RolloutState::RunningBatch { current_batch };
                self.updated_at = chrono::Utc::now().timestamp();
                self.version += 1;
                Ok(())
            }
            _ => Err(CoreError::InvalidTask {
                reason: "Rollout is not paused".to_string(),
            }),
        }
    }

    pub fn abort(&mut self, reason: String) -> Result<(), CoreError> {
        match self.state {
            RolloutState::Created
            | RolloutState::RunningBatch { .. }
            | RolloutState::WaitingForNextBatch { .. }
            | RolloutState::Paused { .. } => {
                self.state = RolloutState::Aborted { reason };
                self.updated_at = chrono::Utc::now().timestamp();
                self.version += 1;
                Ok(())
            }
            _ => Err(CoreError::InvalidTask {
                reason: "Rollout cannot be aborted in current state".to_string(),
            }),
        }
    }

    pub fn rollback(&mut self, reason: String) -> Result<(), CoreError> {
        match self.state {
            RolloutState::Succeeded | RolloutState::Failed { .. } => {
                self.state = RolloutState::RollingBack { reason };
                self.updated_at = chrono::Utc::now().timestamp();
                self.version += 1;
                Ok(())
            }
            _ => Err(CoreError::InvalidTask {
                reason: "Rollout cannot be rolled back in current state".to_string(),
            }),
        }
    }

    /// 检查是否为活动状态（需要自动化处理）
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            RolloutState::RunningBatch { .. } | RolloutState::WaitingForNextBatch { .. }
        )
    }

    /// 获取当前批次索引
    pub fn current_batch(&self) -> Option<usize> {
        match self.state {
            RolloutState::RunningBatch { current_batch } => Some(current_batch),
            RolloutState::WaitingForNextBatch { .. } => self.progress.current_batch,
            _ => None,
        }
    }

    /// 添加任务到当前批次
    pub fn add_batch_task(&mut self, node_id: String, task_id: String) {
        self.current_batch_tasks.insert(node_id, task_id);
    }

    /// 标记节点为已处理
    pub fn mark_node_processed(&mut self, node_id: String) {
        self.processed_agents.insert(node_id);
    }

    /// 检查节点是否已处理
    pub fn is_node_processed(&self, node_id: &str) -> bool {
        self.processed_agents.contains(node_id)
    }

    /// 获取批次延迟时间（秒）
    pub fn batch_delay_secs(&self) -> u64 {
        match &self.config.strategy {
            RolloutStrategy::Rolling {
                batch_delay_secs, ..
            } => *batch_delay_secs,
            _ => 0,
        }
    }

    /// 获取最大失败数
    pub fn max_failures(&self) -> usize {
        match &self.config.strategy {
            RolloutStrategy::Rolling { max_failures, .. } => *max_failures,
            _ => 0,
        }
    }
}
