//! 倒排索引模块
//!
//! 基于 Roaring 位图的索引实现

use anyhow::Result;
use arc_swap::ArcSwap;
use oasis_core::types::AgentId;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Agent 快照 - 包含 Agent 的完整属性信息
#[derive(Debug, Clone)]
pub struct AgentSnapshot {
    pub id: AgentId,
    pub labels: HashMap<String, String>,
    pub groups: Vec<String>,
    pub is_online: bool,
    pub agent_id: u32, // 内部稠密 ID，用于位图操作
}

/// 倒排索引快照 - 线程安全的只读快照
#[derive(Debug, Clone)]
pub struct IndexSnapshot {
    /// 所有已知 Agent 的位图
    pub universe: RoaringBitmap,
    /// 在线 Agent 的位图
    pub online_index: RoaringBitmap,
    /// 标签索引: key -> value -> bitmap_ids
    pub label_index: HashMap<String, HashMap<String, RoaringBitmap>>,
    /// 标签键索引: key -> bitmap_ids (存在性查询)
    pub label_key_index: HashMap<String, RoaringBitmap>,
    /// 组索引: group -> bitmap_ids
    pub group_index: HashMap<String, RoaringBitmap>,
    /// ID 映射: AgentId -> BitmapId
    pub id_to_bitmap: HashMap<AgentId, u32>,
    /// 反向映射: BitmapId -> AgentId
    pub bitmap_to_id: HashMap<u32, AgentId>,
    /// Agent 数据: BitmapId -> AgentSnapshot
    pub agent_data: HashMap<u32, AgentSnapshot>,
    /// 索引版本号
    pub epoch: u64,
}

impl IndexSnapshot {
    /// 创建空索引
    pub fn new() -> Self {
        Self {
            universe: RoaringBitmap::new(),
            online_index: RoaringBitmap::new(),
            label_index: HashMap::new(),
            label_key_index: HashMap::new(),
            group_index: HashMap::new(),
            id_to_bitmap: HashMap::new(),
            bitmap_to_id: HashMap::new(),
            agent_data: HashMap::new(),
            epoch: 0,
        }
    }

    /// 获取 Agent 数量
    pub fn agent_count(&self) -> usize {
        self.universe.len() as usize
    }

    /// 获取在线 Agent 数量
    pub fn online_agent_count(&self) -> usize {
        self.online_index.len() as usize
    }

    /// 根据标签键值获取节点位图
    pub fn get_label_bitmap(&self, key: &str, value: &str) -> RoaringBitmap {
        self.label_index
            .get(key)
            .and_then(|values| values.get(value))
            .cloned()
            .unwrap_or_else(RoaringBitmap::new)
    }

    /// 根据标签键获取节点位图（存在性查询）
    pub fn get_label_key_bitmap(&self, key: &str) -> RoaringBitmap {
        self.label_key_index
            .get(key)
            .cloned()
            .unwrap_or_else(RoaringBitmap::new)
    }

    /// 根据组名获取节点位图
    pub fn get_group_bitmap(&self, group: &str) -> RoaringBitmap {
        self.group_index
            .get(group)
            .cloned()
            .unwrap_or_else(RoaringBitmap::new)
    }

    /// 将位图转换为 AgentId 列表
    pub fn bitmap_to_agent_ids(&self, bitmap: &RoaringBitmap) -> Vec<AgentId> {
        bitmap
            .iter()
            .filter_map(|bitmap_id| self.bitmap_to_id.get(&bitmap_id).cloned())
            .collect()
    }

    /// 获取索引统计信息
    pub fn stats(&self) -> IndexStats {
        IndexStats {
            total_agents: self.agent_count(),
            online_agents: self.online_agent_count(),
            label_keys: self.label_key_index.len(),
            label_values: self.label_index.values().map(|m| m.len()).sum(),
            groups: self.group_index.len(),
            epoch: self.epoch,
        }
    }
}

/// 索引统计信息
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub total_agents: usize,
    pub online_agents: usize,
    pub label_keys: usize,
    pub label_values: usize,
    pub groups: usize,
    pub epoch: u64,
}

/// 倒排索引 - 支持并发读写
pub struct InvertedIndex {
    /// 当前快照的原子引用
    snapshot: ArcSwap<IndexSnapshot>,
    /// 下一个节点 ID
    next_node_id: std::sync::atomic::AtomicU32,
    /// 下一个版本号
    next_epoch: std::sync::atomic::AtomicU64,
}

impl InvertedIndex {
    /// 创建新的倒排索引
    pub fn new() -> Self {
        Self {
            snapshot: ArcSwap::new(Arc::new(IndexSnapshot::new())),
            next_node_id: std::sync::atomic::AtomicU32::new(1),
            next_epoch: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// 获取当前快照的只读引用
    pub fn snapshot(&self) -> Arc<IndexSnapshot> {
        self.snapshot.load_full()
    }

    /// 添加或更新 Agent
    pub fn upsert_agent(
        &self,
        agent_id: AgentId,
        labels: HashMap<String, String>,
        groups: Vec<String>,
        is_online: bool,
    ) -> Result<()> {
        let snapshot = self.snapshot.load_full();
        let node_id = if let Some(existing_id) = snapshot.id_to_bitmap.get(&agent_id) {
            *existing_id
        } else {
            self.next_node_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        };

        // 创建新的快照
        let mut new_snapshot = (*snapshot).clone();
        let epoch = self
            .next_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        new_snapshot.epoch = epoch;

        // 若该 agent 已存在，先清理旧的标签/组位图，防止快照中残留或重复
        if let Some(existing) = snapshot.agent_data.get(&node_id) {
            // 清理标签值位图与标签键位图（仅当旧项在新快照中不存在或已改变）
            for (old_key, old_val) in existing.labels.iter() {
                let need_remove_value = match labels.get(old_key) {
                    Some(new_val) => new_val != old_val,
                    None => true,
                };
                if need_remove_value {
                    if let Some(values) = new_snapshot.label_index.get_mut(old_key) {
                        if let Some(bitmap) = values.get_mut(old_val) {
                            bitmap.remove(node_id);
                            if bitmap.is_empty() {
                                values.remove(old_val);
                            }
                        }
                        if values.is_empty() {
                            new_snapshot.label_index.remove(old_key);
                        }
                    }
                }

                // 当 key 完全不存在于新标签中时，从 label_key_index 去除该 node
                if !labels.contains_key(old_key) {
                    if let Some(bitmap) = new_snapshot.label_key_index.get_mut(old_key) {
                        bitmap.remove(node_id);
                        if bitmap.is_empty() {
                            new_snapshot.label_key_index.remove(old_key);
                        }
                    }
                }
            }

            // 清理组位图（仅当旧组不在新组集合中）
            for old_group in existing.groups.iter() {
                if !groups.contains(old_group) {
                    if let Some(bitmap) = new_snapshot.group_index.get_mut(old_group) {
                        bitmap.remove(node_id);
                        if bitmap.is_empty() {
                            new_snapshot.group_index.remove(old_group);
                        }
                    }
                }
            }
        }

        // 更新 Agent 数据
        let agent_snapshot = AgentSnapshot {
            id: agent_id.clone(),
            labels: labels.clone(),
            groups: groups.clone(),
            is_online,
            agent_id: node_id,
        };

        new_snapshot.agent_data.insert(node_id, agent_snapshot);
        new_snapshot.id_to_bitmap.insert(agent_id.clone(), node_id);
        new_snapshot.bitmap_to_id.insert(node_id, agent_id.clone());

        // 更新位图索引
        self.update_bitmaps(&mut new_snapshot, node_id, &labels, &groups, is_online);

        // 原子切换快照
        self.snapshot.store(Arc::new(new_snapshot));

        debug!(agent_id = %agent_id, node_id = node_id, epoch = epoch, "Agent upserted");
        Ok(())
    }

    /// 更新位图索引
    fn update_bitmaps(
        &self,
        snapshot: &mut IndexSnapshot,
        node_id: u32,
        labels: &HashMap<String, String>,
        groups: &[String],
        is_online: bool,
    ) {
        // 更新 universe
        snapshot.universe.insert(node_id);

        // 更新在线状态
        if is_online {
            snapshot.online_index.insert(node_id);
        } else {
            snapshot.online_index.remove(node_id);
        }

        // 更新标签索引
        for (key, value) in labels {
            // 更新标签键索引
            snapshot
                .label_key_index
                .entry(key.clone())
                .or_insert_with(RoaringBitmap::new)
                .insert(node_id);

            // 更新标签值索引
            snapshot
                .label_index
                .entry(key.clone())
                .or_insert_with(HashMap::new)
                .entry(value.clone())
                .or_insert_with(RoaringBitmap::new)
                .insert(node_id);
        }

        // 更新组索引
        for group in groups {
            snapshot
                .group_index
                .entry(group.clone())
                .or_insert_with(RoaringBitmap::new)
                .insert(node_id);
        }
    }

    /// 移除 Agent
    pub fn remove_agent(&self, agent_id: &AgentId) -> Result<()> {
        let snapshot = self.snapshot.load_full();
        let node_id = if let Some(id) = snapshot.id_to_bitmap.get(agent_id) {
            *id
        } else {
            return Ok(()); // 节点不存在，无需处理
        };

        // 创建新的快照
        let mut new_snapshot = (*snapshot).clone();
        let epoch = self
            .next_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        new_snapshot.epoch = epoch;

        // 从所有索引中移除节点
        new_snapshot.universe.remove(node_id);
        new_snapshot.online_index.remove(node_id);

        // 从标签索引中移除
        if let Some(agent_data) = snapshot.agent_data.get(&node_id) {
            for (key, value) in &agent_data.labels {
                // 从标签值索引中移除
                if let Some(values) = new_snapshot.label_index.get_mut(key) {
                    if let Some(bitmap) = values.get_mut(value) {
                        bitmap.remove(node_id);
                        if bitmap.is_empty() {
                            values.remove(value);
                        }
                    }
                    if values.is_empty() {
                        new_snapshot.label_index.remove(key);
                    }
                }

                // 从标签键索引中移除
                if let Some(bitmap) = new_snapshot.label_key_index.get_mut(key) {
                    bitmap.remove(node_id);
                    if bitmap.is_empty() {
                        new_snapshot.label_key_index.remove(key);
                    }
                }
            }

            // 从组索引中移除
            for group in &agent_data.groups {
                if let Some(bitmap) = new_snapshot.group_index.get_mut(group) {
                    bitmap.remove(node_id);
                    if bitmap.is_empty() {
                        new_snapshot.group_index.remove(group);
                    }
                }
            }
        }

        // 清理映射
        new_snapshot.agent_data.remove(&node_id);
        new_snapshot.id_to_bitmap.remove(agent_id);
        new_snapshot.bitmap_to_id.remove(&node_id);

        // 原子切换快照
        self.snapshot.store(Arc::new(new_snapshot));

        debug!(agent_id = %agent_id, node_id = node_id, epoch = epoch, "Agent removed");
        Ok(())
    }

    /// 获取索引统计信息
    pub fn stats(&self) -> IndexStats {
        self.snapshot.load_full().stats()
    }

    /// 执行位图集合运算
    pub fn execute_bitmap_operation(&self, operation: BitmapOperation) -> RoaringBitmap {
        let snapshot = self.snapshot.load_full();
        operation.execute(&snapshot)
    }
}

/// 位图操作枚举
#[derive(Debug, Clone)]
pub enum BitmapOperation {
    /// 获取所有节点
    Universe,
    /// 获取在线节点
    Online,
    /// 标签等值查询
    LabelEquals(String, String),
    /// 标签键存在性查询
    LabelKeyExists(String),
    /// 组包含查询
    GroupContains(String),
    /// ID 列表查询
    IdList(Vec<AgentId>),
    /// 位图交集
    And(Box<BitmapOperation>, Box<BitmapOperation>),
    /// 位图并集
    Or(Box<BitmapOperation>, Box<BitmapOperation>),
    /// 位图差集
    Not(Box<BitmapOperation>),
}

impl BitmapOperation {
    /// 执行位图操作
    pub fn execute(&self, snapshot: &IndexSnapshot) -> RoaringBitmap {
        match self {
            BitmapOperation::Universe => snapshot.universe.clone(),
            BitmapOperation::Online => snapshot.online_index.clone(),
            BitmapOperation::LabelEquals(key, value) => snapshot.get_label_bitmap(key, value),
            BitmapOperation::LabelKeyExists(key) => snapshot.get_label_key_bitmap(key),
            BitmapOperation::GroupContains(group) => snapshot.get_group_bitmap(group),
            BitmapOperation::IdList(agent_ids) => {
                let mut bitmap = RoaringBitmap::new();
                for agent_id in agent_ids {
                    if let Some(bitmap_id) = snapshot.id_to_bitmap.get(agent_id) {
                        bitmap.insert(*bitmap_id);
                    }
                }
                bitmap
            }
            BitmapOperation::And(left, right) => {
                let left_bitmap = left.execute(snapshot);
                let right_bitmap = right.execute(snapshot);
                left_bitmap & right_bitmap
            }
            BitmapOperation::Or(left, right) => {
                let left_bitmap = left.execute(snapshot);
                let right_bitmap = right.execute(snapshot);
                left_bitmap | right_bitmap
            }
            BitmapOperation::Not(operand) => {
                let operand_bitmap = operand.execute(snapshot);
                &snapshot.universe - &operand_bitmap
            }
        }
    }

    /// 估算操作的成本（位图基数）
    pub fn estimate_cost(&self, snapshot: &IndexSnapshot) -> usize {
        match self {
            BitmapOperation::Universe => snapshot.universe.len() as usize,
            BitmapOperation::Online => snapshot.online_index.len() as usize,
            BitmapOperation::LabelEquals(key, value) => {
                snapshot.get_label_bitmap(key, value).len() as usize
            }
            BitmapOperation::LabelKeyExists(key) => {
                snapshot.get_label_key_bitmap(key).len() as usize
            }
            BitmapOperation::GroupContains(group) => {
                snapshot.get_group_bitmap(group).len() as usize
            }
            BitmapOperation::IdList(agent_ids) => agent_ids.len(),
            BitmapOperation::And(left, right) => {
                let left_cost = left.estimate_cost(snapshot);
                let right_cost = right.estimate_cost(snapshot);
                std::cmp::min(left_cost, right_cost) // 交集基数不会超过较小的操作数
            }
            BitmapOperation::Or(left, right) => {
                let left_cost = left.estimate_cost(snapshot);
                let right_cost = right.estimate_cost(snapshot);
                left_cost + right_cost // 并集基数不会超过操作数之和
            }
            BitmapOperation::Not(operand) => {
                let operand_cost = operand.estimate_cost(snapshot);
                snapshot.universe.len() as usize - operand_cost
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_index_creation() {
        let index = InvertedIndex::new();
        let stats = index.stats();
        assert_eq!(stats.total_agents, 0);
        assert_eq!(stats.online_agents, 0);
    }

    #[test]
    fn test_node_upsert() {
        let index = InvertedIndex::new();
        let agent_id = AgentId::from("test-agent");
        let mut labels = HashMap::new();
        labels.insert("role".to_string(), "web".to_string());
        labels.insert("env".to_string(), "prod".to_string());
        let groups = vec!["frontend".to_string()];

        index
            .upsert_agent(agent_id.clone(), labels, groups, true)
            .unwrap();

        let snapshot = index.snapshot();
        assert_eq!(snapshot.agent_count(), 1);
        assert_eq!(snapshot.online_agent_count(), 1);
        assert_eq!(snapshot.label_key_index.len(), 2); // role, env
        assert_eq!(snapshot.group_index.len(), 1); // frontend
    }

    #[test]
    fn test_bitmap_operations() {
        let index = InvertedIndex::new();

        // 添加测试节点
        let agent1 = AgentId::from("agent1");
        let agent2 = AgentId::from("agent2");

        let mut labels1 = HashMap::new();
        labels1.insert("role".to_string(), "web".to_string());
        labels1.insert("env".to_string(), "prod".to_string());

        let mut labels2 = HashMap::new();
        labels2.insert("role".to_string(), "db".to_string());
        labels2.insert("env".to_string(), "prod".to_string());

        index
            .upsert_agent(agent1, labels1, vec!["frontend".to_string()], true)
            .unwrap();
        index
            .upsert_agent(agent2, labels2, vec!["backend".to_string()], true)
            .unwrap();

        let snapshot = index.snapshot();

        // 测试标签查询
        let web_bitmap =
            BitmapOperation::LabelEquals("role".to_string(), "web".to_string()).execute(&snapshot);
        assert_eq!(web_bitmap.len(), 1);

        // 测试组查询
        let frontend_bitmap =
            BitmapOperation::GroupContains("frontend".to_string()).execute(&snapshot);
        assert_eq!(frontend_bitmap.len(), 1);

        // 测试交集
        let _prod_bitmap =
            BitmapOperation::LabelEquals("env".to_string(), "prod".to_string()).execute(&snapshot);
        let web_and_prod = BitmapOperation::And(
            Box::new(BitmapOperation::LabelEquals(
                "role".to_string(),
                "web".to_string(),
            )),
            Box::new(BitmapOperation::LabelEquals(
                "env".to_string(),
                "prod".to_string(),
            )),
        )
        .execute(&snapshot);
        assert_eq!(web_and_prod.len(), 1);
    }
}
