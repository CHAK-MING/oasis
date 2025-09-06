use crate::infrastructure::monitor::agent_info_monitor::AgentInfoMonitor;
use crate::infrastructure::monitor::heartbeat_monitor::HeartbeatMonitor;
use oasis_core::agent_types::AgentInfo;
use oasis_core::core_types::AgentId;
use oasis_core::error::{CoreError, ErrorSeverity, Result as CoreResult};
use pest::Parser;
use roaring::RoaringBitmap;
use std::sync::Arc;
use tracing::debug;

#[derive(pest_derive::Parser)]
#[grammar_inline = r#"
WHITESPACE = _{ " " | "\t" | "\r" | "\n" }

selector = { SOI ~ (sys_eq | label_eq | in_groups | all_true | id_list) ~ EOI }

all_true = { ("all" | "true") }

id_list = { id ~ (comma ~ id)* }
comma = _{ "," }

id_char = _{ ASCII_ALPHANUMERIC | "-" | "_" | "." }
id = @{ id_char+ }

sys_eq = { "system" ~ "[" ~ string ~ "]" ~ "==" ~ string }
label_eq = { "labels" ~ "[" ~ string ~ "]" ~ "==" ~ string }
in_groups = { string ~ "in" ~ "groups" }

string = @{ dquote ~ (!dquote ~ ANY)* ~ dquote | squote ~ (!squote ~ ANY)* ~ squote }
dquote = _{ "\"" }
squote = _{ "'" }
"#]
struct SelectorParser;

pub struct SelectorEngine {
    heartbeat: Arc<HeartbeatMonitor>,
    agent_info: Arc<AgentInfoMonitor>,
}

impl SelectorEngine {
    pub fn new(heartbeat: Arc<HeartbeatMonitor>, agent_info: Arc<AgentInfoMonitor>) -> Self {
        Self {
            heartbeat,
            agent_info,
        }
    }

    pub fn get_agent_info(&self, agent_id: &AgentId) -> Option<AgentInfo> {
        self.agent_info.get_info(agent_id).map(|arc| (*arc).clone())
    }

    pub async fn resolve_all(&self, expression: &str) -> CoreResult<Vec<AgentId>> {
        let bitmap = self.evaluate_to_bitmap(expression)?;
        Ok(self.agent_info.bitmap_to_agent_ids(&bitmap))
    }

    pub async fn resolve_online(&self, expression: &str) -> CoreResult<Vec<AgentId>> {
        let all = self.resolve_all(expression).await?;
        let filtered = self.heartbeat.filter_online_agents(all);
        Ok(filtered)
    }

    fn evaluate_to_bitmap(&self, expression: &str) -> CoreResult<RoaringBitmap> {
        debug!("selector_evaluate expr={}", expression);
        let mut pairs = SelectorParser::parse(Rule::selector, expression).map_err(|e| {
            CoreError::InvalidTask {
                reason: format!("Invalid selector: {}", e),
                severity: ErrorSeverity::Error,
            }
        })?;

        let expr_pair = pairs.next().ok_or_else(|| CoreError::InvalidTask {
            reason: "Empty selector".to_string(),
            severity: ErrorSeverity::Error,
        })?;
        if pairs.next().is_some() {
            return Err(CoreError::InvalidTask {
                reason: "Ambiguous selector expression".to_string(),
                severity: ErrorSeverity::Error,
            });
        }

        // Descend into the inner alternative of selector
        let mut inner_iter = expr_pair.into_inner();
        let pair = inner_iter.next().ok_or_else(|| CoreError::InvalidTask {
            reason: "Invalid selector structure".to_string(),
            severity: ErrorSeverity::Error,
        })?;

        fn unquote(s: &str) -> String {
            s.trim_matches('"').trim_matches('\'').to_string()
        }

        debug!("selector_rule={:?}", pair.as_rule());
        match pair.as_rule() {
            Rule::all_true => Ok(self.union_all_agents()),
            Rule::id_list => {
                let mut ids: Vec<String> = Vec::new();
                for p in pair.into_inner() {
                    if p.as_rule() == Rule::id {
                        ids.push(p.as_str().to_string());
                    }
                }
                let bm = self.ids_to_bitmap(&ids);
                debug!("ids_to_bitmap size={}", bm.len());
                Ok(bm)
            }
            Rule::sys_eq => {
                let mut it = pair.into_inner();
                let k = unquote(it.next().unwrap().as_str());
                let v = unquote(it.next().unwrap().as_str());
                let bm = self.lookup_system(&k, &v);
                debug!("lookup_system key='{}' val='{}' size={}", k, v, bm.len());
                Ok(bm)
            }
            Rule::label_eq => {
                let mut it = pair.into_inner();
                let k = unquote(it.next().unwrap().as_str());
                let v = unquote(it.next().unwrap().as_str());
                let bm = self.lookup_label(&k, &v);
                debug!("lookup_label key='{}' val='{}' size={}", k, v, bm.len());
                if bm.is_empty() {
                    // 打印样本键值帮助诊断
                    let mut sample = 0usize;
                    for entry in self.agent_info.snapshot_labels_index().iter() {
                        if sample >= 5 {
                            break;
                        }
                        let (lk, lv) = entry.key();
                        debug!(
                            "indexed_label key='{}' val='{}' size={}",
                            lk,
                            lv,
                            entry.value().len()
                        );
                        sample += 1;
                    }
                }
                Ok(bm)
            }
            Rule::in_groups => {
                let mut it = pair.into_inner();
                let g = unquote(it.next().unwrap().as_str());
                let bm = self.lookup_group(&g);
                debug!("lookup_group name='{}' size={}", g, bm.len());
                Ok(bm)
            }
            _ => Err(CoreError::InvalidTask {
                reason: "Unsupported selector".into(),
                severity: ErrorSeverity::Error,
            }),
        }
    }

    fn union_all_agents(&self) -> RoaringBitmap {
        let mut acc = RoaringBitmap::new();
        for entry in self.agent_info.snapshot_labels_index().iter() {
            acc |= entry.value().clone();
        }
        for entry in self.agent_info.snapshot_system_index().iter() {
            acc |= entry.value().clone();
        }
        for entry in self.agent_info.snapshot_groups_index().iter() {
            acc |= entry.value().clone();
        }
        acc
    }

    fn ids_to_bitmap(&self, ids: &[String]) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        for id in ids {
            if let Some(id32) = self.agent_info.get_id32(&AgentId::from(id.clone())) {
                bm.insert(id32);
            }
        }
        bm
    }

    fn lookup_label(&self, key: &str, value: &str) -> RoaringBitmap {
        self.agent_info
            .snapshot_labels_index()
            .get(&(key.to_string(), value.to_string()))
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }

    fn lookup_system(&self, key: &str, value: &str) -> RoaringBitmap {
        self.agent_info
            .snapshot_system_index()
            .get(&(key.to_string(), value.to_string()))
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }

    fn lookup_group(&self, group: &str) -> RoaringBitmap {
        self.agent_info
            .snapshot_groups_index()
            .get(group)
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }
}
