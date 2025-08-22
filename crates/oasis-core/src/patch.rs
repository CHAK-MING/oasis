use crate::types::AgentId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AttributesPatch {
    pub agent_id: AgentId,
    pub patch: json_patch::Patch,
}
