use std::fmt;

// ===== 为 proto 强类型实现必要的 trait =====

impl fmt::Display for crate::proto::AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl fmt::Display for crate::proto::TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl fmt::Display for crate::proto::RolloutId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

// ===== 转换工具 =====

impl From<String> for crate::proto::AgentId {
    fn from(value: String) -> Self {
        Self { value }
    }
}

impl From<&str> for crate::proto::AgentId {
    fn from(value: &str) -> Self {
        Self { value: value.to_string() }
    }
}

impl From<String> for crate::proto::TaskId {
    fn from(value: String) -> Self {
        Self { value }
    }
}

impl From<&str> for crate::proto::TaskId {
    fn from(value: &str) -> Self {
        Self { value: value.to_string() }
    }
}

impl From<String> for crate::proto::RolloutId {
    fn from(value: String) -> Self {
        Self { value }
    }
}

impl From<&str> for crate::proto::RolloutId {
    fn from(value: &str) -> Self {
        Self { value: value.to_string() }
    }
}

// ===== 解包工具 =====

impl crate::proto::AgentId {
    pub fn as_str(&self) -> &str {
        &self.value
    }
    
    pub fn to_string(&self) -> String {
        self.value.clone()
    }
}

impl crate::proto::TaskId {
    pub fn as_str(&self) -> &str {
        &self.value
    }
    
    pub fn to_string(&self) -> String {
        self.value.clone()
    }
}

impl crate::proto::RolloutId {
    pub fn as_str(&self) -> &str {
        &self.value
    }
    
    pub fn to_string(&self) -> String {
        self.value.clone()
    }
}
