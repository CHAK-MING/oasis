use async_nats::Client as NatsClient;
use std::path::PathBuf;

/// 配置源类型
#[derive(Debug, Clone)]
pub enum ConfigSource {
    /// 文件配置源
    File(PathBuf),
    /// NATS KV 配置源
    NatsKv {
        client: NatsClient,
        bucket: String,
        prefix: String,
        watch: bool,
    },
    /// 默认值配置源
    Defaults,
}

/// 配置加载选项
#[derive(Debug, Clone)]
pub struct ConfigLoadOptions {
    /// 是否验证配置
    pub validate: bool,
    /// 合并策略
    pub merge_strategy: MergeStrategy,
    /// 文件不存在时是否失败
    pub fail_on_missing_file: bool,
    /// 是否允许部分配置
    pub allow_partial: bool,
    /// 配置优先级（数字越大优先级越高）
    pub priority: u8,
}

/// 配置合并策略
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    /// 覆盖策略：后面的配置覆盖前面的
    Override,
}

impl ConfigSource {
    /// 获取配置源的优先级
    pub fn priority(&self) -> u8 {
        match self {
            ConfigSource::Defaults => 0,
            ConfigSource::File(_) => 1,
            ConfigSource::NatsKv { .. } => 2,
        }
    }

    /// 获取配置源的描述
    pub fn description(&self) -> String {
        match self {
            ConfigSource::Defaults => "defaults".to_string(),
            ConfigSource::File(path) => format!("file:{}", path.display()),
            ConfigSource::NatsKv { bucket, prefix, .. } => {
                format!("nats_kv:{}/{}", bucket, prefix)
            }
        }
    }

    /// 检查配置源是否可用
    pub async fn is_available(&self) -> bool {
        match self {
            ConfigSource::Defaults => true,
            ConfigSource::File(path) => path.exists(),
            ConfigSource::NatsKv { client, bucket, .. } => {
                // 检查 NATS 连接和 bucket 是否存在
                let js = async_nats::jetstream::new(client.clone());
                js.get_key_value(bucket).await.is_ok()
            }
        }
    }
}

impl ConfigLoadOptions {
    /// 创建默认选项
    pub fn default() -> Self {
        Self {
            validate: true,
            merge_strategy: MergeStrategy::Override,
            fail_on_missing_file: false,
            allow_partial: false,
            priority: 0,
        }
    }

    /// 设置验证选项
    pub fn with_validation(mut self, validate: bool) -> Self {
        self.validate = validate;
        self
    }

    /// 设置合并策略
    pub fn with_merge_strategy(mut self, strategy: MergeStrategy) -> Self {
        self.merge_strategy = strategy;
        self
    }

    /// 设置文件不存在时的行为
    pub fn with_fail_on_missing_file(mut self, fail: bool) -> Self {
        self.fail_on_missing_file = fail;
        self
    }

    /// 设置是否允许部分配置
    pub fn with_allow_partial(mut self, allow: bool) -> Self {
        self.allow_partial = allow;
        self
    }

    /// 设置优先级
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// 配置合并工具
pub struct ConfigMerger;

impl ConfigMerger {
    /// 合并多个配置值
    pub fn merge(
        target: &mut serde_json::Value,
        source: &serde_json::Value,
        strategy: &MergeStrategy,
    ) -> crate::error::Result<()> {
        match strategy {
            MergeStrategy::Override => {
                *target = source.clone();
                Ok(())
            }
        }
    }
}
