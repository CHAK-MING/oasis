use crate::config::{ConfigLoadOptions, ConfigSource};
use crate::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// 组件配置 trait
///
/// 所有组件的配置都应该实现这个 trait，以支持统一的配置加载、验证和热更新。
#[async_trait]
pub trait ComponentConfig:
    Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + Default + 'static
{
    /// 通用配置类型
    type Common;

    /// 从多个配置源加载配置
    async fn load_from_sources(
        sources: &[ConfigSource],
        options: ConfigLoadOptions,
    ) -> Result<Self>;

    /// 与通用配置合并
    fn merge_with_common(&mut self, common: Self::Common);

    /// 业务规则验证
    fn validate_business_rules(&self) -> Result<()> {
        Ok(())
    }

    /// 获取通用配置部分
    fn common(&self) -> &Self::Common;

    /// 获取通用配置部分的可变引用
    fn common_mut(&mut self) -> &mut Self::Common;

    /// 热更新支持
    fn can_hot_reload(&self, _other: &Self) -> bool {
        true
    }

    /// 获取配置摘要
    fn summary(&self) -> std::collections::HashMap<String, String> {
        let summary = std::collections::HashMap::new();

        // 子类可以重写此方法提供具体实现
        summary
    }

    /// 验证配置完整性
    fn validate(&self) -> Result<()> {
        // 验证业务规则
        self.validate_business_rules()?;

        Ok(())
    }
}
