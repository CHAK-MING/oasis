//! 统一配置管理模块
//!
//! 提供跨组件的统一配置加载、验证和热更新功能。

pub mod common;
pub mod loader;
pub mod sources;
pub mod traits;
pub mod validation;
pub mod watcher;

// 重导出主要类型
pub use common::{CommonConfig, NatsConfig, TelemetryConfig, TlsConfig};
pub use loader::ConfigLoader;
pub use sources::{ConfigLoadOptions, ConfigSource, MergeStrategy};

pub use traits::ComponentConfig;
pub use watcher::{ConfigListener, ConfigProvider, ConfigWatcher, DefaultConfigListener};

// 便捷的配置加载函数
pub async fn load_config<T: ComponentConfig>(
    sources: &[ConfigSource],
    options: ConfigLoadOptions,
) -> crate::error::Result<T> {
    ConfigLoader::new()
        .with_options(options)
        .add_sources(sources)
        .load()
        .await
}

// 默认配置加载选项
impl Default for ConfigLoadOptions {
    fn default() -> Self {
        Self {
            validate: true,
            merge_strategy: MergeStrategy::Override,
            fail_on_missing_file: false,
            allow_partial: false,
            priority: 0,
        }
    }
}

impl Default for MergeStrategy {
    fn default() -> Self {
        Self::Override
    }
}
