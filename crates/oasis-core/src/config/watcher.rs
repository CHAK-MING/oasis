use crate::config::{ComponentConfig, ConfigSource};
use crate::error::Result;
use futures_util::stream::Stream;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::watch;

/// 配置监听器
pub struct ConfigWatcher<T: ComponentConfig> {
    config_tx: watch::Sender<T>,
    config_rx: watch::Receiver<T>,
    listeners: Arc<RwLock<Vec<Box<dyn ConfigListener<Config = T> + Send + Sync>>>>,
}

impl<T: ComponentConfig> ConfigWatcher<T> {
    /// 创建新的配置监听器
    pub fn new(initial_config: T) -> Self {
        let (tx, rx) = watch::channel(initial_config);
        Self {
            config_tx: tx,
            config_rx: rx,
            listeners: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 获取配置订阅者
    pub fn subscribe(&self) -> watch::Receiver<T> {
        self.config_rx.clone()
    }

    /// 添加配置监听器
    pub async fn add_listener(&self, listener: Box<dyn ConfigListener<Config = T> + Send + Sync>) {
        let mut listeners = self.listeners.write().await;
        listeners.push(listener);
    }

    /// 移除配置监听器
    pub async fn remove_listener(&self, index: usize) {
        let mut listeners = self.listeners.write().await;
        if index < listeners.len() {
            listeners.remove(index);
        }
    }

    /// 启动配置监听
    pub async fn start_watching(&self, sources: Vec<ConfigSource>) -> Result<()> {
        // 简化实现：只记录监听启动
        tracing::info!("Config watching started for {} sources", sources.len());
        Ok(())
    }

    /// 手动更新配置
    pub async fn update_config(&self, new_config: T) -> Result<()> {
        let old_config = self.config_tx.borrow().clone();

        // 验证新配置
        new_config.validate()?;

        // 检查是否可以热重载
        if !old_config.can_hot_reload(&new_config) {
            return Err(crate::error::CoreError::Config {
                message: "Configuration cannot be hot reloaded".to_string(),
            });
        }

        // 发送新配置
        self.config_tx
            .send(new_config.clone())
            .map_err(|e| crate::error::CoreError::Config {
                message: format!("Failed to send config update: {}", e),
            })?;

        // 通知监听器
        let listeners = self.listeners.read().await;
        for listener in listeners.iter() {
            if let Err(e) = listener.on_config_changed(&old_config, &new_config).await {
                tracing::error!("Config listener error: {}", e);
            }
        }

        Ok(())
    }

    /// 获取当前配置
    pub fn get_current_config(&self) -> T {
        self.config_rx.borrow().clone()
    }

    /// 获取配置变更流
    pub fn config_changes(&self) -> impl Stream<Item = T> {
        tokio_stream::wrappers::WatchStream::new(self.config_rx.clone())
    }
}

/// 配置监听器 trait
#[async_trait::async_trait]
pub trait ConfigListener {
    type Config: ComponentConfig;

    /// 配置变更回调
    async fn on_config_changed(
        &self,
        _old_config: &Self::Config,
        _new_config: &Self::Config,
    ) -> Result<()> {
        tracing::info!("Configuration changed");
        Ok(())
    }

    /// 配置验证失败回调
    async fn on_config_validation_failed(
        &self,
        _config: &Self::Config,
        errors: Vec<String>,
    ) -> Result<()> {
        tracing::error!("Configuration validation failed: {:?}", errors);
        Ok(())
    }

    /// 配置加载失败回调
    async fn on_config_load_failed(
        &self,
        source: &ConfigSource,
        error: &crate::error::CoreError,
    ) -> Result<()> {
        tracing::error!(
            "Configuration load failed from {}: {}",
            source.description(),
            error
        );
        Ok(())
    }
}

/// 默认配置监听器
pub struct DefaultConfigListener<T: ComponentConfig> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ComponentConfig> DefaultConfigListener<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ComponentConfig> Default for DefaultConfigListener<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl<T: ComponentConfig> ConfigListener for DefaultConfigListener<T> {
    type Config = T;

    async fn on_config_changed(
        &self,
        _old_config: &Self::Config,
        _new_config: &Self::Config,
    ) -> Result<()> {
        tracing::info!("Configuration changed");
        Ok(())
    }

    async fn on_config_validation_failed(
        &self,
        _config: &Self::Config,
        errors: Vec<String>,
    ) -> Result<()> {
        tracing::error!("Configuration validation failed:");
        for error in errors {
            tracing::error!("  - {}", error);
        }
        Ok(())
    }

    async fn on_config_load_failed(
        &self,
        source: &ConfigSource,
        error: &crate::error::CoreError,
    ) -> Result<()> {
        tracing::error!(
            "Failed to load configuration from {}: {}",
            source.description(),
            error
        );
        Ok(())
    }
}

/// 配置提供者
pub struct ConfigProvider<T: ComponentConfig> {
    watcher: Arc<ConfigWatcher<T>>,
}

impl<T: ComponentConfig> ConfigProvider<T> {
    /// 创建新的配置提供者
    pub fn new(initial_config: T) -> Self {
        Self {
            watcher: Arc::new(ConfigWatcher::new(initial_config)),
        }
    }

    /// 获取当前配置
    pub async fn get_config(&self) -> Result<T> {
        Ok(self.watcher.get_current_config())
    }

    /// 更新配置
    pub async fn update_config(&self, config: T) -> Result<()> {
        self.watcher.update_config(config).await
    }

    /// 监听配置变更
    pub async fn watch_config(&self, listener: Box<dyn ConfigListener<Config = T> + Send + Sync>) {
        self.watcher.add_listener(listener).await;
    }

    /// 获取配置监听器
    pub fn get_watcher(&self) -> Arc<ConfigWatcher<T>> {
        self.watcher.clone()
    }
}
