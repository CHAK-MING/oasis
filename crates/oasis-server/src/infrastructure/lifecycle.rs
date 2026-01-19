use oasis_core::shutdown::GracefulShutdown;
use std::collections::HashMap;

use tokio::task::{JoinHandle, JoinSet};
use tracing::{info, warn};

/// 服务优先级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ServicePriority {
    /// 高优先级服务（如gRPC服务器）
    High = 1,
    /// 中优先级服务（如健康监控）
    Medium = 2,
    /// 低优先级服务（如后台清理任务）
    Low = 3,
}

/// 生命周期管理器
///
/// 负责统一管理所有服务的生命周期，包括：
/// - 服务注册和启动
/// - 优雅关闭处理
/// - 服务依赖管理
/// - 关闭超时处理
pub struct LifecycleManager {
    /// 优雅关闭管理器
    shutdown: GracefulShutdown,
    /// 任务集合（使用 JoinSet 管理）
    tasks: JoinSet<()>,
    /// 服务元数据
    service_metadata: HashMap<String, ServicePriority>,
    /// 关闭超时时间（秒）
    shutdown_timeout_secs: u64,
}

impl LifecycleManager {
    /// 创建新的生命周期管理器
    pub fn new() -> Self {
        Self {
            shutdown: GracefulShutdown::new(),
            tasks: JoinSet::new(),
            service_metadata: HashMap::new(),
            shutdown_timeout_secs: 30,
        }
    }

    /// 设置关闭超时时间
    pub fn with_shutdown_timeout(mut self, timeout_secs: u64) -> Self {
        self.shutdown_timeout_secs = timeout_secs;
        self
    }

    /// 获取关闭令牌
    pub fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown.token.clone()
    }

    /// 注册服务
    pub fn register_service(
        &mut self,
        name: String,
        priority: ServicePriority,
        handle: JoinHandle<()>,
    ) {
        info!("Registering service: {} (priority: {:?})", name, priority);
        self.service_metadata.insert(name, priority);
        self.tasks.spawn(async move {
            let _ = handle.await;
        });
    }

    /// 注册高优先级服务
    pub fn register_high_priority_service(&mut self, name: String, handle: JoinHandle<()>) {
        self.register_service(name, ServicePriority::High, handle);
    }

    /// 注册中优先级服务
    pub fn register_medium_priority_service(&mut self, name: String, handle: JoinHandle<()>) {
        self.register_service(name, ServicePriority::Medium, handle);
    }

    /// 注册低优先级服务
    pub fn register_low_priority_service(&mut self, name: String, handle: JoinHandle<()>) {
        self.register_service(name, ServicePriority::Low, handle);
    }

    /// 获取服务数量
    pub fn service_count(&self) -> usize {
        self.tasks.len()
    }

    /// 获取服务名称列表
    pub fn list_service_names(&self) -> Vec<String> {
        self.service_metadata.keys().cloned().collect()
    }

    /// 启动信号监听（后台任务）
    pub fn start_signal_listener(&self) {
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            shutdown.wait_for_signal().await;
        });
    }

    /// 等待关闭信号
    pub async fn wait_for_shutdown_signal(&self) {
        info!("Waiting for shutdown signal...");
        self.shutdown.cancelled().await;
        info!("Shutdown signal received");
    }

    /// 优雅关闭所有服务
    pub async fn graceful_shutdown(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let service_count = self.tasks.len();
        info!("Starting graceful shutdown of {} services", service_count);

        let timeout = tokio::time::Duration::from_secs(self.shutdown_timeout_secs);
        let result = tokio::time::timeout(timeout, async {
            while let Some(result) = self.tasks.join_next().await {
                match result {
                    Ok(_) => info!("Service completed successfully"),
                    Err(e) if e.is_cancelled() => info!("Service was cancelled"),
                    Err(e) => warn!("Service failed: {}", e),
                }
            }
        })
        .await;

        if result.is_err() {
            warn!("Graceful shutdown timeout reached, aborting remaining tasks");
            self.tasks.abort_all();
        }

        info!("All services shut down successfully");
        Ok(())
    }

    /// 运行完整的生命周期管理
    ///
    /// 这个方法会：
    /// 1. 启动信号监听
    /// 2. 等待关闭信号
    /// 3. 执行优雅关闭
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Starting lifecycle manager with {} services",
            self.service_count()
        );
        info!("Registered services: {:?}", self.list_service_names());

        // 启动信号监听
        self.start_signal_listener();

        // 等待关闭信号
        self.wait_for_shutdown_signal().await;

        // 执行优雅关闭
        self.graceful_shutdown().await?;

        info!("Lifecycle manager completed successfully");
        Ok(())
    }
}

impl Default for LifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}
