use oasis_core::shutdown::{GracefulShutdown, wait_for_tasks_with_timeout};
use std::collections::HashMap;

use tokio::task::JoinHandle;
use tracing::info;

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

/// 服务信息
#[derive(Debug)]
pub struct ServiceInfo {
    pub priority: ServicePriority,
    pub handle: JoinHandle<()>,
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
    /// 注册的服务
    services: HashMap<String, ServiceInfo>,
    /// 关闭超时时间（秒）
    shutdown_timeout_secs: u64,
}

impl LifecycleManager {
    /// 创建新的生命周期管理器
    pub fn new() -> Self {
        Self {
            shutdown: GracefulShutdown::new(),
            services: HashMap::new(),
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
        self.services
            .insert(name.clone(), ServiceInfo { priority, handle });
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
        self.services.len()
    }

    /// 获取服务名称列表
    pub fn list_service_names(&self) -> Vec<String> {
        self.services.keys().cloned().collect()
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
        info!(
            "Starting graceful shutdown of {} services",
            self.services.len()
        );

        // 按优先级排序服务
        let mut services: Vec<_> = self.services.drain().collect();
        services.sort_by(|(_, a), (_, b)| a.priority.cmp(&b.priority));

        // 提取所有任务句柄
        let handles: Vec<JoinHandle<()>> = services
            .into_iter()
            .map(|(name, service_info)| {
                info!(
                    "Shutting down service: {} (priority: {:?})",
                    name, service_info.priority
                );
                service_info.handle
            })
            .collect();

        // 等待所有任务完成
        wait_for_tasks_with_timeout(handles, self.shutdown_timeout_secs).await;

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
