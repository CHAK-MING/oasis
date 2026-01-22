use oasis_core::shutdown::GracefulShutdown;
use std::collections::{HashMap, HashSet};

use tokio::task::{JoinHandle, JoinSet};
use tracing::{info, warn};

#[derive(Debug)]
struct ServiceCompletion {
    name: String,
    priority: ServicePriority,
    result: std::result::Result<(), tokio::task::JoinError>,
}

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
/// - 关闭超时处理
pub struct LifecycleManager {
    /// 优雅关闭管理器
    shutdown: GracefulShutdown,
    /// 任务集合（使用 JoinSet 管理）
    tasks: JoinSet<ServiceCompletion>,
    /// 服务元数据
    service_metadata: HashMap<String, ServicePriority>,
    /// 仍在运行/等待退出的服务集合（用于超时排障）
    remaining_services: HashSet<String>,
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
            remaining_services: HashSet::new(),
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
        self.service_metadata.insert(name.clone(), priority);
        self.remaining_services.insert(name.clone());

        self.tasks.spawn(async move {
            let result = handle.await;
            ServiceCompletion {
                name,
                priority,
                result,
            }
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

        // Safety: always notify all services to exit, even if caller invokes this
        // directly (not via signal listener).
        self.shutdown.token.cancel();

        let timeout = tokio::time::Duration::from_secs(self.shutdown_timeout_secs);
        let result = tokio::time::timeout(timeout, async {
            while let Some(join_result) = self.tasks.join_next().await {
                match join_result {
                    Ok(completion) => {
                        self.remaining_services.remove(&completion.name);

                        match completion.result {
                            Ok(()) => info!(
                                service = %completion.name,
                                priority = ?completion.priority,
                                "Service completed successfully"
                            ),
                            Err(e) if e.is_cancelled() => info!(
                                service = %completion.name,
                                priority = ?completion.priority,
                                "Service was cancelled"
                            ),
                            Err(e) if e.is_panic() => warn!(
                                service = %completion.name,
                                priority = ?completion.priority,
                                error = %e,
                                "Service panicked"
                            ),
                            Err(e) => warn!(
                                service = %completion.name,
                                priority = ?completion.priority,
                                error = %e,
                                "Service failed"
                            ),
                        }
                    }
                    Err(e) if e.is_cancelled() => {
                        info!(error = %e, "Lifecycle wrapper task was cancelled")
                    }
                    Err(e) => warn!(error = %e, "Lifecycle wrapper task failed"),
                }
            }
        })
        .await;

        if result.is_err() {
            warn!(
                remaining_services = ?self.remaining_services,
                "Graceful shutdown timeout reached, aborting remaining tasks"
            );
            self.tasks.abort_all();
        }

        if self.remaining_services.is_empty() {
            info!("All services shut down successfully");
        } else {
            warn!(
                remaining_services = ?self.remaining_services,
                "Lifecycle manager completed with remaining services"
            );
        }
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
