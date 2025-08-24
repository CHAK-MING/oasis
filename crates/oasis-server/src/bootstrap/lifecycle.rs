use anyhow::Result;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::infrastructure::services::leader_election::LeaderElectionService;
use crate::infrastructure::tls::TlsService;
use crate::interface::health::HealthService;

/// 服务生命周期管理器 - 统一管理所有服务的启动和停止
pub struct ServiceLifecycleManager {
    /// 优雅停机token
    shutdown_token: CancellationToken,

    /// 正在运行的服务句柄
    running_services: Vec<ServiceHandle>,

    /// 清理超时时间（秒）
    cleanup_timeout_secs: u64,
}

/// 服务句柄 - 包含服务名称和停止逻辑
pub struct ServiceHandle {
    name: String,
    handle_type: ServiceHandleType,
    /// 服务停止优先级，数字越小越优先停止
    stop_priority: u8,
}

pub enum ServiceHandleType {
    /// 异步任务句柄
    Task(JoinHandle<()>),
    /// 领导选举服务
    LeaderElection(LeaderElectionService),
    /// TLS服务（可能需要特殊处理）
    Tls(TlsService),
    /// 健康检查服务
    Health(Arc<HealthService>),
}

impl ServiceLifecycleManager {
    /// 创建新的服务生命周期管理器
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            shutdown_token,
            running_services: Vec::new(),
            cleanup_timeout_secs: 30, // 默认30秒超时
        }
    }

    /// 设置清理超时时间
    pub fn with_cleanup_timeout(mut self, timeout_secs: u64) -> Self {
        self.cleanup_timeout_secs = timeout_secs;
        self
    }

    /// 注册异步任务服务
    pub fn register_task(&mut self, name: String, handle: JoinHandle<()>, stop_priority: u8) {
        self.running_services.push(ServiceHandle {
            name,
            handle_type: ServiceHandleType::Task(handle),
            stop_priority,
        });
    }

    /// 注册多个异步任务服务
    pub fn register_tasks(
        &mut self,
        name: String,
        handles: Vec<JoinHandle<()>>,
        stop_priority: u8,
    ) {
        for (i, handle) in handles.into_iter().enumerate() {
            self.running_services.push(ServiceHandle {
                name: format!("{}_{}", name, i),
                handle_type: ServiceHandleType::Task(handle),
                stop_priority,
            });
        }
    }

    /// 注册领导选举服务
    pub fn register_leader_election(&mut self, service: LeaderElectionService) {
        self.running_services.push(ServiceHandle {
            name: "leader_election".to_string(),
            handle_type: ServiceHandleType::LeaderElection(service),
            stop_priority: 1, // 高优先级，最先停止
        });
    }

    /// 注册TLS服务
    pub fn register_tls_service(&mut self, service: TlsService) {
        self.running_services.push(ServiceHandle {
            name: "tls_service".to_string(),
            handle_type: ServiceHandleType::Tls(service),
            stop_priority: 5, // 较低优先级
        });
    }

    /// 注册健康检查服务
    pub fn register_health_service(&mut self, service: Arc<HealthService>) {
        self.running_services.push(ServiceHandle {
            name: "health_service".to_string(),
            handle_type: ServiceHandleType::Health(service),
            stop_priority: 2, // 中等优先级
        });
    }

    /// 等待关闭信号
    pub async fn wait_for_shutdown_signal(&self) {
        self.shutdown_token.cancelled().await;
        info!("Shutdown signal received, initiating graceful shutdown...");
    }

    /// 执行优雅停机，按优先级停止所有服务
    pub async fn graceful_shutdown(mut self) -> Result<()> {
        info!(
            "Starting graceful shutdown of {} services",
            self.running_services.len()
        );

        // 按优先级排序（数字小的先停止）
        self.running_services.sort_by_key(|s| s.stop_priority);

        let mut task_handles = Vec::new();

        for service in self.running_services {
            info!(
                "Stopping service: {} (priority: {})",
                service.name, service.stop_priority
            );

            match service.handle_type {
                ServiceHandleType::Task(handle) => {
                    task_handles.push(handle);
                }
                ServiceHandleType::LeaderElection(election_service) => {
                    if let Err(e) = election_service.stop_leadership().await {
                        warn!(
                            "Failed to stop leader election service {}: {}",
                            service.name, e
                        );
                    } else {
                        info!(
                            "Successfully stopped leader election service: {}",
                            service.name
                        );
                    }
                }
                ServiceHandleType::Tls(_tls_service) => {
                    // TLS服务通常不需要特殊停止逻辑
                    info!("TLS service {} marked for cleanup", service.name);
                }
                ServiceHandleType::Health(_health_service) => {
                    // 健康检查服务可能有后台任务，但通常由shutdown token控制
                    info!("Health service {} marked for cleanup", service.name);
                }
            }
        }

        // 等待所有异步任务完成
        if !task_handles.is_empty() {
            info!(
                "Waiting for {} background tasks to complete",
                task_handles.len()
            );
            oasis_core::shutdown::wait_for_tasks_with_timeout(
                task_handles,
                self.cleanup_timeout_secs,
            )
            .await;
        }

        info!("Graceful shutdown completed");
        Ok(())
    }

    /// 获取注册的服务数量
    pub fn service_count(&self) -> usize {
        self.running_services.len()
    }

    /// 列出所有注册的服务名称
    pub fn list_service_names(&self) -> Vec<String> {
        self.running_services
            .iter()
            .map(|s| s.name.clone())
            .collect()
    }
}
