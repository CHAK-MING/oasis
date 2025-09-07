use crate::application::context::ApplicationContext;
use crate::infrastructure::monitor::agent_info_monitor::AgentInfoMonitor;
use crate::infrastructure::monitor::heartbeat_monitor::HeartbeatMonitor;
use crate::infrastructure::monitor::task_monitor::TaskMonitor;
use crate::infrastructure::services::{AgentService, FileService, RolloutService, TaskService};
use crate::{
    infrastructure::lifecycle::LifecycleManager, interface::server_manager::GrpcServerManager,
};
use oasis_core::config_strategies::ServerConfigStrategy;
use oasis_core::config_strategy::ConfigStrategy;
use oasis_core::error::Result;
use oasis_core::{config::OasisConfig, nats::NatsClientFactory};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, error, info};

/// 服务器启动引导器
pub struct Bootstrap;

impl Bootstrap {
    pub async fn start(config_path: Option<&str>) -> Result<()> {
        // 1. 创建配置策略
        let config_path = config_path.unwrap_or("oasis.toml");
        let strategy = ServerConfigStrategy::new(config_path)?;

        // 2. 加载初始配置
        let config = strategy.load_initial_config().await?;

        // 3. 启动热重载监控
        let config_rx = if strategy.supports_hot_reload() {
            Some(strategy.start_hot_reload().await?)
        } else {
            None
        };

        // 4. 启动服务器
        Self::start_with_config(config, config_rx).await
    }

    async fn start_with_config(
        config: OasisConfig,
        _config_rx: Option<
            tokio::sync::broadcast::Receiver<oasis_core::config_strategy::ConfigChangeEvent>,
        >,
    ) -> Result<()> {
        // 设置 panic hook
        std::panic::set_hook(Box::new(|panic_info| {
            eprintln!("PANIC occurred: {}", panic_info);
            if let Some(location) = panic_info.location() {
                eprintln!(
                    "Panic location: {}:{}:{}",
                    location.file(),
                    location.line(),
                    location.column()
                );
            }
        }));

        info!("Starting OASIS Server with simplified architecture");

        // 1. 初始化日志
        oasis_core::telemetry::init_tracing_with(&oasis_core::telemetry::LogConfig {
            level: config.telemetry.log_level.clone(),
            format: config.telemetry.log_format.clone(),
            no_ansi: config.telemetry.log_no_ansi,
        });
        info!("Telemetry initialized");

        // 2. 创建生命周期管理器
        let mut lifecycle_manager = LifecycleManager::new().with_shutdown_timeout(30);

        info!("Building ApplicationContext with simplified architecture");

        // 3. 初始化NATS连接
        debug!("Connecting to NATS...");
        let nats_client =
            NatsClientFactory::create_nats_client_with_jetstream(&config.nats, &config.tls).await?;
        let jetstream = Arc::new(nats_client.jetstream);

        // 4. 确保JetStream流和KV存储存在
        debug!("Ensuring JetStream streams and KV buckets...");
        crate::infrastructure::streams::ensure_streams(&jetstream).await?;
        debug!("JetStream streams and KV buckets ensured");

        // 5. 启动监控服务
        // 启动任务结果监听，并注册到生命周期管理器，设置到TaskService

        // 启动心跳监控
        let heartbeat_monitor = Arc::new(HeartbeatMonitor::new(
            jetstream.clone(),
            config.server.heartbeat_ttl_sec,
            lifecycle_manager.shutdown_token(),
        ));
        let heartbeat_monitor_handle = heartbeat_monitor.clone().spawn();

        // 启动 AgentInfo 监控
        let agent_info_monitor = Arc::new(AgentInfoMonitor::new(
            jetstream.clone(),
            lifecycle_manager.shutdown_token(),
        ));
        let agent_info_monitor_handle = agent_info_monitor.clone().spawn();

        // 6. 创建服务层
        debug!("Initializing services...");

        // 启动 TaskMonitor
        let task_monitor = Arc::new(TaskMonitor::new(
            jetstream.clone(),
            lifecycle_manager.shutdown_token(),
        ));
        let task_monitor_handle = task_monitor.clone().spawn();

        let task_service =
            Arc::new(TaskService::new(jetstream.clone(), task_monitor.clone()).await?);
        debug!("TaskService initialized");

        let file_service = Arc::new(FileService::new(jetstream.clone()).await?);
        debug!("FileService initialized");

        let agent_service = Arc::new(AgentService::new(
            jetstream.clone(),
            heartbeat_monitor.clone(),
            agent_info_monitor.clone(),
            lifecycle_manager.shutdown_token(),
        ));
        debug!("AgentService initialized");

        let rollout_service =
            Arc::new(RolloutService::new(jetstream.clone(), task_monitor.clone()).await?);
        debug!("RolloutService initialized");

        // 7. 构造ApplicationContext
        let app_context = Arc::new(ApplicationContext {
            agent_service: agent_service.clone(),
            task_service,
            file_service,
            rollout_service,
        });

        info!("Application context built successfully");

        // 8. 解析地址
        let bind_addr: SocketAddr =
            config
                .listen_addr
                .parse()
                .map_err(|e| oasis_core::error::CoreError::Config {
                    message: format!("Invalid bind address: {}", e),
                    severity: oasis_core::error::ErrorSeverity::Error,
                })?;

        // 9. 启用进程内 TLS（原生 TLS）
        let tls_service: Option<std::sync::Arc<crate::infrastructure::tls::TlsService>> =
            match crate::infrastructure::tls::TlsService::new_with_paths(config.tls.certs_dir).await
            {
                Ok(svc) => Some(std::sync::Arc::new(svc)),
                Err(e) => {
                    tracing::warn!(
                        "Failed to initialize TLS service: {}, falling back to plaintext",
                        e
                    );
                    None
                }
            };

        // 10. 启动 gRPC 服务，同样注册到生命周期管理
        info!("Starting gRPC server manager...");
        let server_manager_handle = {
            let app_context = app_context.clone();
            let tls_service_clone = tls_service.clone();
            let shutdown_token = lifecycle_manager.shutdown_token();

            tokio::spawn(async move {
                info!("gRPC server manager task started");

                // 创建服务工厂函数
                let make_service = || {
                    info!("Creating new gRPC service instance");
                    match Self::create_grpc_service(app_context.clone()) {
                        Ok(service) => service,
                        Err(e) => {
                            tracing::error!("Failed to create gRPC service: {}", e);
                            // 返回一个错误的服务，让调用者处理
                            panic!("Failed to create gRPC service: {}", e);
                        }
                    }
                };

                // 使用 ServerManager 启动服务器
                let tls_for_manager = tls_service_clone.as_ref().map(|arc| (**arc).clone());

                if let Err(e) = GrpcServerManager::run_loop(
                    bind_addr,
                    make_service,
                    tls_for_manager,
                    shutdown_token.clone(),
                )
                .await
                {
                    error!("Server manager error: {}", e);
                } else {
                    info!("gRPC server manager completed successfully");
                }
            })
        };

        lifecycle_manager.register_high_priority_service(
            "grpc_server_manager".to_string(),
            server_manager_handle,
        );
        lifecycle_manager.register_medium_priority_service(
            "heartbeat_monitor".to_string(),
            heartbeat_monitor_handle,
        );
        lifecycle_manager.register_medium_priority_service(
            "agent_info_monitor".to_string(),
            agent_info_monitor_handle,
        );
        lifecycle_manager
            .register_medium_priority_service("task_monitor".to_string(), task_monitor_handle);

        if let Some(cleanup_handle) = agent_service.spawn_cache_cleanup() {
            lifecycle_manager.register_low_priority_service(
                "selector_cache_cleanup".to_string(),
                cleanup_handle,
            );
        }

        // 11. 运行生命周期管理器
        info!("Starting lifecycle manager...");
        info!("Server startup completed, waiting for shutdown signal...");
        if let Err(e) = lifecycle_manager.run().await {
            error!("Lifecycle manager error: {}", e);
            return Err(oasis_core::error::CoreError::Internal {
                message: format!("Lifecycle manager error: {}", e),
                severity: oasis_core::error::ErrorSeverity::Error,
            });
        }

        info!("OASIS Server shut down gracefully");
        Ok(())
    }

    fn create_grpc_service(
        app_context: Arc<crate::application::context::ApplicationContext>,
    ) -> Result<
        oasis_core::proto::oasis_service_server::OasisServiceServer<
            crate::interface::grpc::server::OasisServer,
        >,
    > {
        info!("Creating OasisServer instance...");
        let server = crate::interface::grpc::server::OasisServer::new(app_context);
        info!("OasisServer instance created successfully");

        info!("Wrapping with gRPC service...");
        let service = oasis_core::proto::oasis_service_server::OasisServiceServer::new(server);
        info!("gRPC service wrapped successfully");

        Ok(service)
    }
}
