use crate::application::context::ApplicationContext;
use crate::infrastructure::monitor::agent_info_monitor::AgentInfoMonitor;
use crate::infrastructure::monitor::heartbeat_monitor::HeartbeatMonitor;
use crate::infrastructure::monitor::task_monitor::TaskMonitor;
use crate::infrastructure::services::{
    AgentService, CaService, FileService, RolloutService, TaskService,
};
use crate::{
    infrastructure::lifecycle::LifecycleManager, interface::server_manager::GrpcServerManager,
};
use futures_util::StreamExt;
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

        // 3. 启动服务器
        Self::start_with_config(config).await
    }

    async fn start_with_config(config: OasisConfig) -> Result<()> {
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
        let mut lifecycle_manager = LifecycleManager::new().with_shutdown_timeout(10);

        info!("Building ApplicationContext with simplified architecture");

        // 3. 初始化NATS连接
        debug!("Connecting to NATS...");
        let nats_client =
            NatsClientFactory::create_nats_client_with_jetstream(&config.nats, &config.tls).await?;
        let nats_raw = nats_client.client.clone();
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

        // CA CSR 服务（通过 NATS request/reply 提供证书签发）
        let ca_service = Arc::new(
            CaService::new(
                config.ca.ca_cert_path.clone(),
                config.ca.ca_key_path.clone(),
                (config.ca.cert_validity_days as u64).saturating_mul(24),
            )
            .await?,
        );

        // CA CSR responder
        let ca_responder_handle = {
            let ca_service = ca_service.clone();
            let client = nats_raw.clone();
            let shutdown_token = lifecycle_manager.shutdown_token();
            tokio::spawn(async move {
                let mut sub = match client.subscribe("oasis.ca.csr".to_string()).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to subscribe CA CSR subject: {}", e);
                        return;
                    }
                };

                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            info!("CA CSR responder shutting down");
                            break;
                        }
                        msg = sub.next() => {
                            let Some(msg) = msg else { break; };

                            let Some(reply) = msg.reply.clone() else {
                                continue;
                            };

                            let request: std::result::Result<oasis_core::csr_types::CsrRequest, _> =
                                serde_json::from_slice(&msg.payload);

                            let response = match request {
                                Ok(req) => ca_service.handle_csr_request(req).await,
                                Err(e) => oasis_core::csr_types::CsrResponse::error(format!(
                                    "Invalid CSR request: {e}"
                                )),
                            };

                            let payload = match serde_json::to_vec(&response) {
                                Ok(p) => p,
                                Err(e) => {
                                    let fallback = oasis_core::csr_types::CsrResponse::error(format!(
                                        "Failed to serialize response: {e}"
                                    ));
                                    serde_json::to_vec(&fallback).unwrap_or_default()
                                }
                            };

                            if let Err(e) = client.publish(reply, payload.into()).await {
                                error!("Failed to publish CSR response: {}", e);
                            }
                        }
                    }
                }
            })
        };

        lifecycle_manager
            .register_low_priority_service("ca_csr_responder".to_string(), ca_responder_handle);

        // CA token cleanup
        let ca_cleanup_handle = {
            let ca_service = ca_service.clone();
            let shutdown_token = lifecycle_manager.shutdown_token();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => break,
                        _ = interval.tick() => ca_service.cleanup_expired_tokens().await,
                    }
                }
            })
        };
        lifecycle_manager
            .register_low_priority_service("ca_token_cleanup".to_string(), ca_cleanup_handle);

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
            ca_service: ca_service.clone(),
        });

        info!("Application context built successfully");

        // 8. 解析地址
        let bind_addr: SocketAddr = config.server.listen_addr.parse().map_err(|e| {
            oasis_core::error::CoreError::Config {
                message: format!("Invalid bind address: {}", e),
                severity: oasis_core::error::ErrorSeverity::Error,
            }
        })?;

        // 9. 启用进程内 TLS（原生 TLS）
        let tls_service: Option<std::sync::Arc<crate::infrastructure::tls::TlsService>> =
            match crate::infrastructure::tls::TlsService::new_with_paths(
                config.tls.certs_dir.clone(),
            )
            .await
            {
                Ok(svc) => Some(std::sync::Arc::new(svc)),
                Err(e) => {
                    if config.tls.require_tls {
                        return Err(oasis_core::error::CoreError::Config {
                            message: format!(
                                "TLS initialization failed and require_tls=true: {}. Set tls.require_tls=false to allow plaintext connections.",
                                e
                            ),
                            severity: oasis_core::error::ErrorSeverity::Critical,
                        });
                    }
                    tracing::warn!(
                        "Failed to initialize TLS service: {}, falling back to plaintext (require_tls=false)",
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

                let make_service = || {
                    info!("Creating new gRPC service instance");
                    Self::create_grpc_service(app_context.clone())
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

        // 12. 运行生命周期管理器
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
