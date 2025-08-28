use crate::infrastructure::connection::create_jetstream_context_with_config;
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

pub mod lifecycle;

use crate::bootstrap::lifecycle::ServiceLifecycleManager;
use crate::infrastructure::di_container::InfrastructureDiContainer;
use crate::infrastructure::resource_manager::InfrastructureResourceManager;
use crate::infrastructure::services::leader_election::LeaderElectionService;
use crate::infrastructure::tls::TlsService;
use crate::interface::grpc::factory::GrpcServiceFactory;
use crate::interface::server_manager::GrpcServerManager;
use oasis_core::config::OasisConfig;
use oasis_core::shutdown::GracefulShutdown;

/// 服务器启动器 - 负责整个应用程序的引导过程
pub struct ServerBootstrapper {
    config: OasisConfig,
    shutdown: GracefulShutdown,
}

/// 运行中的服务器实例
pub struct RunningServer {
    lifecycle_manager: ServiceLifecycleManager,
}

impl ServerBootstrapper {
    /// 创建新的服务器启动器
    pub fn new(config: OasisConfig) -> Self {
        let shutdown = GracefulShutdown::new();

        Self { config, shutdown }
    }

    /// 启动服务器，返回运行中的实例
    pub async fn start(self) -> Result<RunningServer> {
        info!(
            listen_addr = %self.config.listen_addr,
            data_dir = %self.config.data_dir.display(),
            "Oasis Server starting"
        );

        // 创建服务生命周期管理器
        let mut lifecycle_manager =
            ServiceLifecycleManager::new(self.shutdown.child_token()).with_cleanup_timeout(30);

        // 监听终止信号
        self.setup_signal_handler();

        // 连接 NATS
        let jetstream = create_jetstream_context_with_config(&self.config.nats).await?;

        // 初始化基础设施资源
        self.initialize_infra_resources(&jetstream).await?;

        // 初始化 TLS 服务
        let tls_service = self.initialize_tls_service().await?;
        // 克隆一份注册到生命周期管理器，保留原始以供 gRPC 使用
        lifecycle_manager.register_tls_service(tls_service.clone());

        // 初始化选主服务
        let leader_election_service = self.initialize_leader_election_service(&jetstream).await?;
        lifecycle_manager.register_leader_election(leader_election_service);

        // 创建 DI 容器和应用程序上下文
        let di_container = InfrastructureDiContainer::new(jetstream.clone());
        let context = di_container.create_application_context()?;

        // 初始化健康服务
        let health_service = self.initialize_health_service(&jetstream, &context).await?;
        lifecycle_manager.register_health_service(health_service.clone());

        // 启动 KV Watchers
        let (kv_watcher_handles, event_processor_handle, index_updater_handle) = self
            .start_kv_watchers(&jetstream, &health_service, context.selector_engine.clone())
            .await?;
        lifecycle_manager.register_tasks("kv_watchers".to_string(), kv_watcher_handles, 3);
        lifecycle_manager.register_task("event_processor".to_string(), event_processor_handle, 3);
        lifecycle_manager.register_task("index_updater".to_string(), index_updater_handle, 3);

        // 启动 RolloutManager（统一纳入生命周期管理）
        {
            use crate::application::services::RolloutManager;

            let rollout_manager = RolloutManager::new(
                context.rollout_repo.clone(),
                context.task_repo.clone(),
                context.agent_repo.clone(),
                context.selector_engine.clone(),
                self.shutdown.child_token(),
            );
            let rollout_arc = Arc::new(rollout_manager);
            let rollout_clone = rollout_arc.clone();
            let handle = tokio::spawn(async move {
                rollout_clone.run().await;
            });
            lifecycle_manager.register_task("rollout_manager".to_string(), handle, 2);
        }

        // 启动 gRPC 服务器
        let grpc_handle = self
            .start_grpc_server(&jetstream, &health_service, &tls_service, &context)
            .await?;
        lifecycle_manager.register_task("grpc_server".to_string(), grpc_handle, 1);

        info!(
            "Oasis Server started successfully with {} services",
            lifecycle_manager.service_count()
        );
        info!(
            "Registered services: {:?}",
            lifecycle_manager.list_service_names()
        );

        Ok(RunningServer { lifecycle_manager })
    }

    /// 设置信号处理器
    fn setup_signal_handler(&self) {
        let shutdown_signal = self.shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal.wait_for_signal().await;
        });
    }

    /// 初始化基础设施资源
    async fn initialize_infra_resources(
        &self,
        jetstream: &async_nats::jetstream::Context,
    ) -> Result<()> {
        InfrastructureResourceManager::initialize(jetstream, self.shutdown.child_token()).await?;
        Ok(())
    }

    /// 初始化 TLS 服务
    async fn initialize_tls_service(&self) -> Result<TlsService> {
        info!("Initializing TLS service...");
        let (tls_reload_sender, _) = broadcast::channel(1);

        // 从配置加载 TLS 路径
        let tls_config = crate::infrastructure::tls::GrpcTlsConfig {
            server_cert: self
                .config
                .tls
                .grpc_server_cert_path
                .to_string_lossy()
                .to_string(),
            server_key: self
                .config
                .tls
                .grpc_server_key_path
                .to_string_lossy()
                .to_string(),
            ca_cert: self.config.tls.grpc_ca_path.to_string_lossy().to_string(),
        };

        let tls_service = TlsService::new(tls_config, tls_reload_sender);

        tls_service
            .load_certificates()
            .await
            .context("Failed to load TLS certificates")?;

        tls_service
            .start_certificate_monitoring()
            .await
            .context("Failed to start certificate monitoring")?;

        info!("TLS service initialized successfully");
        Ok(tls_service)
    }

    /// 初始化选主服务
    async fn initialize_leader_election_service(
        &self,
        jetstream: &async_nats::jetstream::Context,
    ) -> Result<LeaderElectionService> {
        info!("Initializing leader election service...");

        let leader_election_config =
            crate::infrastructure::services::leader_election::LeaderElectionConfig {
                election_key: "oasis.leader".to_string(),
                ttl_sec: 30,
                check_interval_sec: 5,
            };

        let leader_election_service = LeaderElectionService::new(
            jetstream.clone(),
            &leader_election_config,
            self.shutdown.child_token(),
        )
        .await
        .context("Failed to create leader election service")?;

        leader_election_service
            .start_election()
            .await
            .context("Failed to start leader election")?;

        info!("Leader election service initialized successfully");
        Ok(leader_election_service)
    }

    /// 初始化健康服务
    async fn initialize_health_service(
        &self,
        jetstream: &async_nats::jetstream::Context,
        context: &crate::application::context::ApplicationContext,
    ) -> Result<Arc<crate::interface::health::HealthService>> {
        info!("Initializing health service...");

        // 创建健康检查服务
        let health_check_service = Arc::new(
            crate::application::services::health_check_service::AgentHealthCheckService::new(
                context.agent_repo.clone(),
            ),
        );

        // 创建 HealthService
        let health_service = Arc::new(crate::interface::health::HealthService::new(
            health_check_service,
        ));

        // 启动健康检查监控
        health_service.start_monitoring(jetstream.clone(), 60).await;

        info!("Health service initialized successfully");
        Ok(health_service)
    }

    /// 启动 KV Watchers
    async fn start_kv_watchers(
        &self,
        jetstream: &async_nats::jetstream::Context,
        health_service: &Arc<crate::interface::health::HealthService>,
        selector_engine: Arc<crate::application::selector::SelectorEngine>,
    ) -> Result<(
        Vec<tokio::task::JoinHandle<()>>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    )> {
        let (event_sender, _) = broadcast::channel(1000);

        let kv_watcher = crate::infrastructure::services::kv_watcher::KvWatcherService::new(
            jetstream.clone(),
            event_sender.clone(),
            self.shutdown.child_token(),
        );

        // 先启动订阅者（事件处理器与索引更新器）再启动 Watchers，避免错过 watch_all 的回放事件
        let event_processor_handle = self
            .start_event_processor(event_sender.clone(), health_service.clone())
            .await?;

        let index_updater_handle = self
            .start_index_updater(event_sender.clone(), selector_engine)
            .await?;

        // 最后启动 KV Watchers（此时订阅者已就绪，可以接收快照与后续事件）
        let handles = kv_watcher.start_watching().await?;

        info!("KV watchers and index updater started successfully");
        Ok((handles, event_processor_handle, index_updater_handle))
    }

    /// 启动事件处理器
    async fn start_event_processor(
        &self,
        event_sender: broadcast::Sender<crate::domain::events::AgentEvent>,
        health_service: Arc<crate::interface::health::HealthService>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let mut event_receiver = event_sender.subscribe();
        let shutdown_token = self.shutdown.child_token();

        let handle = tokio::spawn(async move {
            info!("Event processor started");
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        info!("Shutdown signal received, stopping event processor.");
                        break;
                    }
                    Ok(event) = event_receiver.recv() => {
                        match event {
                            crate::domain::events::AgentEvent::Online { agent_id, .. } => {
                                health_service.update_agent_status(agent_id.as_str(), true).await;
                            }
                            crate::domain::events::AgentEvent::Offline { agent_id, .. } => {
                                health_service.update_agent_status(agent_id.as_str(), false).await;
                            }
                            crate::domain::events::AgentEvent::LabelsUpdated { agent_id, labels, timestamp } => {
                                info!(agent_id = %agent_id, labels = ?labels, timestamp = %timestamp, "Agent labels updated");
                            }
                            crate::domain::events::AgentEvent::GroupsUpdated { agent_id, groups, timestamp } => {
                                info!(agent_id = %agent_id, groups = ?groups, timestamp = %timestamp, "Agent groups updated");
                            }
                            crate::domain::events::AgentEvent::FactsUpdated { agent_id, facts, timestamp } => {
                                tracing::debug!(agent_id = %agent_id, facts = ?facts, timestamp = %timestamp, "Agent facts updated");
                            }
                            crate::domain::events::AgentEvent::HeartbeatUpdated { agent_id, timestamp } => {
                                tracing::debug!(agent_id = %agent_id, timestamp = %timestamp, "Agent heartbeat updated");
                            }
                        }
                    }
                    else => break,
                }
            }
            info!("Event processor stopped");
        });

        Ok(handle)
    }

    /// 启动索引更新服务
    async fn start_index_updater(
        &self,
        event_sender: broadcast::Sender<crate::domain::events::AgentEvent>,
        selector_engine: Arc<crate::application::selector::SelectorEngine>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let event_receiver = event_sender.subscribe();
        let shutdown_token = self.shutdown.child_token();

        // 创建索引更新服务
        let mut index_updater =
            crate::infrastructure::services::index_updater::IndexUpdaterService::new(
                selector_engine,
                event_receiver,
                shutdown_token,
            );

        let handle = tokio::spawn(async move {
            if let Err(e) = index_updater.run().await {
                tracing::error!("Index updater error: {}", e);
            }
        });

        Ok(handle)
    }

    /// 启动 gRPC 服务器
    async fn start_grpc_server(
        &self,
        jetstream: &async_nats::jetstream::Context,
        health_service: &Arc<crate::interface::health::HealthService>,
        tls_service: &TlsService,
        app_context: &crate::application::context::ApplicationContext,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let tls_service_clone = tls_service.clone();

        let addr = self.config.listen_addr.parse()?;
        let shutdown_token = self.shutdown.child_token(); // 创建子token
        let shutdown_clone = shutdown_token.clone(); // clone用于移动到闭包
        let health_clone = health_service.clone();
        let app_ctx_clone = app_context.clone();

        let streaming_config = crate::interface::grpc::server::StreamingBackoffSection {
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            max_retries: 10,
        };

        let heartbeat_ttl = self.config.server.heartbeat_ttl_sec as u64;

        let grpc_handle = tokio::spawn(async move {
            if let Err(e) = GrpcServerManager::run_loop(
                addr,
                || {
                    let svc =
                        futures::executor::block_on(GrpcServiceFactory::create_service_wrapper(
                            std::sync::Arc::new(app_ctx_clone.clone()),
                            shutdown_clone.child_token(), // 使用clone的token创建子token
                            heartbeat_ttl,
                            streaming_config.clone(),
                            Some(health_clone.clone()),
                        ))
                        .expect("create svc");
                    (svc, Some(health_clone.clone()))
                },
                Some(tls_service_clone),
                shutdown_token, // 移动原始token
            )
            .await
            {
                tracing::error!("gRPC server error: {}", e);
            }
        });

        Ok(grpc_handle)
    }
}

impl RunningServer {
    /// 等待服务器关闭
    pub async fn wait_for_shutdown(self) -> Result<()> {
        // 等待关闭信号
        self.lifecycle_manager.wait_for_shutdown_signal().await;

        // 执行优雅停机
        self.lifecycle_manager.graceful_shutdown().await?;

        info!("Oasis Server shut down gracefully.");
        Ok(())
    }
}
