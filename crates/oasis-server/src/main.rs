use anyhow::{Context, Result};
use clap::Parser;
use oasis_core::config::{ComponentConfig, ConfigLoadOptions, ConfigSource};
use oasis_core::shutdown::GracefulShutdown;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

mod application;
mod config;
mod domain;
mod infrastructure;
mod interface;

use crate::application::services::rollout_manager::RolloutManager;
use crate::infrastructure::connection::NatsConnectionManager;
use crate::infrastructure::resource_manager::InfrastructureResourceManager;
use crate::infrastructure::services::leader_election::LeaderElectionService;
use crate::infrastructure::tls::TlsService;
use crate::interface::grpc::factory::GrpcServiceFactory;
use crate::interface::server_manager::GrpcServerManager;
use std::sync::Arc;

/// Oasis Server 应用程序
pub struct OasisServerApp {
    config: config::ServerConfig,
    shutdown: GracefulShutdown,
    kv_watcher_handles: Option<Vec<tokio::task::JoinHandle<()>>>,
    event_sender: broadcast::Sender<crate::domain::events::NodeEvent>,
    leader_election_service: Option<LeaderElectionService>,
    rollout_manager: Option<Arc<RolloutManager>>,
    health_service: Option<Arc<crate::interface::health::HealthService>>,

    tls_service: Option<TlsService>,
    tls_reload_sender: broadcast::Sender<()>, // 用于 TLS 热重载
}

impl OasisServerApp {
    /// 创建新的 Oasis Server 应用程序
    pub fn new(config: config::ServerConfig) -> Self {
        let shutdown = GracefulShutdown::new();
        let (event_sender, _) = broadcast::channel(1000);
        let (tls_reload_sender, _) = broadcast::channel(1);

        Self {
            config,
            shutdown,
            kv_watcher_handles: None,
            event_sender,
            leader_election_service: None,
            rollout_manager: None,
            health_service: None,
            tls_service: None,
            tls_reload_sender,
        }
    }

    /// 运行服务器
    pub async fn run(&mut self) -> Result<()> {
        info!(
            grpc_addr = %self.config.server.grpc_addr,
            nats_url = %self.config.common.nats.url,
            heartbeat_ttl_sec = self.config.server.heartbeat_ttl_sec,
            "Oasis Server starting"
        );

        // 监听终止信号
        self.setup_signal_handler();

        // 连接 NATS
        let jetstream = self.connect_to_nats().await?;

        // 初始化基础设施资源
        self.initialize_infra_resources(jetstream.clone()).await?;

        // 初始化 TLS 服务
        self.initialize_tls_service().await?;

        // 初始化选主服务
        self.initialize_leader_election_service(jetstream.clone())
            .await?;

        // 初始化共享的 HealthService
        self.initialize_health_service(jetstream.clone()).await?;

        // 启动 KV Watchers
        self.start_kv_watchers(jetstream.clone()).await?;

        // 初始化并启动 Rollout Manager
        self.initialize_and_start_rollout_manager(jetstream.clone())
            .await?;

        let tls_reload_rx = self.tls_reload_sender.subscribe();
        // 交由 GrpcServerManager 统一管理服务器重启循环
        let addr = self.config.server.grpc_addr.parse()?;
        let js_clone = jetstream.clone();
        let shutdown_token = self.shutdown.child_token();
        GrpcServerManager::run_loop(
            addr,
            || {
                futures::executor::block_on(self.create_grpc_service(js_clone.clone()))
                    .expect("create svc")
            },
            tls_reload_rx,
            shutdown_token,
        )
        .await?;

        // 等待所有后台任务完成
        self.cleanup_background_tasks().await?;

        info!("Oasis Server shut down gracefully.");
        Ok(())
    }

    /// 启动信号处理器
    fn setup_signal_handler(&self) {
        let shutdown_signal = self.shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal.wait_for_signal().await;
        });
    }

    /// 连接 NATS
    async fn connect_to_nats(&self) -> Result<async_nats::jetstream::Context> {
        // 连接NATS
        let jetstream = if self.config.common.nats.tls_required {
            info!("Initializing NATS connection with TLS...");
            NatsConnectionManager::create_jetstream_with_tls(
                &self.config.common.nats.url,
                &self.config.common.nats,
            )
            .await?
        } else {
            info!("Initializing NATS connection without TLS...");
            NatsConnectionManager::create_jetstream(&self.config.common.nats.url).await?
        };

        Ok(jetstream)
    }

    /// 初始化基础设施资源
    async fn initialize_infra_resources(
        &mut self,
        jetstream: async_nats::jetstream::Context,
    ) -> Result<()> {
        // 初始化基础设施资源
        InfrastructureResourceManager::initialize(
            &jetstream,
            &self.config,
            self.shutdown.child_token(),
        )
        .await?;

        Ok(())
    }

    /// 初始化 TLS 服务
    async fn initialize_tls_service(&mut self) -> Result<()> {
        if let Some(tls_config) = &self.config.server.grpc_tls {
            if tls_config.enabled {
                info!("Initializing TLS service...");

                let tls_service = TlsService::new(
                    self.config.server.grpc_tls.clone().unwrap(),
                    self.tls_reload_sender.clone(),
                );
                tls_service
                    .load_certificates()
                    .await
                    .context("Failed to load TLS certificates")?;

                // 启动证书监控（热重载）
                tls_service
                    .start_certificate_monitoring()
                    .await
                    .context("Failed to start certificate monitoring")?;

                self.tls_service = Some(tls_service);
                info!("TLS service initialized successfully");
            } else {
                info!("TLS is disabled");
            }
        } else {
            info!("No TLS configuration provided");
        }
        Ok(())
    }

    /// 初始化选主服务
    async fn initialize_leader_election_service(
        &mut self,
        jetstream: async_nats::jetstream::Context,
    ) -> Result<()> {
        info!("Initializing leader election service...");

        let leader_election_service =
            LeaderElectionService::new(jetstream, &self.config, self.shutdown.child_token())
                .await
                .context("Failed to create leader election service")?;

        // 启动选主
        leader_election_service
            .start_election()
            .await
            .context("Failed to start leader election")?;

        self.leader_election_service = Some(leader_election_service);
        info!("Leader election service initialized successfully");
        Ok(())
    }

    /// 初始化共享的 HealthService
    async fn initialize_health_service(
        &mut self,
        jetstream: async_nats::jetstream::Context,
    ) -> Result<()> {
        info!("Initializing health service...");

        // 创建 NodeRepository
        let node_repo = Arc::new(crate::infrastructure::persistence::NatsNodeRepository::new(
            jetstream.clone(),
            self.config.server.heartbeat_ttl_sec,
        ))
            as Arc<dyn crate::application::ports::repositories::NodeRepository>;

        // 创建 HealthService 并传入 NodeRepository
        let health_service = Arc::new(crate::interface::health::HealthService::new(
            node_repo.clone(),
        ));

        // 启动健康检查监控
        let health_service_clone = health_service.clone();
        health_service_clone
            .start_monitoring(jetstream, node_repo, self.config.server.heartbeat_ttl_sec)
            .await;

        self.health_service = Some(health_service);
        info!("Health service initialized successfully");
        Ok(())
    }

    /// 启动 KV Watchers
    async fn start_kv_watchers(&mut self, jetstream: async_nats::jetstream::Context) -> Result<()> {
        let kv_watcher = crate::infrastructure::services::KvWatcherService::new(
            jetstream,
            self.event_sender.clone(),
            self.shutdown.child_token(),
            self.config.clone(),
        );

        let handles = kv_watcher.start_watching().await?;
        self.kv_watcher_handles = Some(handles);

        // 启动事件处理循环
        self.start_event_processor().await?;

        info!("KV watchers started successfully");
        Ok(())
    }

    /// 启动事件处理器
    async fn start_event_processor(&self) -> Result<()> {
        let mut event_receiver = self.event_sender.subscribe();
        let shutdown_token = self.shutdown.child_token();
        let health_service = self.health_service.as_ref().unwrap().clone();

        tokio::spawn(async move {
            info!("Event processor started");
            // 使用 loop 和 select! 替代 while let，以同时监听关闭信号和新事件
            loop {
                tokio::select! {
                    // 分支 1: 监听关闭信号
                    _ = shutdown_token.cancelled() => {
                        info!("Shutdown signal received, stopping event processor.");
                        break;
                    }
                    // 分支 2: 接收新事件
                    Ok(event) = event_receiver.recv() => {
                        match event {
                            crate::domain::events::NodeEvent::Online { node_id, timestamp } => {
                                health_service.update_agent_status(&node_id, true).await;
                            }
                            crate::domain::events::NodeEvent::Offline { node_id, timestamp } => {
                                health_service.update_agent_status(&node_id, false).await;
                            }
                            crate::domain::events::NodeEvent::HeartbeatUpdated { node_id, timestamp } => {
                                health_service.update_agent_status(&node_id, true).await;
                            }
                            crate::domain::events::NodeEvent::LabelsUpdated {
                                node_id,
                                labels,
                                timestamp,
                            } => {
                                info!(node_id = %node_id, labels = ?labels, timestamp = %timestamp, "Node labels updated");
                            }
                            crate::domain::events::NodeEvent::FactsUpdated {
                                node_id,
                                facts,
                                timestamp,
                            } => {
                                debug!(node_id = %node_id, facts = %facts, timestamp = %timestamp, "Node facts updated");
                            }
                        }
                    }
                    // 如果通道关闭，也退出循环
                    else => {
                        break;
                    }
                }
            }
            info!("Event processor stopped");
        });

        Ok(())
    }

    /// 创建gRPC服务
    async fn create_grpc_service(
        &self,
        jetstream: async_nats::jetstream::Context,
    ) -> Result<
        oasis_core::proto::oasis_service_server::OasisServiceServer<
            interface::grpc::server::OasisServer,
        >,
    > {
        GrpcServiceFactory::create_service_wrapper(
            jetstream,
            self.shutdown.child_token(),
            self.config.server.heartbeat_ttl_sec,
            self.config.streaming.clone(),
            self.health_service.clone(),
        )
        .await
        .context("Failed to create gRPC service")
    }

    /// 启动gRPC服务器
    async fn start_grpc_server(
        &mut self,
        svc: oasis_core::proto::oasis_service_server::OasisServiceServer<
            crate::interface::grpc::server::OasisServer,
        >,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let tls_config = if let Some(tls_service) = &self.tls_service {
            tls_service.get_tls_config().await
        } else {
            None
        };

        let addr = self.config.server.grpc_addr.parse()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            shutdown_token.cancelled().await;
            let _ = shutdown_tx.send(());
        });

        let handle = tokio::spawn(async move {
            if let Some(tls_config) = tls_config {
                info!("Starting gRPC server with TLS...");
                GrpcServerManager::start_with_tls(addr, svc, tls_config, shutdown_rx).await
            } else {
                info!("Starting gRPC server without TLS...");
                GrpcServerManager::start(addr, svc, shutdown_rx).await
            }
        });
        Ok(handle)
    }

    /// 初始化并启动 Rollout Manager（已迁移至 GrpcServiceFactory 内部统一管理）
    async fn initialize_and_start_rollout_manager(
        &mut self,
        _jetstream: async_nats::jetstream::Context,
    ) -> Result<()> {
        info!(
            "RolloutManager lifecycle is managed by GrpcServiceFactory; skipping local initialization."
        );
        Ok(())
    }

    async fn cleanup_background_tasks(&mut self) -> Result<()> {
        // 等待 KV Watcher 任务完成
        if let Some(kv_handles) = &mut self.kv_watcher_handles {
            for handle in kv_handles {
                let _ = handle.await;
            }
        }

        // 释放领导权
        if let Some(svc) = &self.leader_election_service {
            if let Err(e) = svc.stop_leadership().await {
                warn!("Failed to relinquish leadership: {}", e);
            }
        }

        info!("Background tasks cleanup complete");
        Ok(())
    }
}

#[derive(Parser)]
#[command(name = "oasis-server")]
#[command(about = "Oasis Server - DDD-based cluster management server")]
#[command(version)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // 加载配置
    let cfg = match args.config.as_deref() {
        Some(config_path) => {
            let sources = vec![ConfigSource::File(config_path.into())];
            config::ServerConfig::load_from_sources(&sources, ConfigLoadOptions::default())
                .await
        }
        None => config::ServerConfig::load_smart().await,
    }
    .map_err(|e| anyhow::anyhow!("Configuration error: {}", e))?;

    // 初始化遥测
    oasis_core::telemetry::init_tracing_with(&oasis_core::telemetry::LogConfig {
        level: cfg.common.telemetry.log_level.clone(),
        format: cfg.common.telemetry.log_format.clone(),
        no_ansi: cfg.common.telemetry.log_no_ansi,
    });

    // 创建并运行应用程序
    let mut app = OasisServerApp::new(cfg);
    app.run().await
}
