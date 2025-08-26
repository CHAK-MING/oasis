use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::infrastructure::di_container::InfrastructureDiContainer;
use crate::interface::grpc::server::StreamingBackoffSection;

/// gRPC 服务工厂 - 只负责组装服务，不创建具体实现
pub struct GrpcServiceFactory;

impl GrpcServiceFactory {
    /// 创建 OasisServer 实例
    pub async fn create_oasis_server(
        jetstream: async_nats::jetstream::Context,
        shutdown_token: CancellationToken,
        heartbeat_ttl_sec: u64,
        streaming_backoff: StreamingBackoffSection,
        health_service: Option<std::sync::Arc<crate::interface::health::HealthService>>,
    ) -> Result<crate::interface::grpc::server::OasisServer> {
        info!(
            "Creating OasisServer with heartbeat TTL: {}s",
            heartbeat_ttl_sec
        );

        // 使用基础设施DI容器创建应用程序上下文
        let di_container = InfrastructureDiContainer::new(jetstream.clone(), heartbeat_ttl_sec);
        let context = di_container.create_application_context()?;

        let server = crate::interface::grpc::server::OasisServer::new(
            context.clone(),
            shutdown_token.clone(),
            heartbeat_ttl_sec,
            streaming_backoff,
            health_service,
        );

        // 注意：RolloutManager 的启动改由 ServerBootstrapper 统一管理
        let _ = (
            context.rollout_repo,
            context.node_repo,
            context.task_repo,
            context.selector_engine,
        );

        info!("OasisServer created successfully");
        Ok(server)
    }

    /// 创建 gRPC 服务包装器
    pub async fn create_service_wrapper(
        jetstream: async_nats::jetstream::Context,
        shutdown_token: CancellationToken,
        heartbeat_ttl_sec: u64,
        streaming_backoff: StreamingBackoffSection,
        health_service: Option<std::sync::Arc<crate::interface::health::HealthService>>,
    ) -> Result<
        oasis_core::proto::oasis_service_server::OasisServiceServer<
            crate::interface::grpc::server::OasisServer,
        >,
    > {
        let server = Self::create_oasis_server(
            jetstream,
            shutdown_token,
            heartbeat_ttl_sec,
            streaming_backoff,
            health_service,
        )
        .await?;

        let svc = oasis_core::proto::oasis_service_server::OasisServiceServer::new(server);
        Ok(svc)
    }
}
