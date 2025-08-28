use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::interface::grpc::server::StreamingBackoffSection;

/// gRPC 服务工厂 - 只负责组装服务，不创建具体实现
pub struct GrpcServiceFactory;

impl GrpcServiceFactory {
    /// 创建 OasisServer 实例
    pub async fn create_oasis_server(
        app_context: std::sync::Arc<crate::application::context::ApplicationContext>,
        shutdown_token: CancellationToken,
        heartbeat_ttl_sec: u64,
        streaming_backoff: StreamingBackoffSection,
        health_service: Option<std::sync::Arc<crate::interface::health::HealthService>>,
    ) -> Result<crate::interface::grpc::server::OasisServer> {
        info!(
            "Creating OasisServer with heartbeat TTL: {}s",
            heartbeat_ttl_sec
        );

        let server = crate::interface::grpc::server::OasisServer::new(
            (*app_context).clone(),
            shutdown_token.clone(),
            heartbeat_ttl_sec,
            streaming_backoff,
            health_service,
        );

        // 注意：RolloutManager 的启动改由 ServerBootstrapper 统一管理
        let _ = (
            app_context.rollout_repo.clone(),
            app_context.task_repo.clone(),
            app_context.selector_engine.clone(),
        );

        info!("OasisServer created successfully");
        Ok(server)
    }

    /// 创建 gRPC 服务包装器
    pub async fn create_service_wrapper(
        app_context: std::sync::Arc<crate::application::context::ApplicationContext>,
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
            app_context,
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
