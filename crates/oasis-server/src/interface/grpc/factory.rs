use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::application::context::ApplicationContextBuilder;
use crate::application::services::RolloutManager;

/// gRPC 服务工厂
pub struct GrpcServiceFactory;

impl GrpcServiceFactory {
    /// 创建 OasisServer 实例
    pub async fn create_oasis_server(
        jetstream: async_nats::jetstream::Context,
        shutdown_token: CancellationToken,
        heartbeat_ttl_sec: u64,
        streaming_backoff: crate::config::StreamingBackoffSection,
        health_service: Option<std::sync::Arc<crate::interface::health::HealthService>>,
    ) -> Result<crate::interface::grpc::server::OasisServer> {
        info!(
            "Creating OasisServer with heartbeat TTL: {}s",
            heartbeat_ttl_sec
        );

        // 在 Interface 层装配基础设施实现
        let node_repo = Arc::new(crate::infrastructure::persistence::NatsNodeRepository::new(
            jetstream.clone(),
            heartbeat_ttl_sec,
        ))
            as Arc<dyn crate::application::ports::repositories::NodeRepository>;
        let task_repo = Arc::new(crate::infrastructure::persistence::NatsTaskRepository::new(
            jetstream.clone(),
        ))
            as Arc<dyn crate::application::ports::repositories::TaskRepository>;
        let rollout_repo = Arc::new(
            crate::infrastructure::persistence::NatsRolloutRepository::new(jetstream.clone()),
        )
            as Arc<dyn crate::application::ports::repositories::RolloutRepository>;
        let file_repo = Arc::new(crate::infrastructure::persistence::NatsFileRepository::new(
            jetstream.clone(),
        ))
            as Arc<dyn crate::application::ports::repositories::FileRepository>;

        let agent_config_repo = Arc::new(
            crate::infrastructure::persistence::agent_config_repository::NatsAgentConfigRepository::new(
                jetstream.clone(),
            ),
        ) as Arc<dyn crate::application::ports::repositories::AgentConfigRepository>;

        let selector_engine =
            Arc::new(crate::domain::services::selector_engine::CelSelectorEngine::new())
                as Arc<dyn crate::domain::services::SelectorEngine>;

        // 使用 ApplicationContextBuilder 注入抽象依赖
        let context = ApplicationContextBuilder::new()
            .with_node_repo(node_repo)
            .with_task_repo(task_repo)
            .with_rollout_repo(rollout_repo)
            .with_file_repo(file_repo)
            .with_agent_config_repo(agent_config_repo)
            .with_selector_engine(selector_engine)
            .build()?;

        let server = crate::interface::grpc::server::OasisServer::new(
            context.clone(),
            shutdown_token.clone(),
            heartbeat_ttl_sec,
            streaming_backoff,
            health_service,
        );

        // 创建 RolloutManager
        let rollout_manager = RolloutManager::new(
            context.rollout_repo,
            context.node_repo,
            context.task_repo,
            context.selector_engine,
            shutdown_token.child_token(),
        );

        // 启动 RolloutManager
        let rollout_manager_arc = Arc::new(rollout_manager);
        let rollout_manager_clone = rollout_manager_arc.clone();
        tokio::spawn(async move {
            rollout_manager_clone.run().await;
        });

        info!("OasisServer created successfully");
        Ok(server)
    }

    /// 创建 gRPC 服务包装器
    pub async fn create_service_wrapper(
        jetstream: async_nats::jetstream::Context,
        shutdown_token: CancellationToken,
        heartbeat_ttl_sec: u64,
        streaming_backoff: crate::config::StreamingBackoffSection,
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
