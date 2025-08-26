use anyhow::Result;
use oasis_agent::{
    application::services::{
        fact_service::FactService, heartbeat::HeartbeatService, task_processor::TaskProcessor,
    },
    domain::{Agent, AgentStatus},
    infrastructure::{
        nats::client::NatsClient,
        system::{executor::CommandExecutor, fact_collector::SystemMonitor},
    },
};
use oasis_core::{
    rate_limit::RateLimiterCollection,
    shutdown::GracefulShutdown,
    telemetry::init_tracing_with,
    types::AgentId,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let nats_url =
        std::env::var("OASIS_NATS_URL").unwrap_or_else(|_| "tls://127.0.0.1:4443".to_string());
    let nats_ca =
        std::env::var("OASIS_NATS_CA").unwrap_or_else(|_| "certs/nats-ca.pem".to_string());
    let nats_client_cert = std::env::var("OASIS_NATS_CLIENT_CERT")
        .unwrap_or_else(|_| "certs/nats-client.pem".to_string());
    let nats_client_key = std::env::var("OASIS_NATS_CLIENT_KEY")
        .unwrap_or_else(|_| "certs/nats-client-key.pem".to_string());

    let server_url =
        std::env::var("OASIS_SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

    let _grpc_ca =
        std::env::var("OASIS_GRPC_CA").unwrap_or_else(|_| "certs/grpc-ca.pem".to_string());
    let _grpc_client_cert = std::env::var("OASIS_GRPC_CLIENT_CERT")
        .unwrap_or_else(|_| "certs/grpc-client.pem".to_string());
    let _grpc_client_key = std::env::var("OASIS_GRPC_CLIENT_KEY")
        .unwrap_or_else(|_| "certs/grpc-client-key.pem".to_string());

    let log_level = std::env::var("OASIS_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    let _heartbeat_interval_sec = std::env::var("OASIS_HEARTBEAT_INTERVAL_SEC")
        .unwrap_or_else(|_| "30".to_string())
        .parse()
        .unwrap_or(30);
    let _fact_collection_interval_sec = std::env::var("OASIS_FACT_COLLECTION_INTERVAL_SEC")
        .unwrap_or_else(|_| "300".to_string())
        .parse()
        .unwrap_or(300);
    let _max_concurrent_tasks = std::env::var("OASIS_MAX_CONCURRENT_TASKS")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);

    // 初始化遥测
    init_tracing_with(&oasis_core::telemetry::LogConfig {
        level: log_level.clone(),
        format: "text".to_string(),
        no_ansi: false,
    });

    info!("Starting Oasis Agent...");
    info!("  NATS URL: {}", nats_url);
    info!("  Server URL: {}", server_url);
    info!("  Log level: {}", log_level);

    // 连接到 NATS
    let nats_client =
        NatsClient::connect_with_config(&nats_url, &nats_ca, &nats_client_cert, &nats_client_key)
            .await?;

    // 生成唯一的 Agent ID
    let agent_id = AgentId::from(Uuid::new_v4().to_string());
    info!("Generated Agent ID: {}", agent_id);

    // 创建 Agent 实例
    let agent = Arc::new(RwLock::new(Agent::new(
        agent_id.clone(),
        oasis_core::selector::NodeAttributes::new(agent_id.clone()),
    )));

    // 创建命令执行器（使用硬编码配置）
    let command_executor = Arc::new(CommandExecutor::new());

    let system_monitor = Arc::new(SystemMonitor::new());
    let fact_repository = Arc::new(
        oasis_agent::infrastructure::nats::fact_repository::NatsFactRepository::new(
            nats_client.client.clone(),
            oasis_core::JS_KV_NODE_FACTS.to_string(),
            oasis_core::kv_key_facts(agent_id.as_str()),
        ),
    );

    let limiters = Arc::new(RateLimiterCollection::default());
    let shutdown = GracefulShutdown::new();

    let heartbeat_service = HeartbeatService::new(
        agent.clone(),
        nats_client.clone(),
        limiters.clone(),
        shutdown.clone(),
    );

    let task_processor = TaskProcessor::new(
        agent.clone(),
        nats_client.clone(),
        command_executor.clone(),
        limiters.clone(),
        shutdown.clone(),
    );

    // 事实采集服务
    let mut fact_service = FactService::new(
        agent_id.clone(),
        system_monitor.clone(),
        fact_repository.clone(),
    );

    info!("Agent started with id: {}", agent_id);

    {
        let mut agent_guard = agent.write().await;
        agent_guard.set_status(AgentStatus::Running);
    }
    info!("Agent status set to Running");

    // 启动各服务
    let heartbeat_handle = tokio::spawn(heartbeat_service.run());
    let facts_handle = tokio::spawn(async move {
        if let Err(e) = fact_service.start().await {
            error!("Fact service failed: {}", e);
        }
    });
    let task_handle = tokio::spawn(task_processor.run());

    info!("All services started successfully");

    // 监听 Ctrl+C，并触发 GracefulShutdown（取消令牌）
    let token = shutdown.token.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        token.cancel();
    });

    // 等待全局关闭信号
    shutdown.wait_for_signal().await;

    info!("Shutdown signal received, stopping services...");

    // 等待任务结束（不使用 abort，交由各自内部优雅停机）
    let _ = futures::future::join3(heartbeat_handle, task_handle, facts_handle).await;

    info!("Agent shut down gracefully");
    Ok(())
}
