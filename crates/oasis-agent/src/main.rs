use anyhow::Result;
use oasis_agent::{
    application::services::{
        fact_service::FactService, heartbeat::HeartbeatService, task_processor::TaskProcessor,
    },
    domain::{Agent, AgentStatus},
    infrastructure::{
        nats::client::NatsClient,
        nats::publisher::NatsPublisher,
        system::{executor::CommandExecutor, fact_collector::SystemMonitor},
    },
};
use oasis_core::{
    config::OasisConfig, rate_limit::RateLimiterCollection, shutdown::GracefulShutdown,
    telemetry::init_tracing_with, types::AgentId,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    // 统一配置加载（文件 + 环境变量覆盖）
    let cfg: OasisConfig = OasisConfig::load_config(None)?;

    let nats_url = cfg.nats.url.clone();
    let _nats_ca = cfg.nats.ca_path.to_string_lossy().to_string();
    let _nats_client_cert = cfg.nats.client_cert_path.to_string_lossy().to_string();
    let _nats_client_key = cfg.nats.client_key_path.to_string_lossy().to_string();

    let server_url = cfg.grpc.url.clone();

    let log_level = cfg.telemetry.log_level.clone();
    let _heartbeat_interval_sec = cfg.agent.heartbeat_interval_sec;
    let _fact_collection_interval_sec = cfg.agent.fact_collection_interval_sec;

    // 读取 Agent 标签和组配置
    let labels = std::env::var("OASIS_AGENT_LABELS")
        .unwrap_or_else(|_| String::new())
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let parts: Vec<&str> = s.splitn(2, '=').collect();
            if parts.len() == 2 {
                (parts[0].to_string(), parts[1].to_string())
            } else {
                (s.to_string(), "true".to_string())
            }
        })
        .collect::<std::collections::HashMap<String, String>>();

    let groups = std::env::var("OASIS_AGENT_GROUPS")
        .unwrap_or_else(|_| String::new())
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

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
    let nats_client = NatsClient::connect_with_oasis_config(&cfg).await?;

    // 生成唯一的 Agent ID
    let agent_id = AgentId::from(Uuid::new_v4().to_string());
    info!("Generated Agent ID: {}", agent_id);

    // 创建 Agent 实例（Agent 模型内部可不再依赖 AgentInfo，仅保留必要运行信息）
    let agent = Arc::new(RwLock::new(Agent::new(agent_id.clone())));

    // 创建命令执行器
    let command_executor = Arc::new(CommandExecutor::new());

    let system_monitor = Arc::new(SystemMonitor::new());
    let fact_repository = Arc::new(
        oasis_agent::infrastructure::nats::fact_repository::NatsFactRepository::new(
            nats_client.client.clone(),
            oasis_core::JS_KV_AGENT_FACTS.to_string(),
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
    let fact_service = FactService::new(
        agent_id.clone(),
        system_monitor.clone(),
        fact_repository.clone(),
    );

    info!("Agent started with id: {}", agent_id);

    // 启动时将 labels 与 groups 发布到 KV（groups 通过保留键写入）
    {
        let mut publish_labels = labels.clone();
        if !groups.is_empty() {
            publish_labels.insert("__groups".to_string(), groups.join(","));
        }
        let attrs_repo =
            oasis_agent::infrastructure::nats::attributes_repository::NatsAttributesRepository::new(
                &nats_client.client,
            );
        if let Err(e) = attrs_repo
            .publish_attributes(&agent_id, &publish_labels)
            .await
        {
            error!("Failed to publish startup labels/groups: {}", e);
        } else {
            info!("Published startup labels/groups to KV");
        }
    }

    {
        let mut agent_guard = agent.write().await;
        agent_guard.set_status(AgentStatus::Running);
    }
    info!("Agent status set to Running");

    // 启动后台服务
    let heartbeat_handle = tokio::spawn(heartbeat_service.run());

    // 启动 fact 收集服务
    let facts_handle = tokio::spawn({
        let mut fact_service_instance = fact_service;
        let shutdown_clone = shutdown.child_token();
        async move {
            if let Err(e) = fact_service_instance.start(shutdown_clone).await {
                error!("Fact service failed: {}", e);
            }
        }
    });
    let task_handle = tokio::spawn(task_processor.run());

    info!("All services started successfully");

    // 等待全局关闭信号
    shutdown.wait_for_signal().await;

    info!("Shutdown signal received, stopping services...");

    // 主动发送一次离线心跳（Protobuf），确保 Server 立即感知
    {
        use oasis_core::agent::{AgentHeartbeat, AgentStatus as CoreAgentStatus};
        let publisher = NatsPublisher::new(nats_client.clone());
        let offline_hb = AgentHeartbeat {
            agent_id: agent_id.clone(),
            status: CoreAgentStatus::Offline,
            last_seen: chrono::Utc::now().timestamp(),
            sequence: 0,
        };
        if let Err(e) = publisher.publish_heartbeat(&offline_hb).await {
            error!("Failed to publish offline heartbeat: {}", e);
        } else {
            info!("Published offline heartbeat");
        }
    }

    // 等待任务结束
    let _ = futures::future::join3(heartbeat_handle, task_handle, facts_handle).await;

    info!("Agent shut down gracefully");
    Ok(())
}
