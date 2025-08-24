use anyhow::Result;
use oasis_agent::{
    application::services::{
        agent_config_listener::AgentConfigListener, fact_service::FactService,
        heartbeat::HeartbeatService, task_processor::TaskProcessor,
    },
    config::AgentConfig,
    domain::{Agent, AgentStatus},
    infrastructure::{
        nats::{client::NatsClient, fact_repository::NatsFactRepository},
        system::{executor::CommandExecutor, fact_collector::SystemMonitor},
    },
};
use oasis_core::{
    rate_limit::RateLimiterCollection,
    shutdown::GracefulShutdown,
    telemetry::{LogConfig, init_tracing_with},
    types::AgentId,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing_with(&LogConfig {
        level: "info".to_string(),
        format: "text".to_string(),
        no_ansi: false,
    });

    let agent_config = AgentConfig::load_smart().await?;

    init_tracing_with(&LogConfig {
        level: agent_config.common.telemetry.log_level.clone(),
        format: agent_config.common.telemetry.log_format.clone(),
        no_ansi: agent_config.common.telemetry.log_no_ansi,
    });

    info!("Starting Oasis Agent");

    let nats_client = NatsClient::connect(&agent_config.common.nats).await?;
    info!("Connected to NATS server");

    let agent = Arc::new(RwLock::new(Agent::new(
        agent_config.agent.agent_id.clone(),
        agent_config.attributes.clone(),
    )));

    let _agent_listener = AgentConfigListener::new(agent.clone());

    let command_executor = Arc::new(CommandExecutor::new(agent_config.executor.clone()));

    let agent_id = AgentId::from(agent_config.agent.agent_id.clone());

    let system_monitor = Arc::new(SystemMonitor::new());
    let fact_repository = Arc::new(NatsFactRepository::new(
        nats_client.client.clone(),
        oasis_core::JS_KV_NODE_FACTS.to_string(),
        oasis_core::kv_key_facts(agent_id.as_str()),
    ));

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
        Arc::new(RwLock::new(agent_config.clone())),
        nats_client.clone(),
        command_executor.clone(),
        limiters.clone(),
        shutdown.clone(),
    );

    // Instantiate FactService here
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

    let heartbeat_handle = tokio::spawn(heartbeat_service.run());

    let facts_handle = tokio::spawn(async move {
        if let Err(e) = fact_service.start().await {
            error!("Fact service failed: {}", e);
        }
    });

    let task_handle = tokio::spawn(task_processor.run());

    info!("All services started successfully");

    shutdown.wait_for_signal().await;

    info!("Shutdown signal received, stopping services...");

    heartbeat_handle.abort();
    task_handle.abort();
    facts_handle.abort();

    info!("Agent shut down gracefully");
    Ok(())
}
