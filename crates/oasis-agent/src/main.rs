use anyhow::Result;
use oasis_agent::{
    application::services::{
        agent_config_listener::AgentConfigListener, fact_service::FactService,
        heartbeat::HeartbeatService, task_processor::TaskProcessor,
    },
    config::AgentConfig,
    domain::Agent,
    infrastructure::{
        nats::{client::NatsClient, fact_repository::NatsFactRepository},
        system::{executor::CommandExecutor, fact_collector::SystemMonitor},
    },
};
use oasis_core::{
    config::{ConfigProvider, ConfigSource, DefaultConfigListener},
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
    // 加载统一配置
    let agent_config = AgentConfig::load_smart().await?;

    // 初始化日志
    init_tracing_with(&LogConfig {
        level: agent_config.common.telemetry.log_level.clone(),
        format: agent_config.common.telemetry.log_format.clone(),
        no_ansi: agent_config.common.telemetry.log_no_ansi,
    });

    info!("Starting Oasis Agent");

    // 创建 NATS 客户端
    let nats_client = NatsClient::connect(&agent_config.common.nats).await?;

    info!("Connected to NATS server");

    // 创建共享状态
    let agent = Arc::new(RwLock::new(Agent::new(
        agent_config.agent.agent_id.clone(),
        agent_config.attributes.clone(),
    )));

    // 创建配置提供者和监听器
    let config_provider = Arc::new(ConfigProvider::new(agent_config.clone()));

    // 添加 Agent 配置监听器
    let agent_listener = Box::new(AgentConfigListener::new(agent.clone()));
    config_provider.watch_config(agent_listener).await;

    // 添加默认配置监听器（用于日志记录）
    let default_listener = Box::new(DefaultConfigListener::<AgentConfig>::new());
    config_provider.watch_config(default_listener).await;

    // 创建基础设施组件
    let command_executor = Arc::new(CommandExecutor::new(agent_config.executor.clone()));

    // 创建事实收集器与仓库
    let system_monitor = Arc::new(SystemMonitor::new());
    let fact_repository = Arc::new(NatsFactRepository::new(&nats_client.client));

    // Agent 标识
    let agent_id = AgentId::from(agent_config.agent.agent_id.clone());

    // 创建限流器
    let limiters = Arc::new(RateLimiterCollection::default());

    // 创建优雅关闭
    let shutdown = GracefulShutdown::new();

    // 启动服务

    // 启动心跳服务
    let heartbeat_service = HeartbeatService::new(
        agent.clone(),
        nats_client.clone(),
        limiters.clone(),
        shutdown.clone(),
    );

    // 获取配置的共享引用
    let config_rx = config_provider.get_watcher().subscribe();

    // 创建事实服务
    let fact_service = FactService::new(agent_id.clone(), system_monitor, fact_repository);

    info!("Agent started with id: {}", agent_id);

    // 将 Agent 状态设置为 Running
    {
        let mut agent_guard = agent.write().await;
        agent_guard.start();
    }
    info!("Agent status set to Running");

    // 第二步：在配置加载后，才创建并启动依赖配置的服务
    let task_processor = TaskProcessor::new(
        agent.clone(),
        Arc::new(RwLock::new(config_rx.borrow().clone())), // 现在 config 已经包含了真实数据
        nats_client.clone(),
        command_executor.clone(),
        limiters.clone(),
        shutdown.clone(),
    );

    // 启动配置热更新监听（基于 NATS KV）
    let config_sources = vec![ConfigSource::NatsKv {
        client: nats_client.client.clone(),
        bucket: "oasis-config".to_string(),
        prefix: format!("agent.{}", agent_id),
        watch: true,
    }];

    let config_watcher_handle = {
        let watcher = config_provider.get_watcher();
        tokio::spawn(async move {
            if let Err(e) = watcher.start_watching(config_sources).await {
                error!("Failed to start config watching: {}", e);
            }
        })
    };

    // 第三步：启动心跳服务（不依赖配置）
    let heartbeat_handle = tokio::spawn(heartbeat_service.run());

    // 第四步：启动事实服务（不依赖配置）
    let facts_handle = tokio::spawn(async move {
        let mut fact_service = fact_service;
        if let Err(e) = fact_service.start().await {
            error!("Fact service failed: {}", e);
        }
    });

    // 第五步：启动任务处理器（依赖配置）
    let task_handle = tokio::spawn(task_processor.run());

    info!("All services started successfully");

    // 等待关闭信号
    shutdown.wait_for_signal().await;

    info!("Shutdown signal received, stopping services...");

    // 停止所有服务
    heartbeat_handle.abort();
    task_handle.abort();
    facts_handle.abort();
    config_watcher_handle.abort();

    info!("Agent shut down gracefully");
    Ok(())
}
