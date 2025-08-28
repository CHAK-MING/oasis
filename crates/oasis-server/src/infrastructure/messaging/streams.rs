use anyhow::Result;
use oasis_core::{
    JS_KV_DLQ, JS_OBJ_ARTIFACTS, JS_STREAM_RESULTS, JS_STREAM_TASKS, JS_STREAM_TASKS_DLQ,
};
use std::time::Duration;
use tracing::{error, info, warn};

/// 确保所有必要的JetStream流存在
pub async fn ensure_streams(js: &async_nats::jetstream::Context) -> Result<()> {
    info!("Starting JetStream streams initialization...");
    info!("This is the single authority to create/update streams and KV buckets");

    // 硬编码配置值
    let tasks_max_age_sec = 3600; // 1小时
    let tasks_max_msgs = 10000;
    let tasks_max_bytes = 1024 * 1024 * 100; // 100MB

    let results_max_age_sec = 86400; // 24小时
    let results_max_msgs = 50000;
    let results_max_bytes = 1024 * 1024 * 500; // 500MB

    let dlq_max_age_sec = 604800; // 7天
    let dlq_max_msgs = 1000;
    let dlq_max_bytes = 1024 * 1024 * 10; // 10MB

    let artifacts_max_age_sec = 2592000; // 30天
    let artifacts_max_bytes = 1024 * 1024 * 1024; // 1GB

    // 任务流
    info!("Creating/updating tasks stream...");
    let desired_tasks_cfg = async_nats::jetstream::stream::Config {
        name: JS_STREAM_TASKS.to_string(),
        subjects: vec![
            "tasks.exec.default".to_string(),
            "tasks.exec.agent.>".to_string(),
        ],
        retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
        max_age: Duration::from_secs(tasks_max_age_sec),
        duplicate_window: Duration::from_secs(oasis_core::DUPLICATE_WINDOW_TASKS_SECS),
        num_replicas: 1,
        storage: async_nats::jetstream::stream::StorageType::File,
        max_messages: tasks_max_msgs,
        max_bytes: tasks_max_bytes,
        ..Default::default()
    };
    ensure_or_update_stream(js, JS_STREAM_TASKS, desired_tasks_cfg).await?;

    // 结果流
    info!("Creating/updating results stream...");
    let desired_results_cfg = async_nats::jetstream::stream::Config {
        name: JS_STREAM_RESULTS.to_string(),
        subjects: vec!["results.>".to_string()],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        max_age: Duration::from_secs(results_max_age_sec),
        duplicate_window: Duration::from_secs(oasis_core::DUPLICATE_WINDOW_RESULTS_SECS),
        num_replicas: 1,
        storage: async_nats::jetstream::stream::StorageType::File,
        max_messages: results_max_msgs,
        max_bytes: results_max_bytes,
        ..Default::default()
    };
    ensure_or_update_stream(js, JS_STREAM_RESULTS, desired_results_cfg).await?;

    // DLQ流
    info!("Creating/updating DLQ stream...");
    let desired_dlq_cfg = async_nats::jetstream::stream::Config {
        name: JS_STREAM_TASKS_DLQ.to_string(),
        subjects: vec!["tasks.dlq.>".to_string()],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        max_age: Duration::from_secs(dlq_max_age_sec),
        num_replicas: 1,
        storage: async_nats::jetstream::stream::StorageType::File,
        max_messages: dlq_max_msgs,
        max_bytes: dlq_max_bytes,
        ..Default::default()
    };
    ensure_or_update_stream(js, JS_STREAM_TASKS_DLQ, desired_dlq_cfg).await?;

    // DLQ KV 存储（用于持久化 DLQ 条目以便审计检索）
    match js.get_key_value(JS_KV_DLQ).await {
        Ok(_) => info!("DLQ KV bucket already exists"),
        Err(e) => {
            warn!("DLQ KV bucket not found, creating: {:?}", e);
            let mut cfg = async_nats::jetstream::kv::Config::default();
            cfg.bucket = JS_KV_DLQ.to_string();
            cfg.description = "Dead Letter Entries".to_string();
            cfg.history = 1;
            cfg.max_age = Duration::from_secs(dlq_max_age_sec);
            cfg.max_bytes = dlq_max_bytes;
            cfg.storage = async_nats::jetstream::stream::StorageType::File;
            cfg.num_replicas = 1;
            let backoff = oasis_core::backoff::kv_operations_backoff();
            oasis_core::backoff::execute_with_backoff(
                || {
                    let js = js.clone();
                    let cfg = cfg.clone();
                    async move {
                        info!("Creating DLQ KV bucket {}", JS_KV_DLQ);
                        js.create_key_value(cfg.clone())
                            .await
                            .map(|_| ())
                            .map_err(|e| anyhow::anyhow!(e))
                    }
                },
                backoff,
            )
            .await?;
        }
    }

    // 统一确保常用 KV buckets 存在
    ensure_kv_buckets(js).await?;

    // 对象存储
    info!("Creating/updating artifacts object store...");
    let _ = match js.get_object_store(JS_OBJ_ARTIFACTS).await {
        Ok(s) => {
            info!("Artifacts object store already exists");
            s
        }
        Err(e) => {
            warn!("Artifacts object store not found, creating: {:?}", e);
            let cfg = async_nats::jetstream::object_store::Config {
                bucket: JS_OBJ_ARTIFACTS.to_string(),
                description: Some("File artifacts storage".to_string()),
                max_bytes: artifacts_max_bytes,
                max_age: Duration::from_secs(artifacts_max_age_sec),
                storage: async_nats::jetstream::stream::StorageType::File,
                num_replicas: 1,
                ..Default::default()
            };
            // 带 backoff 的创建
            let backoff = oasis_core::backoff::kv_operations_backoff();
            oasis_core::backoff::execute_with_backoff(
                || {
                    let js = js.clone();
                    let cfg = cfg.clone();
                    async move {
                        info!("Attempting to create artifacts object store...");
                        let result = js.create_object_store(cfg.clone()).await;
                        match &result {
                            Ok(_) => info!("Successfully created artifacts object store"),
                            Err(e) => error!("Failed to create artifacts object store: {:?}", e),
                        }
                        result.map(|_| ()).map_err(|e| anyhow::anyhow!(e))
                    }
                },
                backoff,
            )
            .await?;
            js.get_object_store(JS_OBJ_ARTIFACTS).await?
        }
    };

    info!("All JetStream streams and object stores ensured successfully");
    Ok(())
}

/// 统一确保常用 KV buckets 存在
pub async fn ensure_kv_buckets(js: &async_nats::jetstream::Context) -> Result<()> {
    use oasis_core::{
        JS_KV_AGENT_FACTS, JS_KV_AGENT_HEARTBEAT, JS_KV_AGENT_LABELS, JS_KV_ROLLOUTS, JS_KV_TASK_STATE,
    };
    let mut kv_specs: Vec<(String, async_nats::jetstream::kv::Config)> = Vec::new();

    // 心跳（TTL型）
    {
        let mut cfg = async_nats::jetstream::kv::Config::default();
        cfg.bucket = JS_KV_AGENT_HEARTBEAT.to_string();
        cfg.description = "Agent heartbeat (TTL-based cleanup)".to_string();
        cfg.history = 1;
        cfg.max_age = std::time::Duration::from_secs(90);
        kv_specs.push((cfg.bucket.clone(), cfg));
    }

    // Facts（版本化不TTL）
    {
        let mut cfg = async_nats::jetstream::kv::Config::default();
        cfg.bucket = JS_KV_AGENT_FACTS.to_string();
        cfg.description = "Agent facts (versioned, no TTL)".to_string();
        cfg.history = 50;
        cfg.max_value_size = 65536;
        kv_specs.push((cfg.bucket.clone(), cfg));
    }

    // Labels（版本化不TTL）
    {
        let mut cfg = async_nats::jetstream::kv::Config::default();
        cfg.bucket = JS_KV_AGENT_LABELS.to_string();
        cfg.description = "Agent labels (versioned, no TTL)".to_string();
        cfg.history = 50;
        cfg.max_value_size = 65536;
        kv_specs.push((cfg.bucket.clone(), cfg));
    }

    // 逐个确保存在
    for (_name, cfg) in kv_specs {
        if js.get_key_value(&cfg.bucket).await.is_err() {
            let backoff = oasis_core::backoff::kv_operations_backoff();
            oasis_core::backoff::execute_with_backoff(
                || {
                    let js = js.clone();
                    let cfg = cfg.clone();
                    async move {
                        js.create_key_value(cfg.clone())
                            .await
                            .map(|_| ())
                            .map_err(|e| anyhow::anyhow!(e))
                    }
                },
                backoff,
            )
            .await?;
        }
    }
    // 确保任务状态 KV 存储存在（用于 Agent 持久化任务状态）
    if js.get_key_value(JS_KV_TASK_STATE).await.is_err() {
        let mut cfg = async_nats::jetstream::kv::Config::default();
        cfg.bucket = JS_KV_TASK_STATE.to_string();
        cfg.description = "Agent task state persistence".to_string();
        cfg.history = 10;
        cfg.max_age = std::time::Duration::from_secs(86400);
        let backoff = oasis_core::backoff::kv_operations_backoff();
        oasis_core::backoff::execute_with_backoff(
            || {
                let js = js.clone();
                let cfg = cfg.clone();
                async move {
                    js.create_key_value(cfg.clone())
                        .await
                        .map(|_| ())
                        .map_err(|e| anyhow::anyhow!(e))
                }
            },
            backoff,
        )
        .await?;
    }
    // Rollouts（版本化不TTL）
    if js.get_key_value(JS_KV_ROLLOUTS).await.is_err() {
        let mut cfg = async_nats::jetstream::kv::Config::default();
        cfg.bucket = JS_KV_ROLLOUTS.to_string();
        cfg.description = "Rollouts (versioned, no TTL)".to_string();
        cfg.history = 50;
        cfg.max_value_size = 65536;
        let backoff = oasis_core::backoff::kv_operations_backoff();
        oasis_core::backoff::execute_with_backoff(
            || {
                let js = js.clone();
                let cfg = cfg.clone();
                async move {
                    js.create_key_value(cfg.clone())
                        .await
                        .map(|_| ())
                        .map_err(|e| anyhow::anyhow!(e))
                }
            },
            backoff,
        )
        .await?;
    }
    Ok(())
}

/// 创建标准化的 Pull 型 Durable Consumer（统一显式 ACK、有界重试等配置）
pub async fn create_standard_pull_consumer(
    js: &async_nats::jetstream::Context,
    stream: &str,
    durable_name: String,
    filter_subject: String,
    ack_wait: std::time::Duration,
    max_deliver: i64,
    max_ack_pending: i64,
) -> Result<async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>>
{
    // 统一先确保流配置
    ensure_streams(js).await?;

    let cfg = async_nats::jetstream::consumer::pull::Config {
        durable_name: Some(durable_name),
        filter_subject,
        deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
        ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
        max_deliver,
        ack_wait,
        max_ack_pending,
        inactive_threshold: std::time::Duration::from_secs(600),
        ..Default::default()
    };

    let consumer = js.create_consumer_on_stream(cfg, stream).await?;
    Ok(consumer)
}

/// 使用 backoff 确保流存在，且在存在时对比配置并尝试更新
async fn ensure_or_update_stream(
    js: &async_nats::jetstream::Context,
    name: &str,
    desired: async_nats::jetstream::stream::Config,
) -> Result<()> {
    info!("Checking stream: {}", name);
    match js.get_stream(name).await {
        Ok(mut existing) => {
            info!("Stream {} already exists, checking configuration...", name);
            // 对比关键配置差异，并尝试更新
            let info = existing.info().await?;
            let current = info.config.clone();
            if !stream_cfg_equivalent(&current, &desired) {
                warn!(stream = %name, "JetStream stream config drift detected, applying update");
                let mut new_cfg = desired.clone();
                // NATS 要求 update 时包含相同的名称
                new_cfg.name = name.to_string();

                let backoff = oasis_core::backoff::kv_operations_backoff();
                oasis_core::backoff::execute_with_backoff(
                    || {
                        let js = js.clone();
                        let cfg = new_cfg.clone();
                        async move {
                            info!("Updating stream {} configuration...", name);
                            let result = js.update_stream(cfg.clone()).await;
                            match &result {
                                Ok(_) => info!("Successfully updated stream {}", name),
                                Err(e) => error!("Failed to update stream {}: {:?}", name, e),
                            }
                            result.map(|_| ()).map_err(|e| anyhow::anyhow!(e))
                        }
                    },
                    backoff,
                )
                .await?;
            } else {
                info!("Stream {} configuration is up to date", name);
            }
            Ok(())
        }
        Err(e) => {
            warn!("Stream {} not found, creating: {:?}", name, e);
            // 不存在则创建（带 backoff）
            let backoff = oasis_core::backoff::kv_operations_backoff();
            oasis_core::backoff::execute_with_backoff(
                || {
                    let js = js.clone();
                    let cfg = desired.clone();
                    async move {
                        info!("Creating stream {}...", name);
                        let result = js.create_stream(cfg.clone()).await;
                        match &result {
                            Ok(_) => info!("Successfully created stream {}", name),
                            Err(e) => error!("Failed to create stream {}: {:?}", name, e),
                        }
                        result.map(|_| ()).map_err(|e| anyhow::anyhow!(e))
                    }
                },
                backoff,
            )
            .await?;
            Ok(())
        }
    }
}

fn stream_cfg_equivalent(
    a: &async_nats::jetstream::stream::Config,
    b: &async_nats::jetstream::stream::Config,
) -> bool {
    a.subjects == b.subjects
        && a.retention == b.retention
        && a.max_age == b.max_age
        && a.duplicate_window == b.duplicate_window
        && a.num_replicas == b.num_replicas
        && a.storage == b.storage
        && a.max_messages == b.max_messages
        && a.max_bytes == b.max_bytes
}
