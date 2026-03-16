//! Task multicast integration test.
//!
//! This test requires a real NATS server running with TLS.
//!
//! Setup steps:
//!   1. cargo build --release
//!   2. ./target/release/oasis-cli system init
//!   3. docker compose -f docker-compose.test.yml up -d
//!
//! Run tests:
//!   TEST_NATS_URL=tls://localhost:14222 cargo test --package oasis-core --test task_multicast_integration -- --ignored --nocapture --test-threads=1
//!
//! Cleanup:
//!   docker compose -f docker-compose.test.yml down -v

use oasis_agent::{nats_client::ManagedNatsClient, task_manager::TaskManager};
use oasis_core::{
    config::{NatsConfig, TlsConfig},
    constants::{JS_STREAM_GROUP_TASKS, JS_STREAM_RESULTS, JS_STREAM_TASKS},
    core_types::{AgentId, SelectorExpression},
    nats::NatsClientFactory,
    task_types::{BatchRequest, TaskState},
};
use oasis_server::infrastructure::{
    monitor::task_monitor::TaskMonitor, services::task_service::TaskService, streams,
};
use std::{env, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

fn get_test_nats_url() -> String {
    env::var("TEST_NATS_URL").unwrap_or_else(|_| {
        panic!(
            "TEST_NATS_URL environment variable not set.\n\
             Start NATS first: docker compose -f docker-compose.test.yml up -d\n\
             Then run: TEST_NATS_URL=tls://localhost:14222 cargo test"
        )
    })
}

fn get_workspace_root() -> PathBuf {
    env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn get_test_tls_config() -> TlsConfig {
    let certs_dir = get_workspace_root().join("certs");
    if !certs_dir.join("nats-ca.pem").exists() {
        panic!(
            "NATS CA certificate not found at {:?}\n\
             Run 'cargo build --release && ./target/release/oasis-cli system init' first",
            certs_dir.join("nats-ca.pem")
        );
    }

    TlsConfig {
        certs_dir,
        require_tls: true,
        renew_before_days: 30,
        renew_check_interval_sec: 6 * 3600,
    }
}

async fn wait_for_batch_success(
    task_service: &TaskService,
    batch_id: &oasis_core::core_types::BatchId,
    expected: usize,
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let details = task_service
                .get_batch_details(batch_id, None)
                .await
                .expect("batch details should be available");

            if details.len() == expected
                && details
                    .iter()
                    .all(|execution| execution.state == TaskState::Success)
            {
                break;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("batch should complete successfully before timeout");
}

#[tokio::test]
#[ignore]
async fn test_group_multicast_executes_once_per_group_and_fan_outs_to_all_agents() {
    let nats_url = get_test_nats_url();
    let tls = get_test_tls_config();
    let nats = NatsConfig {
        url: nats_url.clone(),
    };

    let server_client = NatsClientFactory::create_nats_client_with_jetstream(&nats, &tls)
        .await
        .expect("server should connect to NATS");
    let jetstream = Arc::new(server_client.jetstream);

    streams::ensure_streams(&jetstream)
        .await
        .expect("server streams should exist");

    let mut tasks_stream = jetstream
        .get_stream(JS_STREAM_TASKS)
        .await
        .expect("tasks stream should exist");
    tasks_stream.purge().await.expect("purge tasks stream");

    let mut group_tasks_stream = jetstream
        .get_stream(JS_STREAM_GROUP_TASKS)
        .await
        .expect("group tasks stream should exist");
    group_tasks_stream
        .purge()
        .await
        .expect("purge group tasks stream");

    let results_stream = jetstream
        .get_stream(JS_STREAM_RESULTS)
        .await
        .expect("results stream should exist");
    results_stream.purge().await.expect("purge results stream");

    let shutdown = CancellationToken::new();
    let task_monitor = Arc::new(TaskMonitor::new(jetstream.clone(), shutdown.clone()));
    let task_monitor_handle = task_monitor.clone().spawn();
    let task_service = TaskService::new(jetstream.clone(), task_monitor.clone())
        .await
        .expect("task service should initialize");

    let agent_one = ManagedNatsClient::connect_with_oasis_config(&nats, &tls)
        .await
        .expect("first agent should connect");
    let agent_two = ManagedNatsClient::connect_with_oasis_config(&nats, &tls)
        .await
        .expect("second agent should connect");

    let agent_one_manager = TaskManager::new(
        AgentId::new("group-agent-1"),
        agent_one,
        shutdown.child_token(),
        vec!["web".to_string()],
    );
    let agent_two_manager = TaskManager::new(
        AgentId::new("group-agent-2"),
        agent_two,
        shutdown.child_token(),
        vec!["web".to_string()],
    );

    let agent_one_handle = tokio::spawn({
        let manager = agent_one_manager.clone();
        async move { manager.run().await }
    });
    let agent_two_handle = tokio::spawn({
        let manager = agent_two_manager.clone();
        async move { manager.run().await }
    });

    tokio::time::sleep(Duration::from_millis(800)).await;

    let batch_id = task_service
        .submit_batch(
            BatchRequest {
                command: "/bin/sleep".to_string(),
                args: vec!["1".to_string()],
                selector: SelectorExpression::new("\"web\" in groups"),
                timeout_seconds: 10,
            },
            vec![AgentId::new("group-agent-1"), AgentId::new("group-agent-2")],
        )
        .await
        .expect("batch submission should succeed");

    tokio::time::sleep(Duration::from_millis(150)).await;

    let group_messages = group_tasks_stream
        .info()
        .await
        .expect("group stream info")
        .state
        .messages;
    let unicast_messages = tasks_stream
        .info()
        .await
        .expect("tasks stream info")
        .state
        .messages;

    assert_eq!(
        group_messages, 1,
        "group selector should publish one multicast message"
    );
    assert_eq!(
        unicast_messages, 0,
        "pure group selector should not publish per-agent unicast task messages"
    );
    wait_for_batch_success(&task_service, &batch_id, 2).await;

    shutdown.cancel();
    let _ = agent_one_handle.await;
    let _ = agent_two_handle.await;
    let _ = task_monitor_handle.await;
}
