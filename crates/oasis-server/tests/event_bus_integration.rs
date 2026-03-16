//! Event bus integration test.
//!
//! This test requires a real NATS server running with TLS.
//!
//! Setup steps:
//!   1. cargo build --release
//!   2. ./target/release/oasis-cli system init
//!   3. docker compose -f docker-compose.test.yml up -d
//!
//! Run tests:
//!   TEST_NATS_URL=tls://localhost:14222 cargo test --package oasis-server --test event_bus_integration -- --ignored --nocapture --test-threads=1
//!
//! Cleanup:
//!   docker compose -f docker-compose.test.yml down -v

use oasis_core::{
    config::{NatsConfig, TlsConfig},
    constants::JS_STREAM_EVENTS,
    core_types::{AgentId, OperationId},
    event_types::{OasisEvent, OasisEventKind},
    nats::NatsClientFactory,
};
use oasis_server::infrastructure::{services::event_bus::EventBus, streams};
use std::{env, path::PathBuf, sync::Arc, time::Duration};
use tokio_stream::StreamExt;

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

#[tokio::test]
#[ignore]
async fn test_event_bus_publishes_json_event_to_events_stream() {
    let nats = NatsConfig {
        url: get_test_nats_url(),
    };
    let tls = get_test_tls_config();

    let server_client = NatsClientFactory::create_nats_client_with_jetstream(&nats, &tls)
        .await
        .expect("server should connect to NATS");
    let jetstream = Arc::new(server_client.jetstream);
    streams::ensure_streams(&jetstream)
        .await
        .expect("streams should exist");

    let events_stream = jetstream
        .get_stream(JS_STREAM_EVENTS)
        .await
        .expect("events stream should exist");
    events_stream.purge().await.expect("purge events stream");

    let client = NatsClientFactory::connect_with_config(&nats, &tls)
        .await
        .expect("plain client should connect");
    let mut subscriber = client
        .subscribe("events.agent.online.>".to_string())
        .await
        .expect("should subscribe to events subject");

    let event = OasisEvent::new(OasisEventKind::AgentOnline {
        agent_id: AgentId::new("event-agent-1"),
    });
    let expected_subject = event.subject();
    let event_bus = EventBus::new(jetstream.clone());
    event_bus
        .publish(&event)
        .await
        .expect("event bus should publish event");

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("event should arrive before timeout")
        .expect("subscriber should yield one message");

    assert_eq!(received.subject.to_string(), expected_subject);
    let decoded: OasisEvent =
        serde_json::from_slice(&received.payload).expect("payload should decode as OasisEvent");
    assert_eq!(decoded.event_id, event.event_id);
    assert_eq!(decoded.subject(), expected_subject);
}

#[tokio::test]
#[ignore]
async fn test_event_bus_publishes_file_apply_failed_event() {
    let nats = NatsConfig {
        url: get_test_nats_url(),
    };
    let tls = get_test_tls_config();

    let server_client = NatsClientFactory::create_nats_client_with_jetstream(&nats, &tls)
        .await
        .expect("server should connect to NATS");
    let jetstream = Arc::new(server_client.jetstream);
    streams::ensure_streams(&jetstream)
        .await
        .expect("streams should exist");

    let events_stream = jetstream
        .get_stream(JS_STREAM_EVENTS)
        .await
        .expect("events stream should exist");
    events_stream.purge().await.expect("purge events stream");

    let client = NatsClientFactory::connect_with_config(&nats, &tls)
        .await
        .expect("plain client should connect");
    let mut subscriber = client
        .subscribe("events.file.apply_failed.>".to_string())
        .await
        .expect("should subscribe to file failure events");

    let event = OasisEvent::new(OasisEventKind::FileApplyFailed {
        operation_id: OperationId::new("123e4567-e89b-12d3-a456-426614174000".to_string()),
        agent_id: AgentId::new("event-agent-2"),
        source_path: "/tmp/app.conf".to_string(),
        destination_path: "/etc/app.conf".to_string(),
        revision: 42,
        reason: "permission denied".to_string(),
    });
    let event_bus = EventBus::new(jetstream.clone());
    event_bus
        .publish(&event)
        .await
        .expect("event bus should publish file event");

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("file event should arrive before timeout")
        .expect("subscriber should yield one message");

    let decoded: OasisEvent =
        serde_json::from_slice(&received.payload).expect("payload should decode as OasisEvent");
    match decoded.kind {
        OasisEventKind::FileApplyFailed {
            agent_id,
            revision,
            reason,
            ..
        } => {
            assert_eq!(agent_id, AgentId::new("event-agent-2"));
            assert_eq!(revision, 42);
            assert_eq!(reason, "permission denied");
        }
        other => panic!("unexpected event kind: {:?}", other),
    }
}
