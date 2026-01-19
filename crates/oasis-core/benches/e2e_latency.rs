//! End-to-end latency benchmark: Server → NATS → Agent → NATS → Server
//!
//! Simulates the full command dispatch and response cycle.
//! Measures P50, P95, P99, P999 latencies.
//!
//! Enable with: OASIS_BENCH_ENABLE_NATS_IO=1

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::StreamExt;
use oasis_core::config::{NatsConfig, TlsConfig};
use oasis_core::nats::NatsClientFactory;
use prost::Message;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn enabled() -> bool {
    std::env::var("OASIS_BENCH_ENABLE_NATS_IO")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn build_cfg() -> (NatsConfig, TlsConfig) {
    let url = std::env::var("OASIS_BENCH_NATS_URL")
        .unwrap_or_else(|_| "tls://127.0.0.1:4222".to_string());
    let certs_dir = std::env::var("OASIS_BENCH_CERTS_DIR").unwrap_or_else(|_| "certs".to_string());
    (
        NatsConfig { url },
        TlsConfig {
            certs_dir: certs_dir.into(),
        },
    )
}

fn create_task_payload(task_id: &str) -> Vec<u8> {
    let task = oasis_core::proto::TaskMsg {
        task_id: Some(oasis_core::proto::TaskId {
            value: task_id.to_string(),
        }),
        batch_id: Some(oasis_core::proto::BatchId {
            value: "bench-batch".to_string(),
        }),
        agent_id: Some(oasis_core::proto::AgentId {
            value: "bench-agent".to_string(),
        }),
        command: "echo".to_string(),
        args: vec!["hello".to_string()],
        timeout_seconds: 30,
        state: 1,
        created_at: 0,
        updated_at: 0,
    };
    task.encode_to_vec()
}

fn create_execution_payload(task_id: &str) -> Vec<u8> {
    let exec = oasis_core::proto::TaskExecutionMsg {
        task_id: Some(oasis_core::proto::TaskId {
            value: task_id.to_string(),
        }),
        agent_id: Some(oasis_core::proto::AgentId {
            value: "bench-agent".to_string(),
        }),
        state: 3,
        exit_code: Some(0),
        stdout: "hello".to_string(),
        stderr: String::new(),
        duration_ms: Some(1.5),
        started_at: 0,
        finished_at: Some(0),
    };
    exec.encode_to_vec()
}

fn calculate_percentiles(mut latencies: Vec<Duration>) -> (Duration, Duration, Duration, Duration) {
    latencies.sort();
    let len = latencies.len();
    if len == 0 {
        return (
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        );
    }
    let p50 = latencies[len * 50 / 100];
    let p95 = latencies[len * 95 / 100];
    let p99 = latencies[len * 99 / 100];
    let p999 = latencies[len.saturating_sub(1).min(len * 999 / 1000)];
    (p50, p95, p99, p999)
}

fn bench_e2e_roundtrip_latency(c: &mut Criterion) {
    if !enabled() {
        eprintln!("[e2e_latency] Skipped. Set OASIS_BENCH_ENABLE_NATS_IO=1 to enable.");
        c.bench_function("e2e_latency/disabled", |b| b.iter(|| black_box(0u64)));
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (nats, tls) = build_cfg();

    let setup_res: anyhow::Result<(async_nats::Client, String, String)> = rt.block_on(async {
        let client = NatsClientFactory::connect_with_config(&nats, &tls).await?;

        let task_subject = format!("oasis.bench.task.{}", uuid::Uuid::now_v7());
        let result_subject = format!("oasis.bench.result.{}", uuid::Uuid::now_v7());

        let mut task_sub = client.subscribe(task_subject.clone()).await?;
        let agent_client = client.clone();
        let result_subj = result_subject.clone();

        tokio::spawn(async move {
            while let Some(msg) = task_sub.next().await {
                if let Ok(task) = oasis_core::proto::TaskMsg::decode(msg.payload.as_ref()) {
                    let task_id = task
                        .task_id
                        .map(|t| t.value)
                        .unwrap_or_else(|| "unknown".to_string());
                    let exec_payload = create_execution_payload(&task_id);
                    let _ = agent_client
                        .publish(result_subj.clone(), Bytes::from(exec_payload))
                        .await;
                }
            }
        });

        let warmup_payload = Bytes::from(create_task_payload("warmup"));
        for _ in 0..100u32 {
            client
                .publish(task_subject.clone(), warmup_payload.clone())
                .await?;
        }
        client.flush().await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok((client, task_subject, result_subject))
    });

    let (client, task_subject, result_subject) = match setup_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[e2e_latency] Setup failed: {:#}", e);
            c.bench_function("e2e_latency/roundtrip(setup_failed)", |b| {
                b.iter(|| black_box(0u64))
            });
            return;
        }
    };

    let mut group = c.benchmark_group("e2e_latency");
    group.sample_size(200);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("roundtrip", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let mut result_sub = client.subscribe(result_subject.clone()).await.unwrap();
                let mut total = Duration::ZERO;

                let payload = Bytes::from(create_task_payload("bench"));
                for _ in 0..iters {
                    let start = Instant::now();
                    client
                        .publish(task_subject.clone(), payload.clone())
                        .await
                        .unwrap();

                    let _ = tokio::time::timeout(Duration::from_secs(5), result_sub.next()).await;
                    total += start.elapsed();
                }

                total
            })
        })
    });

    group.finish();

    eprintln!("\n[e2e_latency] Running detailed percentile analysis (1000 samples)...");
    let latencies: Vec<Duration> = rt.block_on(async {
        let mut result_sub = client.subscribe(result_subject.clone()).await.unwrap();
        let mut lats = Vec::with_capacity(1000);

        let payload = Bytes::from(create_task_payload("percentile"));
        for _ in 0..1000u32 {
            let start = Instant::now();
            client
                .publish(task_subject.clone(), payload.clone())
                .await
                .unwrap();

            if let Ok(Some(_)) =
                tokio::time::timeout(Duration::from_secs(5), result_sub.next()).await
            {
                lats.push(start.elapsed());
            }
        }
        lats
    });

    let (p50, p95, p99, p999) = calculate_percentiles(latencies.clone());
    let avg: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;

    eprintln!("╔══════════════════════════════════════════════════════════════╗");
    eprintln!("║           E2E Round-Trip Latency (Server→Agent→Server)       ║");
    eprintln!("╠══════════════════════════════════════════════════════════════╣");
    eprintln!(
        "║  Samples: {:>6}                                             ║",
        latencies.len()
    );
    eprintln!(
        "║  Average: {:>10.3} ms                                      ║",
        avg.as_secs_f64() * 1000.0
    );
    eprintln!(
        "║  P50:     {:>10.3} ms                                      ║",
        p50.as_secs_f64() * 1000.0
    );
    eprintln!(
        "║  P95:     {:>10.3} ms                                      ║",
        p95.as_secs_f64() * 1000.0
    );
    eprintln!(
        "║  P99:     {:>10.3} ms                                      ║",
        p99.as_secs_f64() * 1000.0
    );
    eprintln!(
        "║  P99.9:   {:>10.3} ms                                      ║",
        p999.as_secs_f64() * 1000.0
    );
    eprintln!("╚══════════════════════════════════════════════════════════════╝");
}

fn bench_dispatch_latency(c: &mut Criterion) {
    if !enabled() {
        c.bench_function("e2e_latency/dispatch_disabled", |b| {
            b.iter(|| black_box(0u64))
        });
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (nats, tls) = build_cfg();

    let setup_res: anyhow::Result<(async_nats::Client, String, Arc<AtomicU64>)> =
        rt.block_on(async {
            let client = NatsClientFactory::connect_with_config(&nats, &tls).await?;
            let subject = format!("oasis.bench.dispatch.{}", uuid::Uuid::now_v7());
            let counter = Arc::new(AtomicU64::new(0));

            let mut sub = client.subscribe(subject.clone()).await?;
            let cnt = counter.clone();
            tokio::spawn(async move {
                while let Some(_msg) = sub.next().await {
                    cnt.fetch_add(1, Ordering::Relaxed);
                }
            });

            for _ in 0..100u32 {
                let payload = create_task_payload("warmup");
                client
                    .publish(subject.clone(), Bytes::from(payload))
                    .await?;
            }
            client.flush().await?;
            tokio::time::sleep(Duration::from_millis(50)).await;

            Ok((client, subject, counter))
        });

    let (client, subject, _counter) = match setup_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[e2e_latency] Dispatch setup failed: {:#}", e);
            c.bench_function("e2e_latency/dispatch(setup_failed)", |b| {
                b.iter(|| black_box(0u64))
            });
            return;
        }
    };

    let mut group = c.benchmark_group("e2e_latency");

    group.bench_function("dispatch_only", |b| {
        b.iter(|| {
            rt.block_on(async {
                let payload = create_task_payload("bench");
                client
                    .publish(subject.clone(), Bytes::from(payload))
                    .await
                    .unwrap();
                client.flush().await.unwrap();
            });
            black_box(())
        })
    });

    group.finish();
}

fn bench_concurrent_agents(c: &mut Criterion) {
    if !enabled() {
        c.bench_function("e2e_latency/concurrent_disabled", |b| {
            b.iter(|| black_box(0u64))
        });
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (nats, tls) = build_cfg();

    for agent_count in [10, 100, 500, 1000] {
        let setup_res: anyhow::Result<(async_nats::Client, String, String)> = rt.block_on(async {
            let client = NatsClientFactory::connect_with_config(&nats, &tls).await?;
            let task_subject = format!("oasis.bench.concurrent.task.{}", uuid::Uuid::now_v7());
            let result_subject = format!("oasis.bench.concurrent.result.{}", uuid::Uuid::now_v7());

            for _agent_id in 0..agent_count {
                let mut task_sub = client.subscribe(task_subject.clone()).await?;
                let agent_client = client.clone();
                let result_subj = result_subject.clone();

                tokio::spawn(async move {
                    while let Some(msg) = task_sub.next().await {
                        if let Ok(task) = oasis_core::proto::TaskMsg::decode(msg.payload.as_ref()) {
                            let task_id = task
                                .task_id
                                .map(|t| t.value)
                                .unwrap_or_else(|| "unknown".to_string());
                            let exec_payload = create_execution_payload(&task_id);
                            let _ = agent_client
                                .publish(result_subj.clone(), Bytes::from(exec_payload))
                                .await;
                        }
                    }
                });
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok((client, task_subject, result_subject))
        });

        let (client, task_subject, result_subject) = match setup_res {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[e2e_latency] Concurrent {} agents setup failed: {:#}",
                    agent_count, e
                );
                continue;
            }
        };

        let mut group = c.benchmark_group("e2e_latency");
        group.sample_size(50);

        group.bench_with_input(
            BenchmarkId::new("broadcast_to_agents", agent_count),
            &agent_count,
            |b, &cnt| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut result_sub =
                            client.subscribe(result_subject.clone()).await.unwrap();
                        let mut total = Duration::ZERO;

                        for i in 0..iters {
                            let task_id = format!("concurrent-{}", i);
                            let payload = create_task_payload(&task_id);

                            let start = Instant::now();
                            client
                                .publish(task_subject.clone(), Bytes::from(payload))
                                .await
                                .unwrap();

                            let mut received = 0;
                            while received < cnt {
                                if tokio::time::timeout(Duration::from_secs(10), result_sub.next())
                                    .await
                                    .ok()
                                    .flatten()
                                    .is_some()
                                {
                                    received += 1;
                                } else {
                                    break;
                                }
                            }
                            total += start.elapsed();
                        }
                        total
                    })
                })
            },
        );

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_e2e_roundtrip_latency,
    bench_dispatch_latency,
    bench_concurrent_agents,
);
criterion_main!(benches);
