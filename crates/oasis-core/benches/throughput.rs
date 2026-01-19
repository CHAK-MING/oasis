//! Throughput benchmark: measures commands/second capacity
//!
//! Enable with: OASIS_BENCH_ENABLE_NATS_IO=1

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
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
            value: "throughput-batch".to_string(),
        }),
        agent_id: Some(oasis_core::proto::AgentId {
            value: "throughput-agent".to_string(),
        }),
        command: "echo".to_string(),
        args: vec!["test".to_string()],
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
            value: "throughput-agent".to_string(),
        }),
        state: 3,
        exit_code: Some(0),
        stdout: "ok".to_string(),
        stderr: String::new(),
        duration_ms: Some(0.5),
        started_at: 0,
        finished_at: Some(0),
    };
    exec.encode_to_vec()
}

fn bench_dispatch_throughput(c: &mut Criterion) {
    if !enabled() {
        eprintln!("[throughput] Skipped. Set OASIS_BENCH_ENABLE_NATS_IO=1 to enable.");
        c.bench_function("throughput/disabled", |b| b.iter(|| black_box(0u64)));
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
            let subject = format!("oasis.bench.throughput.{}", uuid::Uuid::now_v7());
            let counter = Arc::new(AtomicU64::new(0));

            let mut sub = client.subscribe(subject.clone()).await?;
            let cnt = counter.clone();
            tokio::spawn(async move {
                while sub.next().await.is_some() {
                    cnt.fetch_add(1, Ordering::Relaxed);
                }
            });

            for _ in 0..500u32 {
                client
                    .publish(subject.clone(), Bytes::from(create_task_payload("warmup")))
                    .await?;
            }
            client.flush().await?;
            tokio::time::sleep(Duration::from_millis(200)).await;

            Ok((client, subject, counter))
        });

    let (client, subject, _counter) = match setup_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[throughput] Setup failed: {:#}", e);
            c.bench_function("throughput/dispatch(setup_failed)", |b| {
                b.iter(|| black_box(0u64))
            });
            return;
        }
    };

    let mut group = c.benchmark_group("throughput");

    for batch_size in [100, 500, 1000, 5000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("dispatch_batch", batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..size {
                            let payload = create_task_payload(&format!("t-{}", i));
                            client
                                .publish(subject.clone(), Bytes::from(payload))
                                .await
                                .unwrap();
                        }
                        client.flush().await.unwrap();
                    });
                    black_box(size)
                })
            },
        );
    }

    group.finish();
}

fn bench_roundtrip_throughput(c: &mut Criterion) {
    if !enabled() {
        c.bench_function("throughput/roundtrip_disabled", |b| {
            b.iter(|| black_box(0u64))
        });
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (nats, tls) = build_cfg();

    let setup_res: anyhow::Result<(async_nats::Client, String, String)> = rt.block_on(async {
        let client = NatsClientFactory::connect_with_config(&nats, &tls).await?;
        let task_subject = format!("oasis.bench.roundtrip.task.{}", uuid::Uuid::now_v7());
        let result_subject = format!("oasis.bench.roundtrip.result.{}", uuid::Uuid::now_v7());

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

        for _ in 0..200u32 {
            client
                .publish(
                    task_subject.clone(),
                    Bytes::from(create_task_payload("warmup")),
                )
                .await?;
        }
        client.flush().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        Ok((client, task_subject, result_subject))
    });

    let (client, task_subject, result_subject) = match setup_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[throughput] Roundtrip setup failed: {:#}", e);
            c.bench_function("throughput/roundtrip(setup_failed)", |b| {
                b.iter(|| black_box(0u64))
            });
            return;
        }
    };

    let mut group = c.benchmark_group("throughput");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    for batch_size in [100, 500, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("roundtrip_batch", batch_size),
            &batch_size,
            |b, &size| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;

                        for _ in 0..iters {
                            let mut result_sub =
                                client.subscribe(result_subject.clone()).await.unwrap();

                            let start = Instant::now();

                            for i in 0..size {
                                let payload = create_task_payload(&format!("rt-{}", i));
                                client
                                    .publish(task_subject.clone(), Bytes::from(payload))
                                    .await
                                    .unwrap();
                            }
                            client.flush().await.unwrap();

                            let mut received = 0;
                            while received < size {
                                if let Ok(Some(_)) =
                                    tokio::time::timeout(Duration::from_secs(30), result_sub.next())
                                        .await
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
    }

    group.finish();
}

fn bench_sustained_throughput(c: &mut Criterion) {
    if !enabled() {
        c.bench_function("throughput/sustained_disabled", |b| {
            b.iter(|| black_box(0u64))
        });
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (nats, tls) = build_cfg();

    eprintln!("\n[throughput] Running sustained throughput test (10 seconds)...");

    let result: anyhow::Result<(u64, Duration, u64)> = rt.block_on(async {
        let client = NatsClientFactory::connect_with_config(&nats, &tls).await?;
        let task_subject = format!("oasis.bench.sustained.task.{}", uuid::Uuid::now_v7());
        let result_subject = format!("oasis.bench.sustained.result.{}", uuid::Uuid::now_v7());

        let sent_counter = Arc::new(AtomicU64::new(0));
        let recv_counter = Arc::new(AtomicU64::new(0));

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

        let mut result_sub = client.subscribe(result_subject.clone()).await?;
        let recv_cnt = recv_counter.clone();
        tokio::spawn(async move {
            while result_sub.next().await.is_some() {
                recv_cnt.fetch_add(1, Ordering::Relaxed);
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let duration = Duration::from_secs(10);
        let start = Instant::now();
        let mut task_counter = 0u64;

        while start.elapsed() < duration {
            for _ in 0..100 {
                let payload = create_task_payload(&format!("sustained-{}", task_counter));
                client
                    .publish(task_subject.clone(), Bytes::from(payload))
                    .await?;
                task_counter += 1;
                sent_counter.fetch_add(1, Ordering::Relaxed);
            }
            client.flush().await?;
        }

        let elapsed = start.elapsed();

        tokio::time::sleep(Duration::from_millis(500)).await;
        let received = recv_counter.load(Ordering::Relaxed);

        Ok((task_counter, elapsed, received))
    });

    match result {
        Ok((sent, elapsed, received)) => {
            let send_rate = sent as f64 / elapsed.as_secs_f64();
            let recv_rate = received as f64 / elapsed.as_secs_f64();

            eprintln!("╔══════════════════════════════════════════════════════════════╗");
            eprintln!("║              Sustained Throughput Test Results               ║");
            eprintln!("╠══════════════════════════════════════════════════════════════╣");
            eprintln!(
                "║  Duration:      {:>8.2} seconds                            ║",
                elapsed.as_secs_f64()
            );
            eprintln!(
                "║  Tasks Sent:    {:>8}                                      ║",
                sent
            );
            eprintln!(
                "║  Tasks Recv:    {:>8}                                      ║",
                received
            );
            eprintln!(
                "║  Send Rate:     {:>8.0} tasks/sec                          ║",
                send_rate
            );
            eprintln!(
                "║  Recv Rate:     {:>8.0} tasks/sec                          ║",
                recv_rate
            );
            eprintln!(
                "║  Completion:    {:>8.1}%                                    ║",
                (received as f64 / sent as f64) * 100.0
            );
            eprintln!("╚══════════════════════════════════════════════════════════════╝");
        }
        Err(e) => {
            eprintln!("[throughput] Sustained test failed: {:#}", e);
        }
    }

    c.bench_function("throughput/sustained_summary", |b| {
        b.iter(|| black_box(0u64))
    });
}

criterion_group!(
    benches,
    bench_dispatch_throughput,
    bench_roundtrip_throughput,
    bench_sustained_throughput,
);
criterion_main!(benches);
