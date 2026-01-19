use anyhow::Context;
use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::StreamExt;
use oasis_core::config::{NatsConfig, TlsConfig};
use oasis_core::nats::NatsClientFactory;
use std::hint::black_box;
use std::time::Duration;

fn enabled() -> bool {
    std::env::var("OASIS_BENCH_ENABLE_NATS_IO")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn print_tls_debug(tls: &TlsConfig) {
    let cwd = std::env::current_dir().ok();
    eprintln!(
        "[nats_io] cwd={}",
        cwd.as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<unknown>".to_string())
    );
    eprintln!("[nats_io] certs_dir={}", tls.certs_dir.display());

    let ca = tls.nats_ca_path();
    let cert = tls.nats_client_cert_path();
    let key = tls.nats_client_key_path();

    let ca_abs = ca.canonicalize().unwrap_or(ca.clone());
    let cert_abs = cert.canonicalize().unwrap_or(cert.clone());
    let key_abs = key.canonicalize().unwrap_or(key.clone());

    eprintln!("[nats_io] ca={} exists={}", ca_abs.display(), ca.exists());
    eprintln!(
        "[nats_io] cert={} exists={}",
        cert_abs.display(),
        cert.exists()
    );
    eprintln!(
        "[nats_io] key={} exists={}",
        key_abs.display(),
        key.exists()
    );
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

fn bench_nats_request_reply(c: &mut Criterion) {
    if !enabled() {
        c.bench_function("nats_io/disabled", |b| b.iter(|| black_box(0u64)));
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (nats, tls) = build_cfg();

    print_tls_debug(&tls);
    eprintln!("[nats_io] nats_url={}", nats.url);

    // setup (sync once)
    let setup_res: anyhow::Result<(async_nats::Client, String)> = rt.block_on(async {
        let client = NatsClientFactory::connect_with_config(&nats, &tls)
            .await
            .context("connect to nats")?;

        let subject = format!("oasis.bench.nats.req.{}", uuid::Uuid::now_v7());

        // responder
        let mut sub = client.subscribe(subject.clone()).await?;
        let responder_client = client.clone();
        tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                if let Some(reply) = msg.reply {
                    let _ = responder_client.publish(reply, msg.payload).await;
                }
            }
        });

        // Warm up
        for _ in 0..100u32 {
            let _ = client
                .request(subject.clone(), Bytes::from_static(b"ping"))
                .await;
        }

        anyhow::Ok((client, subject))
    });

    let (client, subject) = match setup_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[nats_io] connect/setup failed: {:#}", e);
            c.bench_function("nats_io/request_reply(connect_failed)", |b| {
                b.iter(|| black_box(0u64))
            });
            return;
        }
    };

    c.bench_function("nats_io/request_reply", |b| {
        b.iter(|| {
            let payload = rt.block_on(async {
                let r = client
                    .request(subject.clone(), Bytes::from_static(b"ping"))
                    .await
                    .unwrap();
                r.payload
            });
            black_box(payload)
        })
    });
}

fn bench_jetstream_kv_put_get(c: &mut Criterion) {
    if !enabled() {
        c.bench_function("nats_io/disabled_kv", |b| b.iter(|| black_box(0u64)));
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (nats, tls) = build_cfg();

    // setup (sync once)
    let setup_res: anyhow::Result<(
        async_nats::jetstream::Context,
        String,
        async_nats::jetstream::kv::Store,
    )> = rt.block_on(async {
        let nats_client = NatsClientFactory::connect_with_config(&nats, &tls)
            .await
            .context("connect to nats")?;
        let js = async_nats::jetstream::new(nats_client.clone());

        let bucket = format!(
            "OASIS_BENCH_KV_{}",
            uuid::Uuid::now_v7().to_string().replace('-', "")
        );
        let kv = js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: bucket.clone(),
                history: 1,
                max_value_size: 1024 * 1024,
                ..Default::default()
            })
            .await
            .context("create kv")?;

        anyhow::Ok((js, bucket, kv))
    });

    let (js, bucket, kv) = match setup_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[nats_io] connect/setup failed: {:#}", e);
            c.bench_function("nats_io/jetstream_kv_put_get(connect_failed)", |b| {
                b.iter(|| black_box(0u64))
            });
            return;
        }
    };

    let key = "k";
    let value = Bytes::from(vec![0u8; 512]);

    rt.block_on(async {
        // warm up
        for _ in 0..50u32 {
            let _ = kv.put(key, value.clone()).await;
            let _ = kv.get(key).await;
        }
    });

    c.bench_function("nats_io/jetstream_kv_put_get", |b| {
        b.iter(|| {
            let got = rt.block_on(async {
                let _rev = kv.put(key, value.clone()).await.unwrap();
                kv.get(key).await.unwrap().unwrap()
            });
            black_box(got)
        })
    });

    // best-effort cleanup
    rt.block_on(async {
        let _ = js.delete_key_value(&bucket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
}

criterion_group!(
    benches,
    bench_nats_request_reply,
    bench_jetstream_kv_put_get
);
criterion_main!(benches);
