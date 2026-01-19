use criterion::{Criterion, criterion_group, criterion_main};
use oasis_core::proto::{
    AgentId, AgentInfoMsg, BatchRequestMsg, FileApplyRequestMsg, FileChunkMsg, FileConfigMsg,
    FileSpecMsg, SelectorExpression, SubmitBatchRequest,
};
use prost::Message;
use std::hint::black_box;

fn bench_submit_batch_encode_decode(c: &mut Criterion) {
    let msg = SubmitBatchRequest {
        batch_request: Some(BatchRequestMsg {
            command: "/usr/bin/true".to_string(),
            args: vec!["--version".to_string(), "--help".to_string()],
            target: Some(SelectorExpression {
                expression: "all".to_string(),
            }),
            timeout_seconds: 10,
        }),
    };

    c.bench_function("proto/submit_batch/encode", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(msg.encoded_len());
            msg.encode(&mut buf).unwrap();
            black_box(buf)
        })
    });

    let mut encoded = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut encoded).unwrap();

    c.bench_function("proto/submit_batch/decode", |b| {
        b.iter(|| {
            let decoded = SubmitBatchRequest::decode(encoded.as_slice()).unwrap();
            black_box(decoded)
        })
    });
}

fn bench_agent_info_encode_decode(c: &mut Criterion) {
    let mut info = std::collections::HashMap::new();
    info.insert("os".to_string(), "linux".to_string());
    info.insert("arch".to_string(), "x86_64".to_string());
    info.insert("role".to_string(), "web".to_string());

    let msg = AgentInfoMsg {
        agent_id: Some(AgentId {
            value: "agent-0001".to_string(),
        }),
        status: 0,
        info,
        last_heartbeat: 0,
        version: "0.1.0".to_string(),
        capabilities: vec!["exec".to_string(), "file".to_string()],
    };

    c.bench_function("proto/agent_info/encode", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(msg.encoded_len());
            msg.encode(&mut buf).unwrap();
            black_box(buf)
        })
    });

    let mut encoded = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut encoded).unwrap();

    c.bench_function("proto/agent_info/decode", |b| {
        b.iter(|| {
            let decoded = AgentInfoMsg::decode(encoded.as_slice()).unwrap();
            black_box(decoded)
        })
    });
}

fn bench_file_messages_encode_decode(c: &mut Criterion) {
    let spec = FileSpecMsg {
        source_path: "bench://file/1".to_string(),
        size: 4096,
        checksum: "deadbeef".to_string(),
        content_type: "application/octet-stream".to_string(),
        created_at: 0,
    };

    c.bench_function("proto/file_spec/encode", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(spec.encoded_len());
            spec.encode(&mut buf).unwrap();
            black_box(buf)
        })
    });

    let mut spec_encoded = Vec::with_capacity(spec.encoded_len());
    spec.encode(&mut spec_encoded).unwrap();

    c.bench_function("proto/file_spec/decode", |b| {
        b.iter(|| {
            let decoded = FileSpecMsg::decode(spec_encoded.as_slice()).unwrap();
            black_box(decoded)
        })
    });

    let chunk = FileChunkMsg {
        upload_id: "upload-1".to_string(),
        offset: 0,
        data: vec![0u8; 64 * 1024],
    };

    c.bench_function("proto/file_chunk64k/encode", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(chunk.encoded_len());
            chunk.encode(&mut buf).unwrap();
            black_box(buf)
        })
    });

    let mut chunk_encoded = Vec::with_capacity(chunk.encoded_len());
    chunk.encode(&mut chunk_encoded).unwrap();

    c.bench_function("proto/file_chunk64k/decode", |b| {
        b.iter(|| {
            let decoded = FileChunkMsg::decode(chunk_encoded.as_slice()).unwrap();
            black_box(decoded)
        })
    });

    let apply = FileApplyRequestMsg {
        config: Some(FileConfigMsg {
            source_path: "bench://file/1".to_string(),
            destination_path: "/tmp/oasis-bench".to_string(),
            revision: 1,
            owner: "root:root".to_string(),
            mode: "0644".to_string(),
            target: Some(SelectorExpression {
                expression: "all".to_string(),
            }),
        }),
    };

    c.bench_function("proto/file_apply/encode", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(apply.encoded_len());
            apply.encode(&mut buf).unwrap();
            black_box(buf)
        })
    });

    let mut apply_encoded = Vec::with_capacity(apply.encoded_len());
    apply.encode(&mut apply_encoded).unwrap();

    c.bench_function("proto/file_apply/decode", |b| {
        b.iter(|| {
            let decoded = FileApplyRequestMsg::decode(apply_encoded.as_slice()).unwrap();
            black_box(decoded)
        })
    });
}

criterion_group!(
    benches,
    bench_submit_batch_encode_decode,
    bench_agent_info_encode_decode,
    bench_file_messages_encode_decode
);
criterion_main!(benches);
