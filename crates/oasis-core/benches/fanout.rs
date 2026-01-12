use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use oasis_core::core_types::{AgentId, TaskId};
use oasis_core::proto;
use oasis_core::task_types::{TaskExecution, TaskState};

#[derive(Debug, Clone, Copy, Default)]
struct AggregatedStats {
    total: usize,
    success: usize,
    failed: usize,
    timeout: usize,
    cancelled: usize,
    terminal: usize,
}

fn aggregate_stats(executions: &[TaskExecution]) -> AggregatedStats {
    let mut s = AggregatedStats {
        total: executions.len(),
        ..Default::default()
    };

    for e in executions {
        match e.state {
            TaskState::Success => {
                s.success += 1;
                s.terminal += 1;
            }
            TaskState::Failed => {
                s.failed += 1;
                s.terminal += 1;
            }
            TaskState::Timeout => {
                s.timeout += 1;
                s.terminal += 1;
            }
            TaskState::Cancelled => {
                s.cancelled += 1;
                s.terminal += 1;
            }
            _ => {}
        }
    }

    s
}

fn build_executions(n: usize) -> Vec<TaskExecution> {
    let mut v = Vec::with_capacity(n);

    for i in 0..n {
        let task_id = TaskId::from(format!("task-{:06}", i));
        let agent_id = AgentId::from(format!("agent-{:06}", i));

        let state = match i % 10 {
            0..=6 => TaskState::Success,
            7 => TaskState::Failed,
            8 => TaskState::Timeout,
            _ => TaskState::Cancelled,
        };

        let stdout = if i % 50 == 0 {
            "ok\n".repeat(10)
        } else {
            String::new()
        };

        let stderr = if i % 200 == 0 {
            "warn\n".repeat(5)
        } else {
            String::new()
        };

        let e = TaskExecution {
            task_id,
            agent_id,
            state,
            exit_code: Some(0),
            stdout,
            stderr,
            started_at: 0,
            finished_at: Some(0),
            duration_ms: Some(1.0),
        };

        v.push(e);
    }

    v
}

fn bench_fanout_aggregate_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout/aggregate");

    for n in [100usize, 1000usize, 5000usize, 10000usize] {
        let executions = build_executions(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &executions, |b, execs| {
            b.iter(|| {
                let stats = aggregate_stats(black_box(execs));
                // 模拟常见逻辑：判断批次是否已经全部完成
                let done = stats.terminal == stats.total;
                black_box(done);
                black_box(stats)
            })
        });
    }

    group.finish();
}

fn bench_fanout_map_to_proto(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout/map_to_proto");

    for n in [100usize, 1000usize, 5000usize, 10000usize] {
        let executions = build_executions(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &executions, |b, execs| {
            b.iter(|| {
                let out: Vec<proto::TaskExecutionMsg> = execs
                    .iter()
                    .map(|e| proto::TaskExecutionMsg::from(e.clone()))
                    .collect();
                black_box(out)
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_fanout_aggregate_only,
    bench_fanout_map_to_proto
);
criterion_main!(benches);
