use std::hint::black_box;
use std::ops::Bound;
use std::path::Path;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use meteordb::{Durability, Engine, Options, ScanBounds, WriteBatch};
use meteordb_bench_support::{
    Amplification, ComponentBenchmarkReport, Dataset, directory_bytes, smoke_workload,
    write_component_report,
};
use tempfile::TempDir;

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(250))
        .measurement_time(Duration::from_millis(500))
        .without_plots()
}

fn options(path: &std::path::Path) -> Options {
    let config = smoke_workload().engine;
    let mut options = Options::new(path);
    options.durability = match config.durability {
        meteordb_bench_support::DurabilityConfig::Sync => Durability::Sync,
        meteordb_bench_support::DurabilityConfig::Buffered => Durability::Buffered,
    };
    options.memtable_bytes = config.write_buffer_bytes;
    options.target_sstable_bytes = config.target_file_bytes;
    options.block_cache_bytes = config.cache_bytes;
    options
}

fn benchmark_tempdir() -> TempDir {
    let base = std::env::current_dir()
        .expect("current directory")
        .join("target/benchmark-tmp");
    std::fs::create_dir_all(&base).expect("benchmark temporary base");
    tempfile::tempdir_in(base).expect("benchmark directory")
}

fn populated_engine() -> (TempDir, Engine, Dataset) {
    let workload = smoke_workload();
    let dataset = Dataset::generate(&workload).expect("valid benchmark fixture");
    let directory = benchmark_tempdir();
    let engine = Engine::open(options(directory.path())).expect("open benchmark database");
    let mut batch = WriteBatch::default();
    for (key, value) in dataset.keys.iter().zip(&dataset.cache_values) {
        batch.put(key, value);
    }
    engine.write(batch).expect("populate benchmark database");
    engine.flush().expect("flush benchmark database");
    (directory, engine, dataset)
}

fn report_component(path: &Path, engine: &Engine, benchmark: &str, mut operation: impl FnMut()) {
    let mut latencies = Vec::with_capacity(32);
    for _ in 0..32 {
        let started = Instant::now();
        operation();
        latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    let stats = engine.stats();
    let report = ComponentBenchmarkReport::new(
        "engine",
        benchmark,
        &latencies,
        directory_bytes(path).expect("measure benchmark database"),
        Amplification {
            read: Some(stats.read_amplification()),
            write: None,
            space: None,
            notes: vec![
                "read is measured SSTable probes per point read".into(),
                "physical bytes-written and live-data counters are unavailable".into(),
            ],
        },
    )
    .expect("build component report");
    let output = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/criterion/meteordb-sidecars/engine")
        .join(format!("{}.json", benchmark.replace('/', "-")));
    write_component_report(&output, &report).expect("write component report");
    println!(
        "meteordb-component-report {}",
        serde_json::to_string(&report).expect("serialize component report")
    );
}

fn point_operations(c: &mut Criterion) {
    let (directory, engine, dataset) = populated_engine();
    let mut group = c.benchmark_group("engine/point");
    group.throughput(Throughput::Elements(1));
    let present = &dataset.keys[42];
    let absent = &dataset.absent_keys[7];
    let value = &dataset.cache_values[42];

    group.bench_function("get-present", |b| {
        b.iter(|| black_box(engine.get(black_box(present)).expect("point get")))
    });
    group.bench_function("get-absent", |b| {
        b.iter(|| black_box(engine.get(black_box(absent)).expect("absent get")))
    });
    group.bench_function("put-1kib", |b| {
        b.iter(|| {
            engine
                .put(black_box(present), black_box(value))
                .expect("point put")
        })
    });
    group.bench_function("delete", |b| {
        b.iter(|| engine.delete(black_box(absent)).expect("point delete"))
    });
    group.finish();
    report_component(directory.path(), &engine, "point/get-present", || {
        black_box(engine.get(black_box(present)).expect("point get"));
    });
    report_component(directory.path(), &engine, "point/get-absent", || {
        black_box(engine.get(black_box(absent)).expect("absent get"));
    });
    report_component(directory.path(), &engine, "point/put-1kib", || {
        engine
            .put(black_box(present), black_box(value))
            .expect("point put");
    });
    report_component(directory.path(), &engine, "point/delete", || {
        engine.delete(black_box(absent)).expect("point delete");
    });
}

fn batch_write(c: &mut Criterion) {
    let directory = benchmark_tempdir();
    let engine = Engine::open(options(directory.path())).expect("open benchmark database");
    let dataset = Dataset::generate(&smoke_workload()).expect("valid benchmark fixture");
    let mut group = c.benchmark_group("engine/batch-write");
    for size in [1_usize, 8, 32] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut batch = WriteBatch::default();
                for index in 0..size {
                    batch.put(
                        black_box(&dataset.keys[index]),
                        black_box(&dataset.cache_values[index]),
                    );
                }
                engine.write(batch).expect("batch write");
            })
        });
        report_component(
            directory.path(),
            &engine,
            &format!("batch-write/{size}"),
            || {
                let mut batch = WriteBatch::default();
                for index in 0..size {
                    batch.put(&dataset.keys[index], &dataset.cache_values[index]);
                }
                engine.write(batch).expect("batch write report");
            },
        );
    }
    group.finish();
}

fn scans(c: &mut Criterion) {
    let (directory, engine, dataset) = populated_engine();
    let mut group = c.benchmark_group("engine/scans");
    group.throughput(Throughput::Elements(20));
    let prefix = &dataset.keys[0][..1];
    group.bench_function("prefix", |b| {
        b.iter(|| {
            let rows = engine
                .scan_prefix(black_box(prefix), 20)
                .expect("prefix scan")
                .collect::<meteordb::Result<Vec<_>>>()
                .expect("consume prefix scan");
            black_box(rows);
        })
    });
    let start = dataset.keys[10].clone();
    let end = dataset.keys[80].clone();
    group.bench_function("range", |b| {
        b.iter(|| {
            let rows = engine
                .scan(
                    ScanBounds::new(
                        Bound::Included(black_box(start.clone())),
                        Bound::Excluded(black_box(end.clone())),
                    ),
                    20,
                )
                .expect("range scan")
                .collect::<meteordb::Result<Vec<_>>>()
                .expect("consume range scan");
            black_box(rows);
        })
    });
    group.finish();
    report_component(directory.path(), &engine, "scans/prefix", || {
        black_box(
            engine
                .scan_prefix(prefix, 20)
                .expect("prefix scan")
                .collect::<meteordb::Result<Vec<_>>>()
                .expect("consume prefix scan"),
        );
    });
    report_component(directory.path(), &engine, "scans/range", || {
        black_box(
            engine
                .scan(
                    ScanBounds::new(Bound::Included(start.clone()), Bound::Excluded(end.clone())),
                    20,
                )
                .expect("range scan")
                .collect::<meteordb::Result<Vec<_>>>()
                .expect("consume range scan"),
        );
    });
}

fn maintenance(c: &mut Criterion) {
    c.bench_function("engine/flush", |b| {
        b.iter_batched(
            || {
                let directory = benchmark_tempdir();
                let engine =
                    Engine::open(options(directory.path())).expect("open benchmark database");
                let dataset =
                    Dataset::generate(&smoke_workload()).expect("valid benchmark fixture");
                for index in 0..32 {
                    engine
                        .put(&dataset.keys[index], &dataset.cache_values[index])
                        .expect("prepare flush");
                }
                (directory, engine)
            },
            |(_directory, engine)| engine.flush().expect("flush"),
            BatchSize::LargeInput,
        )
    });

    c.bench_function("engine/compaction", |b| {
        b.iter_batched(
            || {
                let directory = benchmark_tempdir();
                let mut opts = options(directory.path());
                opts.memtable_bytes = 8 * 1024;
                opts.target_sstable_bytes = 8 * 1024;
                let engine = Engine::open(opts).expect("open benchmark database");
                let dataset =
                    Dataset::generate(&smoke_workload()).expect("valid benchmark fixture");
                for round in 0..6 {
                    for index in 0..32 {
                        engine
                            .put(
                                &dataset.keys[index],
                                &dataset.cache_values[(index + round) % 100],
                            )
                            .expect("prepare compaction");
                    }
                    engine.flush().expect("prepare compaction flush");
                }
                (directory, engine)
            },
            |(_directory, engine)| black_box(engine.compact().expect("compact")),
            BatchSize::LargeInput,
        )
    });

    c.bench_function("engine/recovery", |b| {
        b.iter_batched(
            || {
                let (directory, engine, _dataset) = populated_engine();
                engine.close().expect("close before recovery");
                directory
            },
            |directory| {
                let engine = Engine::open(options(directory.path())).expect("recover database");
                black_box(engine.stats());
                engine.close().expect("close recovered database");
            },
            BatchSize::LargeInput,
        )
    });

    let flush_directory = benchmark_tempdir();
    let flush_engine =
        Engine::open(options(flush_directory.path())).expect("open flush report database");
    let flush_dataset = Dataset::generate(&smoke_workload()).expect("valid benchmark fixture");
    for index in 0..32 {
        flush_engine
            .put(
                &flush_dataset.keys[index],
                &flush_dataset.cache_values[index],
            )
            .expect("prepare flush report");
    }
    report_component(
        flush_directory.path(),
        &flush_engine,
        "maintenance/flush",
        || flush_engine.flush().expect("flush report"),
    );

    let compaction_directory = benchmark_tempdir();
    let mut opts = options(compaction_directory.path());
    opts.memtable_bytes = 8 * 1024;
    opts.target_sstable_bytes = 8 * 1024;
    let compaction_engine = Engine::open(opts).expect("open compaction report database");
    for round in 0..6 {
        for index in 0..32 {
            compaction_engine
                .put(
                    &flush_dataset.keys[index],
                    &flush_dataset.cache_values[(index + round) % 100],
                )
                .expect("prepare compaction report");
        }
        compaction_engine
            .flush()
            .expect("prepare compaction report");
    }
    report_component(
        compaction_directory.path(),
        &compaction_engine,
        "maintenance/compaction",
        || {
            black_box(compaction_engine.compact().expect("compact report"));
        },
    );

    let (recovery_directory, recovery_engine, _dataset) = populated_engine();
    recovery_engine
        .close()
        .expect("close before recovery report");
    drop(recovery_engine);
    let recovery_path = recovery_directory.path().to_path_buf();
    let started = Instant::now();
    let recovered = Engine::open(options(&recovery_path)).expect("recover report database");
    let recovery_latency = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let report = ComponentBenchmarkReport::new(
        "engine",
        "maintenance/recovery",
        &[recovery_latency],
        directory_bytes(&recovery_path).expect("measure recovery database"),
        Amplification {
            read: Some(recovered.stats().read_amplification()),
            write: None,
            space: None,
            notes: vec!["recovery has no portable write/space amplification counters".into()],
        },
    )
    .expect("build recovery report");
    let output = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/criterion/meteordb-sidecars/engine/maintenance-recovery.json");
    write_component_report(output, &report).expect("write recovery report");
    println!(
        "meteordb-component-report {}",
        serde_json::to_string(&report).expect("serialize recovery report")
    );
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = point_operations, batch_write, scans, maintenance
}
criterion_main!(benches);
