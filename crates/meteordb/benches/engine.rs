use std::hint::black_box;
use std::ops::Bound;
use std::path::Path;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use meteordb::{Durability, Engine, Options, ScanBounds, WriteBatch};
use meteordb_bench_support::{
    Amplification, ComponentBenchmarkReport, CounterSnapshot, Dataset, directory_bytes,
    measure_prepared_samples, read_amplification_delta, smoke_workload, write_component_report,
};
use tempfile::TempDir;

const REPORT_SAMPLES: usize = 32;

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
    let before = engine.stats();
    let mut latencies = Vec::with_capacity(REPORT_SAMPLES);
    for _ in 0..REPORT_SAMPLES {
        let started = Instant::now();
        operation();
        latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    let after = engine.stats();
    let read = read_amplification_delta(
        CounterSnapshot {
            point_reads: before.point_reads,
            sstable_probes: before.sstable_probes,
        },
        CounterSnapshot {
            point_reads: after.point_reads,
            sstable_probes: after.sstable_probes,
        },
    );
    let mut notes = vec![
        "read is measured SSTable probes per point read within this reporting probe".into(),
        "physical bytes-written and live-data counters are unavailable".into(),
    ];
    if read.is_none() {
        notes.push("the reporting probe performed no point reads; read is unavailable".into());
    }
    let report = ComponentBenchmarkReport::new(
        "engine",
        benchmark,
        &latencies,
        directory_bytes(path).expect("measure benchmark database"),
        Amplification {
            read,
            write: None,
            space: None,
            notes,
        },
    )
    .expect("build component report");
    emit_report(benchmark, &report);
}

fn emit_report(benchmark: &str, report: &ComponentBenchmarkReport) {
    let output = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/criterion/meteordb-sidecars/engine")
        .join(format!("{}.json", benchmark.replace('/', "-")));
    write_component_report(&output, report).expect("write component report");
    println!(
        "meteordb-component-report {}",
        serde_json::to_string(report).expect("serialize component report")
    );
}

fn sstable_count(path: &Path) -> usize {
    std::fs::read_dir(path)
        .expect("read benchmark database")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "sst")
        })
        .count()
}

fn prepare_flush_sample(dataset: &Dataset) -> (TempDir, Engine, usize) {
    let directory = benchmark_tempdir();
    let engine = Engine::open(options(directory.path())).expect("open benchmark database");
    for index in 0..32 {
        engine
            .put(&dataset.keys[index], &dataset.cache_values[index])
            .expect("prepare flush");
    }
    let before = sstable_count(directory.path());
    (directory, engine, before)
}

fn run_flush_sample(sample: (TempDir, Engine, usize)) -> (TempDir, Engine, usize) {
    let (directory, engine, before) = sample;
    engine.flush().expect("flush");
    (directory, engine, before)
}

fn flush_sample_did_work(sample: &(TempDir, Engine, usize)) -> bool {
    sstable_count(sample.0.path()) > sample.2
}

fn prepare_compaction_sample(dataset: &Dataset) -> (TempDir, Engine) {
    let directory = benchmark_tempdir();
    let mut opts = options(directory.path());
    opts.memtable_bytes = 8 * 1024;
    opts.target_sstable_bytes = 8 * 1024;
    let engine = Engine::open(opts).expect("open benchmark database");
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
    let dataset = Dataset::generate(&smoke_workload()).expect("valid benchmark fixture");
    c.bench_function("engine/flush", |b| {
        b.iter_batched(
            || prepare_flush_sample(&dataset),
            |sample| {
                let sample = run_flush_sample(sample);
                assert!(
                    flush_sample_did_work(&sample),
                    "flush benchmark sample performed no work"
                );
            },
            BatchSize::LargeInput,
        )
    });

    c.bench_function("engine/compaction", |b| {
        b.iter_batched(
            || prepare_compaction_sample(&dataset),
            |(_directory, engine)| {
                assert!(
                    black_box(engine.compact().expect("compact")),
                    "compaction benchmark sample performed no work"
                );
            },
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

    let mut flush_database_bytes = 0;
    let flush_latencies = measure_prepared_samples(
        REPORT_SAMPLES,
        || prepare_flush_sample(&dataset),
        run_flush_sample,
        |sample| {
            flush_database_bytes =
                directory_bytes(sample.0.path()).expect("measure flush report database");
            flush_sample_did_work(sample)
        },
    )
    .expect("every flush reporting sample must perform work");
    let flush_report = ComponentBenchmarkReport::new(
        "engine",
        "maintenance/flush",
        &flush_latencies,
        flush_database_bytes,
        Amplification {
            read: None,
            write: None,
            space: None,
            notes: vec![
                "every sample flushes a fresh dirty memtable prepared outside the timed interval"
                    .into(),
                "physical bytes-written and live-data counters are unavailable".into(),
            ],
        },
    )
    .expect("build flush report");
    emit_report("maintenance/flush", &flush_report);

    let mut compaction_database_bytes = 0;
    let compaction_latencies = measure_prepared_samples(
        REPORT_SAMPLES,
        || prepare_compaction_sample(&dataset),
        |(directory, engine)| {
            let did_work = black_box(engine.compact().expect("compact report"));
            (directory, did_work)
        },
        |(directory, did_work)| {
            compaction_database_bytes =
                directory_bytes(directory.path()).expect("measure compaction report database");
            *did_work
        },
    )
    .expect("every compaction reporting sample must perform work");
    let compaction_report = ComponentBenchmarkReport::new(
        "engine",
        "maintenance/compaction",
        &compaction_latencies,
        compaction_database_bytes,
        Amplification {
            read: None,
            write: None,
            space: None,
            notes: vec![
                "every sample compacts a fresh overfull level prepared outside the timed interval"
                    .into(),
                "physical bytes-written and live-data counters are unavailable".into(),
            ],
        },
    )
    .expect("build compaction report");
    emit_report("maintenance/compaction", &compaction_report);

    let (recovery_directory, recovery_engine, _dataset) = populated_engine();
    recovery_engine
        .close()
        .expect("close before recovery report");
    drop(recovery_engine);
    let recovery_path = recovery_directory.path().to_path_buf();
    let started = Instant::now();
    let recovered = Engine::open(options(&recovery_path)).expect("recover report database");
    let recovery_latency = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    recovered.close().expect("close recovery report database");
    let report = ComponentBenchmarkReport::new(
        "engine",
        "maintenance/recovery",
        &[recovery_latency],
        directory_bytes(&recovery_path).expect("measure recovery database"),
        Amplification {
            read: None,
            write: None,
            space: None,
            notes: vec![
                "the recovery probe performs no point reads; read is unavailable".into(),
                "recovery has no portable write/space amplification counters".into(),
            ],
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
