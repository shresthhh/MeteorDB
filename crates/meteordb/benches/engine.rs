use std::hint::black_box;
use std::ops::Bound;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use meteordb::{Durability, Engine, Options, ScanBounds, WriteBatch};
use meteordb_bench_support::{Dataset, smoke_workload};
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

fn point_operations(c: &mut Criterion) {
    let (_directory, engine, dataset) = populated_engine();
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
    }
    group.finish();
}

fn scans(c: &mut Criterion) {
    let (_directory, engine, dataset) = populated_engine();
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
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = point_operations, batch_write, scans, maintenance
}
criterion_main!(benches);
