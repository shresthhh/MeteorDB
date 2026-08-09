use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use meteordb::{
    Embedding, EmbeddingKey, EmbeddingStore, Engine, FeatureKey, FeatureRecord, FeatureStore,
    FeatureValue, InferenceCache, InferenceEntry, InferenceKey, Options,
};
use meteordb_bench_support::{
    Amplification, ComponentBenchmarkReport, Dataset, directory_bytes, smoke_workload,
    write_component_report,
};

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(250))
        .measurement_time(Duration::from_millis(500))
        .without_plots()
}

fn benchmark_tempdir() -> tempfile::TempDir {
    let base = std::env::current_dir()
        .expect("current directory")
        .join("target/benchmark-tmp");
    std::fs::create_dir_all(&base).expect("benchmark temporary base");
    tempfile::tempdir_in(base).expect("benchmark directory")
}

fn engine() -> (tempfile::TempDir, Engine, Dataset) {
    let directory = benchmark_tempdir();
    let workload = smoke_workload();
    let dataset = Dataset::generate(&workload).expect("valid benchmark fixture");
    let mut options = Options::new(directory.path());
    options.memtable_bytes = workload.engine.write_buffer_bytes;
    options.target_sstable_bytes = workload.engine.target_file_bytes;
    options.block_cache_bytes = workload.engine.cache_bytes;
    let engine = Engine::open(options).expect("open benchmark database");
    (directory, engine, dataset)
}

fn report_component(path: &Path, benchmark: &str, mut operation: impl FnMut()) {
    let mut latencies = Vec::with_capacity(32);
    for _ in 0..32 {
        let started = Instant::now();
        operation();
        latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    let report = ComponentBenchmarkReport::new(
        "workloads",
        benchmark,
        &latencies,
        directory_bytes(path).expect("measure benchmark database"),
        Amplification {
            read: None,
            write: None,
            space: None,
            notes: vec![
                "AI-store wrappers do not expose per-workload physical amplification counters"
                    .into(),
            ],
        },
    )
    .expect("build component report");
    let output = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/criterion/meteordb-sidecars/workloads")
        .join(format!("{}.json", benchmark.replace('/', "-")));
    write_component_report(&output, &report).expect("write component report");
    println!(
        "meteordb-component-report {}",
        serde_json::to_string(&report).expect("serialize component report")
    );
}

fn inference_cache(c: &mut Criterion) {
    let (directory, engine, dataset) = engine();
    let cache = InferenceCache::new(engine, b"criterion").expect("create cache");
    let keys = (0..dataset.keys.len())
        .map(|index| InferenceKey::new(b"model", b"v1", &dataset.keys[index]))
        .collect::<Vec<_>>();
    for (key, value) in keys.iter().zip(&dataset.cache_values) {
        cache
            .put(key, InferenceEntry::new(value), Some(60_000))
            .expect("seed cache");
    }
    let mut group = c.benchmark_group("workloads/inference-cache");
    group.throughput(Throughput::Bytes(1024));
    group.bench_function("zipfian-hit-1kib", |b| {
        let indices = dataset
            .sample_indices(
                10_000,
                meteordb_bench_support::AccessDistribution::Zipfian { theta: 0.99 },
            )
            .expect("sample accesses");
        let mut cursor = 0;
        b.iter(|| {
            let result = cache
                .get(&keys[indices[cursor % indices.len()]])
                .expect("cache get");
            cursor += 1;
            black_box(result);
        })
    });
    group.bench_function("put-with-ttl-1kib", |b| {
        b.iter(|| {
            cache
                .put(
                    black_box(&keys[0]),
                    InferenceEntry::new(black_box(&dataset.cache_values[0])),
                    Some(60_000),
                )
                .expect("cache put")
        })
    });
    group.finish();
    let report_indices = dataset
        .sample_indices(
            32,
            meteordb_bench_support::AccessDistribution::Zipfian { theta: 0.99 },
        )
        .expect("sample report accesses");
    let mut report_cursor = 0;
    report_component(directory.path(), "inference-cache/zipfian-hit-1kib", || {
        black_box(
            cache
                .get(&keys[report_indices[report_cursor % report_indices.len()]])
                .expect("cache report get"),
        );
        report_cursor += 1;
    });
    report_component(
        directory.path(),
        "inference-cache/put-with-ttl-1kib",
        || {
            cache
                .put(
                    &keys[0],
                    InferenceEntry::new(&dataset.cache_values[0]),
                    Some(60_000),
                )
                .expect("cache report put");
        },
    );
}

fn feature_store(c: &mut Criterion) {
    let (directory, engine, _dataset) = engine();
    let store = FeatureStore::new(engine, b"criterion").expect("create feature store");
    let keys = (0..100)
        .map(|index| FeatureKey::new(b"user", b"42", b"ranking", index as i64))
        .collect::<Vec<_>>();
    let mut record = FeatureRecord::new();
    record
        .insert(b"score", FeatureValue::F64(0.75))
        .expect("feature")
        .insert(b"active", FeatureValue::Bool(true))
        .expect("feature");
    for key in &keys {
        store
            .put(key, &record, Some(300_000))
            .expect("seed feature");
    }
    let mut group = c.benchmark_group("workloads/feature-store");
    group.throughput(Throughput::Elements(1));
    group.bench_function("point-get", |b| {
        b.iter(|| black_box(store.get(black_box(&keys[42])).expect("feature get")))
    });
    group.throughput(Throughput::Elements(20));
    group.bench_function("history-scan", |b| {
        b.iter(|| {
            black_box(
                store
                    .history(b"user", b"42", b"ranking", i64::MIN..=i64::MAX, 20)
                    .expect("feature history"),
            )
        })
    });
    group.finish();
    report_component(directory.path(), "feature-store/point-get", || {
        black_box(store.get(&keys[42]).expect("feature report get"));
    });
    report_component(directory.path(), "feature-store/history-scan", || {
        black_box(
            store
                .history(b"user", b"42", b"ranking", i64::MIN..=i64::MAX, 20)
                .expect("feature report history"),
        );
    });
}

fn embedding_storage(c: &mut Criterion) {
    let (directory, engine, dataset) = engine();
    let store = EmbeddingStore::new(engine, b"criterion").expect("create embedding store");
    let keys = (0..dataset.keys.len())
        .map(|index| EmbeddingKey::new(&dataset.keys[index]).with_model(b"encoder", b"v1"))
        .collect::<Vec<_>>();
    let embeddings = dataset
        .embedding_values
        .iter()
        .map(|bytes| {
            let values = bytes
                .chunks_exact(4)
                .map(|chunk| f32::from(chunk[0]) / 255.0)
                .collect::<Vec<_>>();
            let mut embedding = Embedding::from_f32(&values).expect("embedding");
            embedding.set_model(b"encoder", b"v1");
            embedding
        })
        .collect::<Vec<_>>();
    store
        .put_many(keys.iter().zip(&embeddings))
        .expect("seed embeddings");
    let mut group = c.benchmark_group("workloads/embedding-storage");
    group.throughput(Throughput::Bytes((8 * 6 * 1024) as u64));
    group.bench_function("batch-get-8x6kib", |b| {
        b.iter(|| {
            black_box(
                store
                    .get_many(black_box(keys[..8].iter()))
                    .expect("embedding batch get"),
            )
        })
    });
    group.bench_function("batch-put-8x6kib", |b| {
        b.iter(|| {
            store
                .put_many(black_box(keys[..8].iter().zip(&embeddings[..8])))
                .expect("embedding batch put")
        })
    });
    group.finish();
    report_component(
        directory.path(),
        "embedding-storage/batch-get-8x6kib",
        || {
            black_box(
                store
                    .get_many(keys[..8].iter())
                    .expect("embedding report get"),
            );
        },
    );
    report_component(
        directory.path(),
        "embedding-storage/batch-put-8x6kib",
        || {
            store
                .put_many(keys[..8].iter().zip(&embeddings[..8]))
                .expect("embedding report put");
        },
    );
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = inference_cache, feature_store, embedding_storage
}
criterion_main!(benches);
