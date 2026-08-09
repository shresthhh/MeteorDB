use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use meteordb::{Durability, Engine, Options, ScanBounds, WriteBatch as MeteorWriteBatch};
use meteordb_bench_support::{
    Amplification, ComparisonResult, CompressionConfig, CounterSnapshot, Dataset, DurabilityConfig,
    LatencySummary, WorkloadFile, WorkloadKind, WorkloadResult, WorkloadSpec, capture_environment,
    compression_equivalence, read_amplification_delta, smoke_workload, verify_compaction_sample,
};

type BenchResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum EngineChoice {
    Meteordb,
    Rocksdb,
}

#[derive(Debug, Parser)]
#[command(about = "Run a deterministic MeteorDB/RocksDB comparison workload")]
struct Args {
    #[arg(long, value_enum)]
    engine: EngineChoice,
    #[arg(long, conflicts_with = "workload")]
    smoke: bool,
    #[arg(long, value_name = "JSON", conflicts_with = "smoke")]
    workload: Option<PathBuf>,
    #[arg(long)]
    pretty: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    let args = Args::parse();
    let workload = match (args.smoke, args.workload) {
        (true, None) => smoke_workload(),
        (false, Some(path)) => WorkloadFile::from_path(path)?,
        (false, None) => {
            return Err("provide exactly one of --smoke or --workload <JSON>".into());
        }
        (true, Some(_)) => unreachable!("clap enforces conflicts"),
    };
    workload.validate()?;
    let dataset = Dataset::generate(&workload)?;

    let result = match args.engine {
        EngineChoice::Meteordb => run_meteordb(workload, dataset)?,
        EngineChoice::Rocksdb => run_rocksdb(workload, dataset)?,
    };
    let stdout = io::stdout();
    let mut output = stdout.lock();
    if args.pretty {
        serde_json::to_writer_pretty(&mut output, &result)?;
    } else {
        serde_json::to_writer(&mut output, &result)?;
    }
    output.write_all(b"\n")?;
    Ok(())
}

trait KvEngine {
    fn get(&self, key: &[u8]) -> BenchResult<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: &[u8]) -> BenchResult<()>;
    fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl_ms: u64) -> BenchResult<()>;
    fn delete(&self, key: &[u8]) -> BenchResult<()>;
    fn write_batch(&self, entries: &[(&[u8], &[u8])]) -> BenchResult<()>;
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> BenchResult<usize>;
    fn scan_range(&self, start: &[u8], end: &[u8], limit: usize) -> BenchResult<usize>;
    fn flush(&self) -> BenchResult<()>;
    fn compaction_marker(&self) -> BenchResult<Option<u64>>;
    fn compact(&self, start: &[u8], end: &[u8]) -> BenchResult<Option<bool>>;
    fn read_stats(&self) -> Option<CounterSnapshot>;
    fn close(&self) -> BenchResult<()>;
}

struct MeteorEngine {
    inner: Engine,
}

impl KvEngine for MeteorEngine {
    fn get(&self, key: &[u8]) -> BenchResult<Option<Vec<u8>>> {
        Ok(self.inner.get(key)?)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> BenchResult<()> {
        Ok(self.inner.put(key, value)?)
    }

    fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl_ms: u64) -> BenchResult<()> {
        let ttl_ms = i64::try_from(ttl_ms).map_err(|_| "TTL exceeds i64::MAX")?;
        Ok(self.inner.put_with_ttl(key, value, ttl_ms)?)
    }

    fn delete(&self, key: &[u8]) -> BenchResult<()> {
        Ok(self.inner.delete(key)?)
    }

    fn write_batch(&self, entries: &[(&[u8], &[u8])]) -> BenchResult<()> {
        let mut batch = MeteorWriteBatch::default();
        for &(key, value) in entries {
            batch.put(key, value);
        }
        Ok(self.inner.write(batch)?)
    }

    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> BenchResult<usize> {
        Ok(self
            .inner
            .scan_prefix(prefix, limit)?
            .collect::<meteordb::Result<Vec<_>>>()?
            .len())
    }

    fn scan_range(&self, start: &[u8], end: &[u8], limit: usize) -> BenchResult<usize> {
        Ok(self
            .inner
            .scan(
                ScanBounds::new(
                    Bound::Included(start.to_vec()),
                    Bound::Excluded(end.to_vec()),
                ),
                limit,
            )?
            .collect::<meteordb::Result<Vec<_>>>()?
            .len())
    }

    fn flush(&self) -> BenchResult<()> {
        Ok(self.inner.flush()?)
    }

    fn compaction_marker(&self) -> BenchResult<Option<u64>> {
        Ok(None)
    }

    fn compact(&self, _start: &[u8], _end: &[u8]) -> BenchResult<Option<bool>> {
        Ok(Some(self.inner.compact()?))
    }

    fn read_stats(&self) -> Option<CounterSnapshot> {
        let stats = self.inner.stats();
        Some(CounterSnapshot {
            point_reads: stats.point_reads,
            sstable_probes: stats.sstable_probes,
        })
    }

    fn close(&self) -> BenchResult<()> {
        Ok(self.inner.close()?)
    }
}

fn meteor_options(path: &Path, workload: &WorkloadFile) -> Options {
    let mut options = Options::new(path);
    options.durability = match workload.engine.durability {
        DurabilityConfig::Sync => Durability::Sync,
        DurabilityConfig::Buffered => Durability::Buffered,
    };
    options.block_cache_bytes = workload.engine.cache_bytes;
    options.memtable_bytes = workload.engine.write_buffer_bytes;
    options.target_sstable_bytes = workload.engine.target_file_bytes;
    options
}

struct RunSummary {
    results: Vec<WorkloadResult>,
    recovery_time_ns: u64,
    database_bytes: u64,
    read_amplification: Option<f64>,
}

fn run_meteordb(workload: WorkloadFile, dataset: Dataset) -> BenchResult<ComparisonResult> {
    if workload.engine.compression != CompressionConfig::None {
        return Err(
            "MeteorDB does not expose a runtime compression option; use engine.compression=\"none\" for an equivalent comparison"
                .into(),
        );
    }
    let root = benchmark_tempdir()?;
    let factory = |path: &Path, _spec: &WorkloadSpec| -> BenchResult<Box<dyn KvEngine>> {
        Ok(Box::new(MeteorEngine {
            inner: Engine::open(meteor_options(path, &workload))?,
        }))
    };
    let summary = execute_suite(&workload, &dataset, root.path(), factory)?;
    let environment = capture_environment(std::env::current_dir()?)?;
    let semantic_equivalence = equivalence_notes(&workload);
    let non_equivalence = engine_non_equivalence(
        &workload,
        vec![
            "MeteorDB compacts one highest-priority level; RocksDB uses synchronous manual compact_range over the fixture's explicit minimum-inclusive/maximum-exclusive key bounds, so the work selected is not exactly equivalent".into(),
            "foreground workload concurrency is one; MeteorDB background scheduling is engine-internal".into(),
        ],
    );
    Ok(ComparisonResult {
        schema_version: ComparisonResult::SCHEMA_VERSION,
        engine: "meteordb".into(),
        engine_version: env!("CARGO_PKG_VERSION").into(),
        environment,
        engine_options: workload.engine.clone(),
        workload,
        results: summary.results,
        peak_rss_bytes: peak_rss_bytes(),
        recovery_time_ns: Some(summary.recovery_time_ns),
        database_bytes: summary.database_bytes,
        amplification: Amplification {
            read: summary.read_amplification,
            write: None,
            space: None,
            notes: vec![
                "read is measured-interval SSTable-probe deltas per point-read delta; warm-up and fixture setup are excluded".into(),
                "physical bytes-written and live-data counters are unavailable; write and space amplification are null".into(),
            ],
        },
        semantic_equivalence,
        non_equivalence,
    })
}

fn execute_suite<F>(
    workload: &WorkloadFile,
    dataset: &Dataset,
    root: &Path,
    factory: F,
) -> BenchResult<RunSummary>
where
    F: Fn(&Path, &WorkloadSpec) -> BenchResult<Box<dyn KvEngine>>,
{
    let mut results = Vec::with_capacity(workload.workloads.len());
    let mut probes = 0_u64;
    let mut point_reads = 0_u64;
    for (index, spec) in workload.workloads.iter().enumerate() {
        if spec.kind == WorkloadKind::Compaction {
            results.push(execute_compaction_workload(
                workload, dataset, spec, root, index, &factory,
            )?);
            continue;
        }
        let path = root.join(format!("{index:02}-{}", spec.name));
        fs::create_dir(&path)?;
        let engine = factory(&path, spec)?;
        seed_engine(engine.as_ref(), dataset, spec)?;
        let (result, before, after) = execute_workload(engine.as_ref(), dataset, spec, workload)?;
        if let (Some(before), Some(after)) = (before, after) {
            probes =
                probes.saturating_add(after.sstable_probes.saturating_sub(before.sstable_probes));
            point_reads =
                point_reads.saturating_add(after.point_reads.saturating_sub(before.point_reads));
        }
        engine.close()?;
        results.push(result);
    }

    let recovery_path = root.join("recovery");
    fs::create_dir(&recovery_path)?;
    let recovery_spec = workload.workloads.first().expect("validated workloads");
    let engine = factory(&recovery_path, recovery_spec)?;
    seed_engine(engine.as_ref(), dataset, recovery_spec)?;
    engine.flush()?;
    engine.close()?;
    drop(engine);
    let started = Instant::now();
    let recovered = factory(&recovery_path, recovery_spec)?;
    let recovery_time_ns = nanos(started.elapsed());
    recovered.close()?;
    drop(recovered);

    Ok(RunSummary {
        results,
        recovery_time_ns,
        database_bytes: directory_bytes(root)?,
        read_amplification: read_amplification_delta(
            CounterSnapshot {
                point_reads: 0,
                sstable_probes: 0,
            },
            CounterSnapshot {
                point_reads,
                sstable_probes: probes,
            },
        ),
    })
}

fn execute_compaction_workload<F>(
    workload: &WorkloadFile,
    dataset: &Dataset,
    spec: &WorkloadSpec,
    root: &Path,
    workload_index: usize,
    factory: &F,
) -> BenchResult<WorkloadResult>
where
    F: Fn(&Path, &WorkloadSpec) -> BenchResult<Box<dyn KvEngine>>,
{
    let total = workload.measurement.warmup_operations + workload.measurement.measured_operations;
    let mut measured_elapsed_ns = 0_u64;
    let mut latencies = Vec::new();
    let (start, end) = compaction_bounds(dataset);

    for sample_index in 0..total {
        let path = root.join(format!(
            "{workload_index:02}-{}-{sample_index:04}",
            spec.name
        ));
        fs::create_dir(&path)?;
        let engine = factory(&path, spec)?;
        prepare_compaction_sample(engine.as_ref(), dataset)?;
        let before = engine.compaction_marker()?;

        let started = Instant::now();
        let direct_result = engine.compact(start, &end)?;
        let elapsed = nanos(started.elapsed());

        let after = engine.compaction_marker()?;
        let did_work = verify_compaction_sample(direct_result, before, after);
        if !did_work {
            return Err(format!("compaction sample {} performed no work", sample_index + 1).into());
        }
        engine.close()?;

        if sample_index >= workload.measurement.warmup_operations {
            measured_elapsed_ns = measured_elapsed_ns.saturating_add(elapsed);
            let measured_index = sample_index - workload.measurement.warmup_operations;
            if measured_index % workload.measurement.sample_interval_operations == 0 {
                latencies.push(elapsed);
            }
        }
    }

    let operations = workload.measurement.measured_operations as u64;
    let throughput = if measured_elapsed_ns == 0 {
        0.0
    } else {
        operations as f64 * 1_000_000_000.0 / measured_elapsed_ns as f64
    };
    Ok(WorkloadResult {
        name: spec.name.clone(),
        operations,
        elapsed_ns: measured_elapsed_ns,
        throughput_ops_per_second: throughput,
        latency: LatencySummary::from_nanos(&latencies)?,
    })
}

fn prepare_compaction_sample(engine: &dyn KvEngine, dataset: &Dataset) -> BenchResult<()> {
    for round in 0..6 {
        for (keys, values) in dataset.keys.chunks(32).zip(dataset.cache_values.chunks(32)) {
            let entries = keys
                .iter()
                .zip(values.iter().cycle().skip(round))
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            engine.write_batch(&entries)?;
        }
        engine.flush()?;
    }
    Ok(())
}

fn compaction_bounds(dataset: &Dataset) -> (&[u8], Vec<u8>) {
    let start = dataset
        .keys
        .iter()
        .map(Vec::as_slice)
        .min()
        .expect("validated non-empty dataset");
    let mut end = dataset
        .keys
        .iter()
        .max()
        .expect("validated non-empty dataset")
        .clone();
    end.push(0);
    (start, end)
}

fn seed_engine(engine: &dyn KvEngine, dataset: &Dataset, spec: &WorkloadSpec) -> BenchResult<()> {
    let values = if spec.kind == WorkloadKind::EmbeddingStorage {
        &dataset.embedding_values
    } else {
        &dataset.cache_values
    };
    for (keys, values) in dataset.keys.chunks(32).zip(values.chunks(32)) {
        let entries = keys
            .iter()
            .zip(values)
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>();
        engine.write_batch(&entries)?;
    }
    engine.flush()
}

fn execute_workload(
    engine: &dyn KvEngine,
    dataset: &Dataset,
    spec: &WorkloadSpec,
    workload: &WorkloadFile,
) -> BenchResult<(
    WorkloadResult,
    Option<CounterSnapshot>,
    Option<CounterSnapshot>,
)> {
    let total = workload.measurement.warmup_operations + workload.measurement.measured_operations;
    let indices = dataset.sample_indices(total, spec.distribution.clone())?;
    let mut writes = 0_usize;
    let mut latencies = Vec::new();
    let mut measured_elapsed_ns = 0_u64;
    let mut before_measurement = (workload.measurement.warmup_operations == 0)
        .then(|| engine.read_stats())
        .flatten();

    for (operation, &key_index) in indices.iter().enumerate() {
        let measured = operation >= workload.measurement.warmup_operations;
        if operation == workload.measurement.warmup_operations {
            before_measurement = engine.read_stats();
        }
        let started = Instant::now();
        execute_operation(engine, dataset, spec, operation, key_index, &mut writes)?;
        let elapsed = nanos(started.elapsed());
        if measured {
            measured_elapsed_ns = measured_elapsed_ns.saturating_add(elapsed);
            let measured_index = operation - workload.measurement.warmup_operations;
            if measured_index % workload.measurement.sample_interval_operations == 0 {
                latencies.push(elapsed);
            }
        }
    }

    let operations = workload.measurement.measured_operations as u64;
    let throughput = if measured_elapsed_ns == 0 {
        0.0
    } else {
        operations as f64 * 1_000_000_000.0 / measured_elapsed_ns as f64
    };
    let after_measurement = engine.read_stats();
    Ok((
        WorkloadResult {
            name: spec.name.clone(),
            operations,
            elapsed_ns: measured_elapsed_ns,
            throughput_ops_per_second: throughput,
            latency: LatencySummary::from_nanos(&latencies)?,
        },
        before_measurement,
        after_measurement,
    ))
}

fn execute_operation(
    engine: &dyn KvEngine,
    dataset: &Dataset,
    spec: &WorkloadSpec,
    operation: usize,
    key_index: usize,
    writes: &mut usize,
) -> BenchResult<()> {
    match spec.kind {
        WorkloadKind::PrefixScan => {
            let key = &dataset.keys[key_index];
            let prefix_bytes = spec.prefix_bytes.min(key.len());
            engine.scan_prefix(&key[..prefix_bytes], spec.scan_length)?;
        }
        WorkloadKind::RangeScan => {
            scan_range(engine, dataset, key_index, spec.scan_length)?;
        }
        WorkloadKind::Compaction => unreachable!("compaction uses fresh prepared samples"),
        _ => {
            let selector = ((operation as u64 * 37) % u64::from(spec.operation_mix.total())) as u32;
            let read_end = spec.operation_mix.reads;
            let write_end = read_end + spec.operation_mix.writes;
            let delete_end = write_end + spec.operation_mix.deletes;
            if selector < read_end {
                if spec.kind == WorkloadKind::EmbeddingStorage {
                    for (key, _) in batch_entries(dataset, key_index, spec.batch_size, true) {
                        let _ = engine.get(key)?;
                    }
                } else {
                    let key = if operation.is_multiple_of(10) {
                        &dataset.absent_keys[key_index % dataset.absent_keys.len()]
                    } else {
                        &dataset.keys[key_index]
                    };
                    let _ = engine.get(key)?;
                }
            } else if selector < write_end {
                *writes += 1;
                if spec.kind == WorkloadKind::EmbeddingStorage {
                    let entries = batch_entries(dataset, key_index, spec.batch_size, true);
                    engine.write_batch(&entries)?;
                } else {
                    let key = &dataset.keys[key_index];
                    let value = &dataset.cache_values[key_index];
                    if spec.ttl_every_writes > 0 && writes.is_multiple_of(spec.ttl_every_writes) {
                        engine.put_with_ttl(key, value, spec.ttl_ms)?;
                    } else {
                        engine.put(key, value)?;
                    }
                }
            } else if selector < delete_end {
                engine.delete(&dataset.keys[key_index])?;
            } else if spec.kind == WorkloadKind::FeatureStore {
                scan_range(engine, dataset, key_index, spec.scan_length)?;
            } else {
                let key = &dataset.keys[key_index];
                engine.scan_prefix(&key[..spec.prefix_bytes.min(key.len())], spec.scan_length)?;
            }
        }
    }
    Ok(())
}

fn batch_entries(
    dataset: &Dataset,
    start: usize,
    count: usize,
    embeddings: bool,
) -> Vec<(&[u8], &[u8])> {
    let values = if embeddings {
        &dataset.embedding_values
    } else {
        &dataset.cache_values
    };
    (0..count)
        .map(|offset| {
            let index = (start + offset) % dataset.keys.len();
            (dataset.keys[index].as_slice(), values[index].as_slice())
        })
        .collect()
}

fn scan_range(
    engine: &dyn KvEngine,
    dataset: &Dataset,
    start: usize,
    limit: usize,
) -> BenchResult<()> {
    let mut sorted = dataset.keys.iter().collect::<Vec<_>>();
    sorted.sort_unstable();
    let start = start % sorted.len();
    let end_index = (start + limit.max(1)).min(sorted.len());
    let end = if end_index < sorted.len() {
        sorted[end_index].as_slice()
    } else {
        dataset.absent_keys[0].as_slice()
    };
    engine.scan_range(sorted[start], end, limit)?;
    Ok(())
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn directory_bytes(path: &Path) -> io::Result<u64> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            bytes = bytes.saturating_add(directory_bytes(&entry.path())?);
        } else if metadata.is_file() {
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok(bytes)
}

fn benchmark_tempdir() -> io::Result<tempfile::TempDir> {
    let base = std::env::current_dir()?.join("target/benchmark-tmp");
    fs::create_dir_all(&base)?;
    tempfile::Builder::new()
        .prefix("meteordb-comparison-")
        .tempdir_in(base)
}

fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the provided rusage on a successful return.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized usage.
    let rss = unsafe { usage.assume_init() }.ru_maxrss;
    let rss = u64::try_from(rss).ok()?;
    if cfg!(target_os = "macos") {
        Some(rss)
    } else {
        rss.checked_mul(1024)
    }
}

fn equivalence_notes(workload: &WorkloadFile) -> Vec<String> {
    let mut notes = vec![
        "identical versioned workload JSON, seed, generated keys/values, access schedule, warm-up exclusion, and sample interval".into(),
        "sync/buffered durability, cache budget, write buffer target, and target file size are mapped explicitly".into(),
        "point, absent, prefix/range, cache, feature, embedding batch, and compaction workloads use the same logical operations".into(),
        "foreground workload concurrency is one operation stream for both engines".into(),
    ];
    notes.extend(compression_equivalence(workload.engine.compression).semantic);
    notes
}

fn engine_non_equivalence(workload: &WorkloadFile, mut notes: Vec<String>) -> Vec<String> {
    notes.extend(compression_equivalence(workload.engine.compression).non_equivalent);
    notes
}

#[cfg(not(feature = "rocksdb-engine"))]
fn run_rocksdb(workload: WorkloadFile, _dataset: Dataset) -> BenchResult<ComparisonResult> {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let dependency_check = Command::new("bash")
        .arg(repository.join("scripts/check-rocksdb-bench-deps.sh"))
        .current_dir(&repository)
        .output()?;
    if !dependency_check.status.success() {
        return Err(String::from_utf8_lossy(&dependency_check.stderr)
            .trim()
            .to_owned()
            .into());
    }

    let base = repository.join("target/benchmark-tmp");
    fs::create_dir_all(&base)?;
    let workload_file = tempfile::Builder::new()
        .prefix("rocksdb-workload-")
        .suffix(".json")
        .tempfile_in(base)?;
    serde_json::to_writer(workload_file.as_file(), &workload)?;

    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "-p",
            "meteordb-rocks-bench",
            "--features",
            "rocksdb-engine",
            "--",
            "--engine",
            "rocksdb",
            "--workload",
        ])
        .arg(workload_file.path())
        .current_dir(&repository)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "RocksDB benchmark build/run failed after dependency checks:\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[cfg(feature = "rocksdb-engine")]
mod rocks {
    use rocksdb::{
        BlockBasedOptions, DB, DBCompressionType, Direction, IteratorMode, Options as RocksOptions,
        WriteBatch, WriteOptions,
    };

    use super::*;

    const ROCKSDB_BACKGROUND_JOBS: i32 = 2;

    pub(super) struct RocksEngine {
        db: DB,
        write_options: WriteOptions,
    }

    impl KvEngine for RocksEngine {
        fn get(&self, key: &[u8]) -> BenchResult<Option<Vec<u8>>> {
            Ok(self.db.get(key)?)
        }

        fn put(&self, key: &[u8], value: &[u8]) -> BenchResult<()> {
            Ok(self.db.put_opt(key, value, &self.write_options)?)
        }

        fn put_with_ttl(&self, key: &[u8], value: &[u8], _ttl_ms: u64) -> BenchResult<()> {
            self.put(key, value)
        }

        fn delete(&self, key: &[u8]) -> BenchResult<()> {
            Ok(self.db.delete_opt(key, &self.write_options)?)
        }

        fn write_batch(&self, entries: &[(&[u8], &[u8])]) -> BenchResult<()> {
            let mut batch = WriteBatch::default();
            for &(key, value) in entries {
                batch.put(key, value);
            }
            Ok(self.db.write_opt(batch, &self.write_options)?)
        }

        fn scan_prefix(&self, prefix: &[u8], limit: usize) -> BenchResult<usize> {
            let mut count = 0;
            for entry in self.db.prefix_iterator(prefix) {
                let (key, _) = entry?;
                if !key.starts_with(prefix) || count == limit {
                    break;
                }
                count += 1;
            }
            Ok(count)
        }

        fn scan_range(&self, start: &[u8], end: &[u8], limit: usize) -> BenchResult<usize> {
            let mut count = 0;
            for entry in self
                .db
                .iterator(IteratorMode::From(start, Direction::Forward))
            {
                let (key, _) = entry?;
                if key.as_ref() >= end || count == limit {
                    break;
                }
                count += 1;
            }
            Ok(count)
        }

        fn flush(&self) -> BenchResult<()> {
            Ok(self.db.flush()?)
        }

        fn compaction_marker(&self) -> BenchResult<Option<u64>> {
            Ok(self.db.property_int_value("rocksdb.num-files-at-level0")?)
        }

        fn compact(&self, start: &[u8], end: &[u8]) -> BenchResult<Option<bool>> {
            self.db.compact_range(Some(start), Some(end));
            Ok(None)
        }

        fn read_stats(&self) -> Option<CounterSnapshot> {
            None
        }

        fn close(&self) -> BenchResult<()> {
            self.db.flush()?;
            Ok(())
        }
    }

    pub(super) fn options(
        path: &Path,
        workload: &WorkloadFile,
        manual_compaction: bool,
    ) -> BenchResult<RocksEngine> {
        let mut options = RocksOptions::default();
        options.create_if_missing(true);
        options.set_write_buffer_size(workload.engine.write_buffer_bytes);
        options.set_target_file_size_base(workload.engine.target_file_bytes as u64);
        options.set_max_background_jobs(ROCKSDB_BACKGROUND_JOBS);
        options.set_disable_auto_compactions(manual_compaction);
        options.set_compression_type(match workload.engine.compression {
            CompressionConfig::None => DBCompressionType::None,
            CompressionConfig::Snappy => DBCompressionType::Snappy,
        });
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&rocksdb::Cache::new_lru_cache(workload.engine.cache_bytes));
        options.set_block_based_table_factory(&table);
        let mut write_options = WriteOptions::default();
        write_options.set_sync(matches!(workload.engine.durability, DurabilityConfig::Sync));
        Ok(RocksEngine {
            db: DB::open(&options, path)?,
            write_options,
        })
    }
}

#[cfg(feature = "rocksdb-engine")]
fn run_rocksdb(workload: WorkloadFile, dataset: Dataset) -> BenchResult<ComparisonResult> {
    let root = benchmark_tempdir()?;
    let factory = |path: &Path, spec: &WorkloadSpec| -> BenchResult<Box<dyn KvEngine>> {
        Ok(Box::new(rocks::options(
            path,
            &workload,
            spec.kind == WorkloadKind::Compaction,
        )?))
    };
    let summary = execute_suite(&workload, &dataset, root.path(), factory)?;
    let environment = capture_environment(std::env::current_dir()?)?;
    let semantic_equivalence = equivalence_notes(&workload);
    let non_equivalence = engine_non_equivalence(
        &workload,
        vec![
            "RocksDB has no per-entry TTL in this runner; TTL-marked writes persist and expiry is not measured".into(),
            "RocksDB uses synchronous manual compact_range over the fixture's explicit minimum-inclusive/maximum-exclusive key bounds while MeteorDB compacts one highest-priority level; selection semantics are not exactly equivalent".into(),
            "RocksDB uses two background jobs independently of the one foreground workload thread; cache implementation, file format, Bloom filters, and recovery algorithms are engine-specific".into(),
        ],
    );
    Ok(ComparisonResult {
        schema_version: ComparisonResult::SCHEMA_VERSION,
        engine: "rocksdb".into(),
        engine_version: "rust-rocksdb 0.24.0".into(),
        environment,
        engine_options: workload.engine.clone(),
        workload,
        results: summary.results,
        peak_rss_bytes: peak_rss_bytes(),
        recovery_time_ns: Some(summary.recovery_time_ns),
        database_bytes: summary.database_bytes,
        amplification: Amplification {
            read: None,
            write: None,
            space: None,
            notes: vec![
                "portable rust-rocksdb counters do not expose equivalent probes or physical bytes-written; amplification fields are null".into(),
            ],
        },
        semantic_equivalence,
        non_equivalence,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;

    struct CompactionProbe {
        compacted: Mutex<bool>,
        no_op: bool,
    }

    impl KvEngine for CompactionProbe {
        fn get(&self, _key: &[u8]) -> BenchResult<Option<Vec<u8>>> {
            unreachable!()
        }

        fn put(&self, _key: &[u8], _value: &[u8]) -> BenchResult<()> {
            unreachable!()
        }

        fn put_with_ttl(&self, _key: &[u8], _value: &[u8], _ttl_ms: u64) -> BenchResult<()> {
            unreachable!()
        }

        fn delete(&self, _key: &[u8]) -> BenchResult<()> {
            unreachable!()
        }

        fn write_batch(&self, _entries: &[(&[u8], &[u8])]) -> BenchResult<()> {
            std::thread::sleep(Duration::from_millis(20));
            Ok(())
        }

        fn scan_prefix(&self, _prefix: &[u8], _limit: usize) -> BenchResult<usize> {
            unreachable!()
        }

        fn scan_range(&self, _start: &[u8], _end: &[u8], _limit: usize) -> BenchResult<usize> {
            unreachable!()
        }

        fn flush(&self) -> BenchResult<()> {
            std::thread::sleep(Duration::from_millis(20));
            Ok(())
        }

        fn compaction_marker(&self) -> BenchResult<Option<u64>> {
            Ok(None)
        }

        fn compact(&self, _start: &[u8], _end: &[u8]) -> BenchResult<Option<bool>> {
            let mut compacted = self.compacted.lock().unwrap();
            assert!(!*compacted, "a timed sample reused its database");
            *compacted = true;
            Ok(Some(!self.no_op))
        }

        fn read_stats(&self) -> Option<CounterSnapshot> {
            None
        }

        fn close(&self) -> BenchResult<()> {
            Ok(())
        }
    }

    fn compaction_only_workload() -> WorkloadFile {
        let mut workload = smoke_workload();
        workload.measurement.warmup_operations = 1;
        workload.measurement.measured_operations = 2;
        workload
            .workloads
            .retain(|spec| spec.kind == WorkloadKind::Compaction);
        workload
    }

    #[test]
    fn comparison_compaction_prepares_fresh_samples_outside_timing() {
        let workload = compaction_only_workload();
        let dataset = Dataset::generate(&workload).unwrap();
        let root = benchmark_tempdir().unwrap();
        let summary = execute_suite(&workload, &dataset, root.path(), |_, _| {
            Ok(Box::new(CompactionProbe {
                compacted: Mutex::new(false),
                no_op: false,
            }))
        })
        .unwrap();

        let result = &summary.results[0];
        assert_eq!(result.operations, 2);
        assert!(
            result.elapsed_ns < Duration::from_millis(10).as_nanos() as u64,
            "setup leaked into compaction latency: {}ns",
            result.elapsed_ns
        );
    }

    #[test]
    fn comparison_compaction_rejects_a_no_op_sample() {
        let workload = compaction_only_workload();
        let dataset = Dataset::generate(&workload).unwrap();
        let root = benchmark_tempdir().unwrap();
        let error = execute_suite(&workload, &dataset, root.path(), |_, _| {
            Ok(Box::new(CompactionProbe {
                compacted: Mutex::new(false),
                no_op: true,
            }))
        })
        .err()
        .expect("no-op compaction must fail");

        assert!(error.to_string().contains("performed no work"), "{error}");
    }
}
