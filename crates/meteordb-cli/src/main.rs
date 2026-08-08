use std::ffi::OsStr;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use meteordb::{
    CacheLookup, Durability, Engine, Error, InferenceCache, InferenceEntry, InferenceKey,
    ManifestInspection, ManifestInspectionOptions, Options, SstableInspection, StatsSnapshot,
    TableReader, WalInspection, inspect_manifest_with_options, inspect_wal,
};
use serde::Serialize;

const DEFAULT_MAX_OUTPUT_ITEMS: usize = 1_000;

#[derive(Debug, Parser)]
#[command(
    name = "meteordb",
    version,
    about = "Read-only MeteorDB inspection and benchmarks"
)]
struct Cli {
    /// Database directory. Inspection commands never recover, truncate, or take its writer lock.
    #[arg(long, value_name = "DIRECTORY")]
    path: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate CURRENT, manifest edits, referenced SSTables, and required WALs.
    Check {
        /// Output encoding.
        #[arg(long, value_enum, default_value_t)]
        format: OutputFormat,
        /// Trusted maximum logical WAL batch payload.
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        max_batch_bytes: usize,
        /// Maximum live SSTable and required WAL files inspected.
        #[arg(long, default_value_t = 100_000, value_parser = parse_positive_usize)]
        max_files: usize,
        /// Maximum raw manifest key bytes retained while selecting live files.
        #[arg(long, default_value_t = 16 * 1024 * 1024, value_parser = parse_positive_usize)]
        max_bytes: usize,
    },
    /// Render checked manifest edits and the resulting level layout.
    DumpManifest {
        /// Output encoding.
        #[arg(long, value_enum, default_value_t)]
        format: OutputFormat,
        /// Maximum edits rendered; all edits are still validated.
        #[arg(long, default_value_t = DEFAULT_MAX_OUTPUT_ITEMS, value_parser = parse_positive_usize)]
        max_edits: usize,
        /// Maximum SSTable references rendered across edits and live levels.
        #[arg(long, default_value_t = DEFAULT_MAX_OUTPUT_ITEMS, value_parser = parse_positive_usize)]
        max_files: usize,
        /// Maximum raw key bytes retained in rendered SSTable metadata.
        #[arg(long, default_value_t = 1024 * 1024, value_parser = parse_positive_usize)]
        max_bytes: usize,
    },
    /// Render checked SSTable metadata, blocks, key ranges, and bounded entries.
    DumpSstable {
        /// Canonical SSTable filename relative to the database directory.
        #[arg(long, value_name = "NNNNNN.sst")]
        file: PathBuf,
        /// Output encoding.
        #[arg(long, value_enum, default_value_t)]
        format: OutputFormat,
        /// Maximum records rendered; every block and record is still checked.
        #[arg(long, default_value_t = 100, value_parser = parse_positive_usize)]
        max_entries: usize,
        /// Maximum block locations rendered; every block is still checked.
        #[arg(long, default_value_t = 100, value_parser = parse_positive_usize)]
        max_blocks: usize,
        /// Maximum raw key bytes retained for rendered entry samples.
        #[arg(long, default_value_t = 1024 * 1024, value_parser = parse_positive_usize)]
        max_bytes: usize,
    },
    /// Run a reproducible workload benchmark against the database path.
    Bench {
        /// Benchmark duration in seconds.
        #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
        seconds: u64,
        /// Seed controlling the operation sequence.
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Number of pre-populated inference-cache entries.
        #[arg(long, default_value_t = 1_000)]
        dataset_size: usize,
        /// Workload adapter to exercise.
        #[arg(long, value_enum)]
        workload: Workload,
        /// Output encoding.
        #[arg(long, value_enum, default_value_t)]
        format: OutputFormat,
        /// Write durability used by the benchmark (default: sync).
        #[arg(long, value_enum, default_value_t)]
        durability: BenchDurability,
    },
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Workload {
    InferenceCache,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum BenchDurability {
    #[default]
    Sync,
    Buffered,
}

impl BenchDurability {
    fn name(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Buffered => "buffered",
        }
    }

    fn engine(self) -> Durability {
        match self {
            Self::Sync => Durability::Sync,
            Self::Buffered => Durability::Buffered,
        }
    }
}

#[derive(Serialize)]
struct CheckOutput {
    format_version: u32,
    status: &'static str,
    manifest: String,
    manifest_edits: usize,
    sstables: usize,
    wal_segments: usize,
    wal_batches: usize,
    wal_operations: usize,
}

#[derive(Serialize)]
struct ManifestOutput<'a> {
    format_version: u32,
    manifest: &'a str,
    file_bytes: u64,
    edits: &'a [meteordb::ManifestEditInspection],
    edits_total: usize,
    edits_truncated: bool,
    levels: &'a [Vec<meteordb::ManifestFileInspection>],
    files_total: usize,
    level_file_counts: &'a [usize],
    files_truncated: bool,
    next_file_number: u64,
    last_sequence: u64,
    log_number: u64,
    active_log_number: u64,
    wal_sequence: u64,
}

#[derive(Serialize)]
struct BenchOutput {
    format_version: u32,
    workload: &'static str,
    durability: &'static str,
    seconds: u64,
    seed: u64,
    dataset_size: usize,
    operations: u64,
    throughput_ops_per_second: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    stats: StatsSnapshot,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            error.exit_code()
        }
    }
}

fn run(cli: Cli) -> Result<(), CliError> {
    match cli.command {
        Command::Check {
            format,
            max_batch_bytes,
            max_files,
            max_bytes,
        } => check(&cli.path, max_batch_bytes, max_files, max_bytes, format),
        Command::DumpManifest {
            format,
            max_edits,
            max_files,
            max_bytes,
        } => dump_manifest(&cli.path, max_edits, max_files, max_bytes, format),
        Command::DumpSstable {
            file,
            format,
            max_entries,
            max_blocks,
            max_bytes,
        } => dump_sstable(&cli.path, &file, max_entries, max_blocks, max_bytes, format),
        Command::Bench {
            seconds,
            seed,
            dataset_size,
            workload,
            format,
            durability,
        } => bench(
            &cli.path,
            seconds,
            seed,
            dataset_size,
            workload,
            durability,
            format,
        ),
    }
}

fn check(
    path: &Path,
    max_batch_bytes: usize,
    max_files: usize,
    max_bytes: usize,
    format: OutputFormat,
) -> Result<(), CliError> {
    if max_batch_bytes == 0 {
        return Err(CliError::message(
            "max_batch_bytes must be greater than zero",
        ));
    }
    let manifest = inspect_manifest_with_options(
        path,
        ManifestInspectionOptions {
            max_edits: 1,
            max_files,
            max_bytes,
        },
    )?;
    if manifest.files_truncated {
        return Err(CliError::message(format!(
            "live SSTable count {} exceeds max_files {max_files}",
            manifest.files_total
        )));
    }
    let mut sstables = 0;
    for file in manifest.levels.iter().flatten() {
        let table_path = path.join(format!("{:06}.sst", file.number));
        let inspected = TableReader::open(&table_path)?.inspect(0, 0)?;
        if inspected.file_bytes != file.file_size {
            return Err(Error::Corruption {
                context: "SSTable",
                detail: format!(
                    "{} has length {}, expected {}",
                    table_path.display(),
                    inspected.file_bytes,
                    file.file_size
                ),
            }
            .into());
        }
        if inspected.file_number != file.number {
            return Err(Error::Corruption {
                context: "SSTable properties",
                detail: format!(
                    "{} records file number {}, expected {}",
                    table_path.display(),
                    inspected.file_number,
                    file.number
                ),
            }
            .into());
        }
        sstables += 1;
    }
    let remaining_files = max_files - manifest.files_total;
    let wals = inspect_required_wals(path, &manifest, max_batch_bytes, remaining_files)?;
    let output = CheckOutput {
        format_version: 1,
        status: "ok",
        manifest: manifest.manifest,
        manifest_edits: manifest.edits.len(),
        sstables,
        wal_segments: wals.len(),
        wal_batches: wals.iter().map(|wal| wal.batches).sum(),
        wal_operations: wals.iter().map(|wal| wal.operations).sum(),
    };
    match format {
        OutputFormat::Human => {
            println!("status: {}", output.status);
            println!("manifest: {}", output.manifest);
            println!("manifest_edits: {}", output.manifest_edits);
            println!("SSTables: {}", output.sstables);
            println!("WAL_segments: {}", output.wal_segments);
            println!("WAL_batches: {}", output.wal_batches);
            println!("WAL_operations: {}", output.wal_operations);
            Ok(())
        }
        OutputFormat::Json => write_json(&output),
    }
}

fn inspect_required_wals(
    path: &Path,
    manifest: &ManifestInspection,
    max_batch_bytes: usize,
    max_files: usize,
) -> Result<Vec<WalInspection>, CliError> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(path).map_err(|source| Error::Io {
        operation: "read database directory",
        path: path.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| Error::Io {
            operation: "read database directory entry",
            path: path.to_path_buf(),
            source,
        })?;
        if let Some(number) = parse_numbered_name(&entry.file_name(), ".wal") {
            if paths.len() == max_files {
                return Err(CliError::message(format!(
                    "required WAL count exceeds remaining max_files {max_files}"
                )));
            }
            paths.push((number, entry.path()));
        }
    }
    paths.sort_by_key(|(number, _)| *number);

    let selected = if manifest.log_number == 0 && manifest.active_log_number == 0 {
        paths
    } else {
        for required in [manifest.log_number, manifest.active_log_number] {
            if !paths.iter().any(|(number, _)| *number == required) {
                return Err(Error::Corruption {
                    context: "WAL",
                    detail: format!("missing required WAL {required:06}.wal"),
                }
                .into());
            }
        }
        paths
            .into_iter()
            .filter(|(number, _)| {
                *number >= manifest.log_number && *number <= manifest.active_log_number
            })
            .collect()
    };

    let mut inspections = Vec::with_capacity(selected.len());
    for (_, wal_path) in selected {
        inspections.push(inspect_wal(wal_path, max_batch_bytes)?);
    }
    let legacy_wal_metadata = manifest.log_number == 0 && manifest.active_log_number == 0;
    let mut last_sequence = manifest.last_sequence;
    let mut expected_sequence = manifest
        .last_sequence
        .checked_add(1)
        .ok_or_else(|| CliError::message("sequence number space is exhausted"))?;
    for wal in &inspections {
        let Some(mut first) = wal.first_sequence else {
            continue;
        };
        if legacy_wal_metadata
            && wal
                .last_sequence
                .is_some_and(|last| last <= manifest.last_sequence)
        {
            continue;
        }
        if legacy_wal_metadata {
            first = first.max(expected_sequence);
        }
        if first != expected_sequence {
            return Err(Error::Corruption {
                context: "WAL",
                detail: format!("expected sequence {expected_sequence}, found {first}"),
            }
            .into());
        }
        if let Some(last) = wal.last_sequence {
            last_sequence = last;
            expected_sequence = last_sequence
                .checked_add(1)
                .ok_or_else(|| CliError::message("sequence number space is exhausted"))?;
        }
    }
    if last_sequence < manifest.wal_sequence {
        return Err(Error::Corruption {
            context: "WAL",
            detail: format!(
                "required WALs end at sequence {last_sequence}, before manifest WAL sequence {}",
                manifest.wal_sequence
            ),
        }
        .into());
    }
    Ok(inspections)
}

fn dump_manifest(
    path: &Path,
    max_edits: usize,
    max_files: usize,
    max_bytes: usize,
    format: OutputFormat,
) -> Result<(), CliError> {
    let manifest = inspect_manifest_with_options(
        path,
        ManifestInspectionOptions {
            max_edits,
            max_files,
            max_bytes,
        },
    )?;
    let shown = manifest.edits.len();
    match format {
        OutputFormat::Human => {
            println!("manifest: {}", manifest.manifest);
            println!("file_bytes: {}", manifest.file_bytes);
            println!("edits: {}", manifest.edits_total);
            println!("shown_edits: {shown}");
            println!("edits_truncated: {}", manifest.edits_truncated);
            for edit in &manifest.edits {
                println!(
                    "edit {}: added={} deleted={}",
                    edit.index, edit.added_files_total, edit.deleted_files_total
                );
            }
            for (level, files) in manifest.levels.iter().enumerate() {
                println!(
                    "level {level}: {} file(s), {} shown",
                    manifest.level_file_counts[level],
                    files.len()
                );
                for file in files {
                    println!(
                        "  {:06}.sst bytes={} smallest={} largest={}",
                        file.number, file.file_size, file.smallest_key_hex, file.largest_key_hex
                    );
                }
            }
            println!("next_file_number: {}", manifest.next_file_number);
            println!("last_sequence: {}", manifest.last_sequence);
            println!("log_number: {}", manifest.log_number);
            println!("active_log_number: {}", manifest.active_log_number);
            println!("wal_sequence: {}", manifest.wal_sequence);
            Ok(())
        }
        OutputFormat::Json => write_json(&ManifestOutput {
            format_version: manifest.format_version,
            manifest: &manifest.manifest,
            file_bytes: manifest.file_bytes,
            edits: &manifest.edits[..shown],
            edits_total: manifest.edits_total,
            edits_truncated: manifest.edits_truncated,
            levels: &manifest.levels,
            files_total: manifest.files_total,
            level_file_counts: &manifest.level_file_counts,
            files_truncated: manifest.files_truncated,
            next_file_number: manifest.next_file_number,
            last_sequence: manifest.last_sequence,
            log_number: manifest.log_number,
            active_log_number: manifest.active_log_number,
            wal_sequence: manifest.wal_sequence,
        }),
    }
}

fn dump_sstable(
    database: &Path,
    file: &Path,
    max_entries: usize,
    max_blocks: usize,
    max_bytes: usize,
    format: OutputFormat,
) -> Result<(), CliError> {
    validate_sstable_name(file)?;
    let inspection = TableReader::open(database.join(file))?.inspect_with_limits(
        max_entries,
        max_blocks,
        max_bytes,
    )?;
    match format {
        OutputFormat::Human => write_sstable_human(&inspection),
        OutputFormat::Json => write_json(&inspection),
    }
}

fn write_sstable_human(inspection: &SstableInspection) -> Result<(), CliError> {
    println!("file_number: {}", inspection.file_number);
    println!("file_bytes: {}", inspection.file_bytes);
    println!("entries: {}", inspection.entries);
    println!("data_blocks: {}", inspection.data_blocks);
    println!("compression: {}", inspection.compression);
    println!("smallest_key_hex: {}", inspection.smallest_key_hex);
    println!("largest_key_hex: {}", inspection.largest_key_hex);
    println!("max_data_block_bytes: {}", inspection.max_data_block_bytes);
    println!("checksums: ok");
    println!("shown_blocks: {}", inspection.blocks.len());
    println!("blocks_truncated: {}", inspection.blocks_truncated);
    println!("shown_entries: {}", inspection.shown.len());
    println!("shown_bytes: {}", inspection.shown_bytes);
    println!("bytes_truncated: {}", inspection.bytes_truncated);
    println!("truncated: {}", inspection.truncated);
    for block in &inspection.blocks {
        println!(
            "block {}: offset={} size={}",
            block.index, block.offset, block.size
        );
    }
    for entry in &inspection.shown {
        println!(
            "entry: key={} user_key={} sequence={} kind={} value_bytes={}",
            entry.key_hex, entry.user_key_hex, entry.sequence, entry.kind, entry.value_bytes
        );
    }
    Ok(())
}

fn validate_sstable_name(file: &Path) -> Result<(), CliError> {
    let mut components = file.components();
    let Some(Component::Normal(name)) = components.next() else {
        return Err(CliError::message(
            "--file must be a canonical SSTable filename",
        ));
    };
    if components.next().is_some() || parse_numbered_name(name, ".sst").is_none() {
        return Err(CliError::message(
            "--file must be a canonical SSTable filename such as 000042.sst",
        ));
    }
    Ok(())
}

fn bench(
    path: &Path,
    seconds: u64,
    seed: u64,
    dataset_size: usize,
    workload: Workload,
    durability: BenchDurability,
    format: OutputFormat,
) -> Result<(), CliError> {
    if dataset_size == 0 {
        return Err(CliError::message("dataset_size must be greater than zero"));
    }
    match workload {
        Workload::InferenceCache => {
            bench_inference_cache(path, seconds, seed, dataset_size, durability, format)
        }
    }
}

fn bench_inference_cache(
    path: &Path,
    seconds: u64,
    seed: u64,
    dataset_size: usize,
    durability: BenchDurability,
    format: OutputFormat,
) -> Result<(), CliError> {
    let mut options = Options::new(path);
    options.durability = durability.engine();
    let engine = Engine::open(options)?;
    let cache = InferenceCache::new(engine.clone(), b"bench")?;
    let keys = (0..dataset_size)
        .map(|index| InferenceKey::new(b"model", b"1", index.to_le_bytes()))
        .collect::<Vec<_>>();
    for (index, key) in keys.iter().enumerate() {
        cache.put(key, InferenceEntry::new(format!("result-{index:08}")), None)?;
    }

    let duration = Duration::from_secs(seconds);
    let started = Instant::now();
    let mut generator = Lcg::new(seed);
    let mut histogram = Histogram::<u64>::new(3)
        .map_err(|error| CliError::message(format!("create latency histogram: {error}")))?;
    let mut operations = 0_u64;
    while started.elapsed() < duration {
        let index = usize::try_from(generator.next()).unwrap_or(usize::MAX) % keys.len();
        let operation_started = Instant::now();
        if !matches!(cache.get(&keys[index])?, CacheLookup::Hit(_)) {
            return Err(CliError::message("seeded benchmark cache entry was absent"));
        }
        let micros = u64::try_from(operation_started.elapsed().as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        histogram
            .record(micros)
            .map_err(|error| CliError::message(format!("record latency: {error}")))?;
        operations = operations.saturating_add(1);
    }
    let elapsed = started.elapsed().as_secs_f64();
    let output = BenchOutput {
        format_version: 1,
        workload: "inference-cache",
        durability: durability.name(),
        seconds,
        seed,
        dataset_size,
        operations,
        throughput_ops_per_second: operations as f64 / elapsed,
        p50_us: histogram.value_at_quantile(0.50),
        p95_us: histogram.value_at_quantile(0.95),
        p99_us: histogram.value_at_quantile(0.99),
        stats: engine.stats(),
    };
    engine.close()?;

    match format {
        OutputFormat::Human => {
            println!("workload: {}", output.workload);
            println!("durability: {}", output.durability);
            println!("seconds: {}", output.seconds);
            println!("seed: {}", output.seed);
            println!("dataset_size: {}", output.dataset_size);
            println!("operations: {}", output.operations);
            println!(
                "throughput_ops_per_second: {:.3}",
                output.throughput_ops_per_second
            );
            println!("p50_us: {}", output.p50_us);
            println!("p95_us: {}", output.p95_us);
            println!("p99_us: {}", output.p99_us);
            println!("point_reads: {}", output.stats.point_reads);
            println!(
                "read_amplification: {:.6}",
                output.stats.read_amplification()
            );
            Ok(())
        }
        OutputFormat::Json => write_json(&output),
    }
}

fn parse_numbered_name(name: &OsStr, suffix: &str) -> Option<u64> {
    let name = name.to_str()?;
    let digits = name.strip_suffix(suffix)?;
    let number = digits.parse::<u64>().ok()?;
    (number != 0 && digits == format!("{number:06}")).then_some(number)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid positive integer: {error}"))
        .and_then(|parsed| {
            (parsed > 0)
                .then_some(parsed)
                .ok_or_else(|| "value must be greater than zero".to_owned())
        })
}

fn write_json(value: &impl Serialize) -> Result<(), CliError> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, value)
        .map_err(|error| CliError::message(format!("serialize JSON output: {error}")))?;
    output
        .write_all(b"\n")
        .map_err(|error| CliError::message(format!("write output: {error}")))
}

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

enum CliError {
    Engine(Error),
    Message(String),
}

impl CliError {
    fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    fn exit_code(&self) -> ExitCode {
        match self {
            Self::Engine(Error::Corruption { .. } | Error::UnsupportedFormat { .. }) => {
                ExitCode::from(3)
            }
            Self::Engine(_) | Self::Message(_) => ExitCode::from(1),
        }
    }
}

impl From<Error> for CliError {
    fn from(error: Error) -> Self {
        Self::Engine(error)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Engine(error) => error.fmt(formatter),
            Self::Message(message) => formatter.write_str(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BenchDurability;
    use meteordb::Durability;

    #[test]
    fn benchmark_durability_maps_to_engine_options() {
        assert_eq!(BenchDurability::Sync.engine(), Durability::Sync);
        assert_eq!(BenchDurability::Buffered.engine(), Durability::Buffered);
    }
}
