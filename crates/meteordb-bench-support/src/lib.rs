//! Shared deterministic fixtures and result schema for storage benchmarks.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;

use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};

pub const CACHE_VALUE_BYTES: usize = 1024;
pub const EMBEDDING_VALUE_BYTES: usize = 6 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadFile {
    pub schema_version: u32,
    pub name: String,
    pub seed: u64,
    pub dataset: DatasetConfig,
    pub engine: EngineConfig,
    pub measurement: MeasurementConfig,
    pub workloads: Vec<WorkloadSpec>,
}

impl WorkloadFile {
    pub const SCHEMA_VERSION: u32 = 1;

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, BenchError> {
        let bytes = fs::read(path)?;
        let workload: Self = serde_json::from_slice(&bytes)?;
        workload.validate()?;
        Ok(workload)
    }

    pub fn validate(&self) -> Result<(), BenchError> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err(BenchError::Invalid(format!(
                "schema_version must be {}, got {}",
                Self::SCHEMA_VERSION,
                self.schema_version
            )));
        }
        if self.name.trim().is_empty() {
            return Err(BenchError::Invalid("name must not be empty".into()));
        }
        self.dataset.validate()?;
        self.engine.validate()?;
        self.measurement.validate()?;
        if self.workloads.is_empty() {
            return Err(BenchError::Invalid(
                "workloads must contain at least one entry".into(),
            ));
        }
        for workload in &self.workloads {
            workload.validate(&self.dataset)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetConfig {
    pub key_count: usize,
    pub key_bytes: usize,
    pub cache_value_bytes: usize,
    pub embedding_value_bytes: usize,
    pub absent_key_count: usize,
}

impl DatasetConfig {
    fn validate(&self) -> Result<(), BenchError> {
        require_positive("dataset.key_count", self.key_count)?;
        require_positive("dataset.key_bytes", self.key_bytes)?;
        if self.cache_value_bytes != CACHE_VALUE_BYTES {
            return Err(BenchError::Invalid(format!(
                "dataset.cache_value_bytes must be {CACHE_VALUE_BYTES} (1024 bytes)"
            )));
        }
        if self.embedding_value_bytes != EMBEDDING_VALUE_BYTES {
            return Err(BenchError::Invalid(format!(
                "dataset.embedding_value_bytes must be {EMBEDDING_VALUE_BYTES} (6144 bytes)"
            )));
        }
        require_positive("dataset.absent_key_count", self.absent_key_count)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityConfig {
    Sync,
    Buffered,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionConfig {
    None,
    Snappy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    pub durability: DurabilityConfig,
    pub compression: CompressionConfig,
    pub cache_bytes: usize,
    pub threads: usize,
    pub write_buffer_bytes: usize,
    pub target_file_bytes: usize,
}

impl EngineConfig {
    fn validate(&self) -> Result<(), BenchError> {
        require_positive("engine.cache_bytes", self.cache_bytes)?;
        require_positive("engine.threads", self.threads)?;
        require_positive("engine.write_buffer_bytes", self.write_buffer_bytes)?;
        require_positive("engine.target_file_bytes", self.target_file_bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementConfig {
    pub warmup_operations: usize,
    pub measured_operations: usize,
    pub sample_interval_operations: usize,
}

impl MeasurementConfig {
    fn validate(&self) -> Result<(), BenchError> {
        require_positive("measurement.warmup_operations", self.warmup_operations)?;
        require_positive("measurement.measured_operations", self.measured_operations)?;
        require_positive(
            "measurement.sample_interval_operations",
            self.sample_interval_operations,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccessDistribution {
    Uniform,
    Zipfian { theta: f64 },
}

impl AccessDistribution {
    fn validate(&self) -> Result<(), BenchError> {
        match self {
            Self::Uniform => Ok(()),
            Self::Zipfian { theta } if theta.is_finite() && *theta > 0.0 && *theta < 1.0 => Ok(()),
            Self::Zipfian { theta } => Err(BenchError::Invalid(format!(
                "zipfian theta must be finite and in (0, 1), got {theta}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationMix {
    pub reads: u32,
    pub writes: u32,
    pub deletes: u32,
    pub scans: u32,
}

impl OperationMix {
    pub fn total(&self) -> u32 {
        self.reads
            .saturating_add(self.writes)
            .saturating_add(self.deletes)
            .saturating_add(self.scans)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    PointReadWrite,
    PrefixScan,
    RangeScan,
    InferenceCache,
    FeatureStore,
    EmbeddingStorage,
    Compaction,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    pub name: String,
    pub kind: WorkloadKind,
    pub distribution: AccessDistribution,
    pub operation_mix: OperationMix,
    pub batch_size: usize,
    pub prefix_bytes: usize,
    pub scan_length: usize,
    pub ttl_every_writes: usize,
    pub ttl_ms: u64,
}

impl WorkloadSpec {
    fn validate(&self, dataset: &DatasetConfig) -> Result<(), BenchError> {
        if self.name.trim().is_empty() {
            return Err(BenchError::Invalid(
                "workload.name must not be empty".into(),
            ));
        }
        self.distribution.validate()?;
        if self.operation_mix.total() == 0 {
            return Err(BenchError::Invalid(format!(
                "workload {} operation_mix must be nonzero",
                self.name
            )));
        }
        require_positive("workload.batch_size", self.batch_size)?;
        if self.prefix_bytes > dataset.key_bytes {
            return Err(BenchError::Invalid(format!(
                "workload {} prefix_bytes exceeds dataset.key_bytes",
                self.name
            )));
        }
        if matches!(
            self.kind,
            WorkloadKind::PrefixScan | WorkloadKind::RangeScan | WorkloadKind::FeatureStore
        ) {
            require_positive("workload.scan_length", self.scan_length)?;
        }
        if self.ttl_every_writes > 0 && self.ttl_ms == 0 {
            return Err(BenchError::Invalid(format!(
                "workload {} ttl_ms must be nonzero when TTL churn is enabled",
                self.name
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dataset {
    pub seed: u64,
    pub keys: Vec<Vec<u8>>,
    pub absent_keys: Vec<Vec<u8>>,
    pub cache_values: Vec<Vec<u8>>,
    pub embedding_values: Vec<Vec<u8>>,
}

impl Dataset {
    pub fn generate(workload: &WorkloadFile) -> Result<Self, BenchError> {
        workload.validate()?;
        let mut rng = ChaCha8Rng::seed_from_u64(workload.seed);
        let keys = generate_keys(
            &mut rng,
            workload.dataset.key_count,
            workload.dataset.key_bytes,
            0,
        );
        let absent_keys = generate_keys(
            &mut rng,
            workload.dataset.absent_key_count,
            workload.dataset.key_bytes,
            1,
        );
        let cache_values = generate_values(
            &mut rng,
            workload.dataset.key_count,
            workload.dataset.cache_value_bytes,
        );
        let embedding_values = generate_values(
            &mut rng,
            workload.dataset.key_count,
            workload.dataset.embedding_value_bytes,
        );
        Ok(Self {
            seed: workload.seed,
            keys,
            absent_keys,
            cache_values,
            embedding_values,
        })
    }

    pub fn sample_indices(
        &self,
        count: usize,
        distribution: AccessDistribution,
    ) -> Result<Vec<usize>, BenchError> {
        if self.keys.is_empty() {
            return Err(BenchError::Invalid("cannot sample an empty dataset".into()));
        }
        distribution.validate()?;
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed ^ 0x7361_6d70_6c65);
        match distribution {
            AccessDistribution::Uniform => Ok((0..count)
                .map(|_| rng.gen_range(0..self.keys.len()))
                .collect()),
            AccessDistribution::Zipfian { theta } => {
                let mut cumulative = Vec::with_capacity(self.keys.len());
                let mut sum = 0.0;
                for rank in 1..=self.keys.len() {
                    sum += 1.0 / (rank as f64).powf(theta);
                    cumulative.push(sum);
                }
                Ok((0..count)
                    .map(|_| {
                        let target = rng.gen_range(0.0..sum);
                        cumulative.partition_point(|&value| value < target)
                    })
                    .collect())
            }
        }
    }
}

fn generate_keys(rng: &mut ChaCha8Rng, count: usize, bytes: usize, namespace: u8) -> Vec<Vec<u8>> {
    (0..count)
        .map(|index| {
            let mut key = vec![0; bytes];
            rng.fill_bytes(&mut key);
            key[0] = namespace;
            if bytes > 9 {
                key[1] = u8::try_from((index / 16) % 256).expect("prefix group fits u8");
            }
            let encoded = (index as u64).to_be_bytes();
            let copied = encoded.len().min(bytes.saturating_sub(1));
            key[bytes - copied..].copy_from_slice(&encoded[encoded.len() - copied..]);
            key
        })
        .collect()
}

fn generate_values(rng: &mut ChaCha8Rng, count: usize, bytes: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|_| {
            let mut value = vec![0; bytes];
            rng.fill_bytes(&mut value);
            value
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySummary {
    pub samples: u64,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

impl LatencySummary {
    pub fn from_nanos(samples: &[u64]) -> Result<Self, BenchError> {
        if samples.is_empty() {
            return Err(BenchError::Invalid(
                "latency samples must not be empty".into(),
            ));
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Ok(Self {
            samples: sorted.len() as u64,
            min_ns: sorted[0],
            p50_ns: nearest_rank(&sorted, 50),
            p95_ns: nearest_rank(&sorted, 95),
            p99_ns: nearest_rank(&sorted, 99),
            max_ns: *sorted.last().expect("nonempty samples"),
        })
    }
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let rank = (percentile * sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentInfo {
    pub git_revision: String,
    pub os: String,
    pub architecture: String,
    pub cpu_model: String,
    pub logical_cpus: usize,
    pub memory_bytes: Option<u64>,
    pub rustc_version: String,
    pub cargo_version: String,
    pub tool_versions: BTreeMap<String, String>,
}

pub fn capture_environment(repository: impl AsRef<Path>) -> Result<EnvironmentInfo, BenchError> {
    let repository = repository.as_ref();
    let mut git_revision = command_output(
        Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(["rev-parse", "HEAD"]),
    )
    .unwrap_or_else(|| "unknown".into());
    let dirty = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| !output.stdout.is_empty());
    if dirty {
        git_revision.push_str("-dirty");
    }

    let mut tool_versions = BTreeMap::new();
    for (name, program, argument) in [
        ("git", "git", "--version"),
        ("cc", "cc", "--version"),
        ("c++", "c++", "--version"),
        ("cmake", "cmake", "--version"),
        ("pkg-config", "pkg-config", "--version"),
    ] {
        let version = command_output(Command::new(program).arg(argument))
            .unwrap_or_else(|| "unavailable".into());
        tool_versions.insert(name.into(), version);
    }

    Ok(EnvironmentInfo {
        git_revision,
        os: std::env::consts::OS.into(),
        architecture: std::env::consts::ARCH.into(),
        cpu_model: linux_field("/proc/cpuinfo", "model name").unwrap_or_else(|| "unknown".into()),
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        memory_bytes: linux_memory_bytes(),
        rustc_version: command_output(Command::new("rustc").arg("--version"))
            .unwrap_or_else(|| "unavailable".into()),
        cargo_version: command_output(Command::new("cargo").arg("--version"))
            .unwrap_or_else(|| "unavailable".into()),
        tool_versions,
    })
}

fn command_output(command: &mut Command) -> Option<String> {
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
}

fn linux_field(path: &str, field: &str) -> Option<String> {
    fs::read_to_string(path).ok()?.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == field).then(|| value.trim().to_owned())
    })
}

fn linux_memory_bytes() -> Option<u64> {
    let kib = linux_field("/proc/meminfo", "MemTotal")?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    kib.checked_mul(1024)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Amplification {
    pub read: Option<f64>,
    pub write: Option<f64>,
    pub space: Option<f64>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadResult {
    pub name: String,
    pub operations: u64,
    pub elapsed_ns: u64,
    pub throughput_ops_per_second: f64,
    pub latency: LatencySummary,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonResult {
    pub schema_version: u32,
    pub engine: String,
    pub engine_version: String,
    pub environment: EnvironmentInfo,
    pub engine_options: EngineConfig,
    pub workload: WorkloadFile,
    pub results: Vec<WorkloadResult>,
    pub peak_rss_bytes: Option<u64>,
    pub recovery_time_ns: Option<u64>,
    pub database_bytes: u64,
    pub amplification: Amplification,
    pub semantic_equivalence: Vec<String>,
    pub non_equivalence: Vec<String>,
}

impl ComparisonResult {
    pub const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    #[error("invalid benchmark configuration: {0}")]
    Invalid(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

fn require_positive(field: &str, value: usize) -> Result<(), BenchError> {
    if value == 0 {
        Err(BenchError::Invalid(format!(
            "{field} must be greater than zero"
        )))
    } else {
        Ok(())
    }
}

pub fn smoke_workload() -> WorkloadFile {
    WorkloadFile {
        schema_version: WorkloadFile::SCHEMA_VERSION,
        name: "smoke-v1".into(),
        seed: 0x4d45_5445_4f52_4442,
        dataset: DatasetConfig {
            key_count: 100,
            key_bytes: 32,
            cache_value_bytes: CACHE_VALUE_BYTES,
            embedding_value_bytes: EMBEDDING_VALUE_BYTES,
            absent_key_count: 25,
        },
        engine: EngineConfig {
            durability: DurabilityConfig::Sync,
            compression: CompressionConfig::None,
            cache_bytes: 8 * 1024 * 1024,
            threads: 1,
            write_buffer_bytes: 4 * 1024 * 1024,
            target_file_bytes: 4 * 1024 * 1024,
        },
        measurement: MeasurementConfig {
            warmup_operations: 50,
            measured_operations: 200,
            sample_interval_operations: 1,
        },
        workloads: vec![
            WorkloadSpec {
                name: "point-read-write".into(),
                kind: WorkloadKind::PointReadWrite,
                distribution: AccessDistribution::Uniform,
                operation_mix: OperationMix {
                    reads: 70,
                    writes: 20,
                    deletes: 5,
                    scans: 5,
                },
                batch_size: 8,
                prefix_bytes: 4,
                scan_length: 16,
                ttl_every_writes: 0,
                ttl_ms: 0,
            },
            WorkloadSpec {
                name: "inference-cache".into(),
                kind: WorkloadKind::InferenceCache,
                distribution: AccessDistribution::Zipfian { theta: 0.99 },
                operation_mix: OperationMix {
                    reads: 90,
                    writes: 10,
                    deletes: 0,
                    scans: 0,
                },
                batch_size: 8,
                prefix_bytes: 4,
                scan_length: 0,
                ttl_every_writes: 10,
                ttl_ms: 60_000,
            },
            WorkloadSpec {
                name: "feature-store".into(),
                kind: WorkloadKind::FeatureStore,
                distribution: AccessDistribution::Zipfian { theta: 0.90 },
                operation_mix: OperationMix {
                    reads: 75,
                    writes: 20,
                    deletes: 0,
                    scans: 5,
                },
                batch_size: 16,
                prefix_bytes: 8,
                scan_length: 20,
                ttl_every_writes: 20,
                ttl_ms: 300_000,
            },
            WorkloadSpec {
                name: "embedding-storage".into(),
                kind: WorkloadKind::EmbeddingStorage,
                distribution: AccessDistribution::Uniform,
                operation_mix: OperationMix {
                    reads: 50,
                    writes: 50,
                    deletes: 0,
                    scans: 0,
                },
                batch_size: 8,
                prefix_bytes: 4,
                scan_length: 0,
                ttl_every_writes: 0,
                ttl_ms: 0,
            },
            WorkloadSpec {
                name: "prefix-scan".into(),
                kind: WorkloadKind::PrefixScan,
                distribution: AccessDistribution::Uniform,
                operation_mix: OperationMix {
                    reads: 0,
                    writes: 0,
                    deletes: 0,
                    scans: 100,
                },
                batch_size: 1,
                prefix_bytes: 2,
                scan_length: 20,
                ttl_every_writes: 0,
                ttl_ms: 0,
            },
            WorkloadSpec {
                name: "range-scan".into(),
                kind: WorkloadKind::RangeScan,
                distribution: AccessDistribution::Uniform,
                operation_mix: OperationMix {
                    reads: 0,
                    writes: 0,
                    deletes: 0,
                    scans: 100,
                },
                batch_size: 1,
                prefix_bytes: 0,
                scan_length: 20,
                ttl_every_writes: 0,
                ttl_ms: 0,
            },
            WorkloadSpec {
                name: "compaction".into(),
                kind: WorkloadKind::Compaction,
                distribution: AccessDistribution::Uniform,
                operation_mix: OperationMix {
                    reads: 0,
                    writes: 100,
                    deletes: 0,
                    scans: 0,
                },
                batch_size: 16,
                prefix_bytes: 0,
                scan_length: 0,
                ttl_every_writes: 0,
                ttl_ms: 0,
            },
        ],
    }
}
