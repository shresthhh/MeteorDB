use assert_cmd::Command;
use meteordb_bench_support::{ComparisonResult, WorkloadKind, smoke_workload};
use predicates::prelude::*;

#[test]
fn meteordb_smoke_emits_versioned_result_schema() {
    let output = Command::cargo_bin("meteordb-rocks-bench")
        .unwrap()
        .args(["--engine", "meteordb", "--smoke"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: ComparisonResult = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result.schema_version, ComparisonResult::SCHEMA_VERSION);
    assert_eq!(result.engine, "meteordb");
    assert!(!result.results.is_empty());
    assert_eq!(result.results.len(), result.workload.workloads.len());
    assert!(result.results.iter().all(|entry| {
        entry.operations == result.workload.measurement.measured_operations as u64
            && entry.latency.samples > 0
    }));
    assert!(
        result
            .workload
            .workloads
            .iter()
            .any(|entry| entry.kind == WorkloadKind::RangeScan)
    );
    assert!(!result.environment.rustc_version.is_empty());
    assert!(result.database_bytes > 0);
    assert_eq!(
        result.workload.engine.durability,
        meteordb_bench_support::DurabilityConfig::Sync
    );
}

#[test]
fn rocksdb_without_native_feature_fails_actionably() {
    Command::cargo_bin("meteordb-rocks-bench")
        .unwrap()
        .args(["--engine", "rocksdb", "--smoke"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("RocksDB support is not compiled"))
        .stderr(predicate::str::contains("--features rocksdb-engine"));
}

#[test]
fn workload_file_and_smoke_are_mutually_exclusive() {
    Command::cargo_bin("meteordb-rocks-bench")
        .unwrap()
        .args([
            "--engine",
            "meteordb",
            "--smoke",
            "--workload",
            "benchmarks/workloads/v1-smoke.json",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn invalid_workload_file_fails_before_measurement() {
    let base = std::env::current_dir()
        .unwrap()
        .join("target/benchmark-test-tmp");
    std::fs::create_dir_all(&base).unwrap();
    let directory = tempfile::tempdir_in(base).unwrap();
    let path = directory.path().join("invalid.json");
    let mut workload = smoke_workload();
    workload.schema_version = 999;
    std::fs::write(&path, serde_json::to_vec(&workload).unwrap()).unwrap();

    Command::cargo_bin("meteordb-rocks-bench")
        .unwrap()
        .args(["--engine", "meteordb", "--workload"])
        .arg(path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("schema_version"));
}
