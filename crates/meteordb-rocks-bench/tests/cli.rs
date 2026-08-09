use assert_cmd::Command;
use meteordb_bench_support::{ComparisonResult, CompressionConfig, WorkloadKind, smoke_workload};
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
    assert!(
        result
            .semantic_equivalence
            .iter()
            .any(|note| note.contains("both engines are configured with no compression"))
    );
    assert!(result.non_equivalence.iter().any(|note| {
        note.contains("explicit minimum-inclusive/maximum-exclusive key bounds")
            && note.contains("not exactly equivalent")
    }));
}

#[test]
fn rocksdb_exact_smoke_command_auto_enables_native_implementation_or_fails_on_prerequisites() {
    let base = std::env::current_dir()
        .unwrap()
        .join("target/benchmark-test-tmp");
    std::fs::create_dir_all(&base).unwrap();
    let tools = tempfile::tempdir_in(base).unwrap();
    let bash = tools.path().join("bash");
    std::fs::write(
        &bash,
        "#!/bin/sh\necho 'error: RocksDB benchmark dependencies are unavailable; Cargo was not invoked' >&2\nexit 1\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&bash).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&bash, permissions).unwrap();
    }

    Command::cargo_bin("meteordb-rocks-bench")
        .unwrap()
        .args(["--engine", "rocksdb", "--smoke"])
        .env("PATH", tools.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "RocksDB benchmark dependencies are unavailable",
        ))
        .stderr(predicate::str::contains("--features").not());
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

#[test]
fn both_frontends_reject_non_equivalent_foreground_thread_counts() {
    let base = std::env::current_dir()
        .unwrap()
        .join("target/benchmark-test-tmp");
    std::fs::create_dir_all(&base).unwrap();
    let directory = tempfile::tempdir_in(base).unwrap();
    let path = directory.path().join("threads.json");
    let mut workload = smoke_workload();
    workload.engine.threads = 2;
    std::fs::write(&path, serde_json::to_vec(&workload).unwrap()).unwrap();

    for engine in ["meteordb", "rocksdb"] {
        Command::cargo_bin("meteordb-rocks-bench")
            .unwrap()
            .args(["--engine", engine, "--workload"])
            .arg(&path)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "engine.threads must be 1 foreground workload thread",
            ));
    }
}

#[test]
fn no_compression_json_and_human_output_are_labeled_equivalent() {
    let output = Command::cargo_bin("meteordb-rocks-bench")
        .unwrap()
        .args(["--engine", "meteordb", "--smoke", "--pretty"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let human = String::from_utf8(output.stdout).unwrap();
    assert!(human.contains("both engines are configured with no compression"));
    let result: ComparisonResult = serde_json::from_str(&human).unwrap();
    assert_eq!(result.engine_options.compression, CompressionConfig::None);
    assert!(
        result
            .semantic_equivalence
            .iter()
            .any(|note| note.contains("both engines are configured with no compression"))
    );
    assert!(
        result
            .non_equivalence
            .iter()
            .all(|note| !note.contains("Snappy"))
    );
}
