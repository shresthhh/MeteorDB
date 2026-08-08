use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use meteordb::{Engine, Options};
use predicates::prelude::*;

fn command(path: &Path) -> Command {
    let mut command = Command::cargo_bin("meteordb").unwrap();
    command.arg("--path").arg(path);
    command
}

fn create_database(path: &Path) -> PathBuf {
    let database = Engine::open(Options::new(path)).unwrap();
    for number in 0..8 {
        database
            .put(
                format!("key-{number:03}"),
                format!("value-{number:03}-{}", "x".repeat(256)),
            )
            .unwrap();
    }
    database.flush().unwrap();
    database.close().unwrap();
    find_file(path, ".sst")
}

fn find_file(path: &Path, suffix: &str) -> PathBuf {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|entry| entry.to_string_lossy().ends_with(suffix))
        .unwrap()
}

fn database_bytes(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|entry| entry.is_file())
        .map(|entry| {
            (
                entry.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read(entry).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn corrupt_byte(path: &Path, offset: u64) {
    let original = fs::read(path).unwrap()[usize::try_from(offset).unwrap()];
    let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[original ^ 0xff]).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn check_validates_a_live_database_without_mutating_or_taking_the_writer_lock() {
    let dir = tempfile::tempdir().unwrap();
    let database = Engine::open(Options::new(dir.path())).unwrap();
    database.put(b"live", b"value").unwrap();
    let before = database_bytes(dir.path());

    command(dir.path())
        .arg("check")
        .assert()
        .success()
        .stdout(predicate::str::contains("status: ok"))
        .stdout(predicate::str::contains("WAL"));

    assert_eq!(database_bytes(dir.path()), before);
    database.put(b"still-writable", b"value").unwrap();
    database.close().unwrap();
}

#[test]
fn check_surfaces_sstable_corruption_with_a_distinct_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let sstable = create_database(dir.path());
    corrupt_byte(&sstable, 0);

    command(dir.path())
        .arg("check")
        .assert()
        .code(3)
        .stderr(predicate::str::contains("corruption"))
        .stderr(predicate::str::contains("SSTable"));
}

#[test]
fn check_surfaces_wal_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let database = Engine::open(Options::new(dir.path())).unwrap();
    database.put(b"key", b"value").unwrap();
    database.close().unwrap();
    let wal = find_file(dir.path(), ".wal");
    corrupt_byte(&wal, 0);

    command(dir.path())
        .arg("check")
        .assert()
        .code(3)
        .stderr(predicate::str::contains("corruption"))
        .stderr(predicate::str::contains("WAL"));
}

#[test]
fn check_does_not_recover_or_truncate_a_torn_manifest() {
    let dir = tempfile::tempdir().unwrap();
    create_database(dir.path());
    let manifest = find_file(dir.path(), "MANIFEST-000001");
    let original_len = fs::metadata(&manifest).unwrap().len();
    fs::OpenOptions::new()
        .append(true)
        .open(&manifest)
        .unwrap()
        .write_all(&[1, 2, 3])
        .unwrap();
    let damaged_len = fs::metadata(&manifest).unwrap().len();
    assert!(damaged_len > original_len);

    command(dir.path()).arg("check").assert().code(3);

    assert_eq!(fs::metadata(manifest).unwrap().len(), damaged_len);
}

#[test]
fn dump_manifest_has_stable_human_and_json_output() {
    let dir = tempfile::tempdir().unwrap();
    create_database(dir.path());

    command(dir.path())
        .arg("dump-manifest")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("manifest: MANIFEST-000001\n"))
        .stdout(predicate::str::contains("edits:"))
        .stdout(predicate::str::contains("level 0:"));

    let output = command(dir.path())
        .args(["dump-manifest", "--format", "json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["format_version"], 1);
    assert_eq!(document["manifest"], "MANIFEST-000001");
    assert!(document["edits"].as_array().unwrap().len() >= 2);
    assert_eq!(document["levels"].as_array().unwrap().len(), 7);
}

#[test]
fn dump_manifest_rejects_zero_limits_and_bounds_live_files() {
    let dir = tempfile::tempdir().unwrap();
    create_database(dir.path());

    command(dir.path())
        .args(["dump-manifest", "--max-edits", "0"])
        .assert()
        .code(2);
    command(dir.path())
        .args(["dump-manifest", "--max-files", "0"])
        .assert()
        .code(2);
    command(dir.path())
        .args(["dump-manifest", "--max-bytes", "0"])
        .assert()
        .code(2);

    let output = command(dir.path())
        .args([
            "dump-manifest",
            "--format",
            "json",
            "--max-edits",
            "1",
            "--max-files",
            "1",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["edits"].as_array().unwrap().len(), 1);
    assert!(
        document["levels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|level| level.as_array().unwrap().len())
            .sum::<usize>()
            <= 1
    );
    assert_eq!(document["files_total"], 1);
}

#[test]
fn manifest_corruption_after_the_edit_sample_cap_is_still_reported() {
    let dir = tempfile::tempdir().unwrap();
    create_database(dir.path());
    let manifest = find_file(dir.path(), "MANIFEST-000001");
    let length = fs::metadata(&manifest).unwrap().len();
    corrupt_byte(&manifest, length - 1);

    command(dir.path())
        .args(["dump-manifest", "--max-edits", "1"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("corruption"));
}

#[test]
fn dump_sstable_reports_checked_metadata_and_bounds_entries() {
    let dir = tempfile::tempdir().unwrap();
    let sstable = create_database(dir.path());
    let name = sstable.file_name().unwrap();

    command(dir.path())
        .arg("dump-sstable")
        .arg("--file")
        .arg(name)
        .args(["--max-entries", "2", "--max-blocks", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("file_number:"))
        .stdout(predicate::str::contains("data_blocks:"))
        .stdout(predicate::str::contains("checksums: ok"))
        .stdout(predicate::str::contains("shown_blocks: 1"))
        .stdout(predicate::str::contains("shown_entries: 2"))
        .stdout(predicate::str::contains("truncated: true"));
}

#[test]
fn dump_sstable_rejects_zero_limits() {
    let dir = tempfile::tempdir().unwrap();
    let sstable = create_database(dir.path());
    let name = sstable.file_name().unwrap();

    for (flag, value) in [
        ("--max-entries", "0"),
        ("--max-blocks", "0"),
        ("--max-bytes", "0"),
    ] {
        command(dir.path())
            .arg("dump-sstable")
            .arg("--file")
            .arg(name)
            .args([flag, value])
            .assert()
            .code(2);
    }
}

#[test]
fn bench_reports_seeded_configuration_throughput_and_percentiles() {
    let dir = tempfile::tempdir().unwrap();

    command(dir.path())
        .args([
            "bench",
            "--seconds",
            "1",
            "--workload",
            "inference-cache",
            "--seed",
            "7",
            "--dataset-size",
            "16",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workload: inference-cache"))
        .stdout(predicate::str::contains("durability: sync"))
        .stdout(predicate::str::contains("seed: 7"))
        .stdout(predicate::str::contains("operations:"))
        .stdout(predicate::str::contains("throughput_ops_per_second:"))
        .stdout(predicate::str::contains("p50_us:"))
        .stdout(predicate::str::contains("p95_us:"))
        .stdout(predicate::str::contains("p99_us:"));
}

#[test]
fn bench_maps_and_reports_both_durability_modes_in_json() {
    for mode in ["sync", "buffered"] {
        let dir = tempfile::tempdir().unwrap();
        let output = command(dir.path())
            .args([
                "bench",
                "--seconds",
                "1",
                "--workload",
                "inference-cache",
                "--dataset-size",
                "1",
                "--durability",
                mode,
                "--format",
                "json",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(document["durability"], mode);
    }
}

#[cfg(unix)]
#[test]
fn inspection_rejects_manifest_and_sstable_symlinks() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let sstable = create_database(dir.path());
    let manifest = find_file(dir.path(), "MANIFEST-000001");

    let manifest_target = dir.path().join("manifest-target");
    fs::rename(&manifest, &manifest_target).unwrap();
    symlink(&manifest_target, &manifest).unwrap();
    command(dir.path()).arg("check").assert().failure();

    fs::remove_file(&manifest).unwrap();
    fs::rename(&manifest_target, &manifest).unwrap();
    let table_target = dir.path().join("table-target");
    fs::rename(&sstable, &table_target).unwrap();
    symlink(&table_target, &sstable).unwrap();
    command(dir.path())
        .arg("check")
        .assert()
        .failure()
        .stderr(predicate::str::contains("SSTable"));
}

#[test]
fn missing_database_path_is_a_usage_error() {
    Command::cargo_bin("meteordb")
        .unwrap()
        .arg("check")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--path"));
}
