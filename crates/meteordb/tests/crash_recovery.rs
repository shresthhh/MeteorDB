use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use meteordb::{
    Durability, DurableFs, Engine, Error, FaultEvent, FaultOperation, FaultyFs, Options, WriteBatch,
};

fn options(path: &Path) -> Options {
    let mut options = Options::new(path);
    options.memtable_bytes = 128;
    options.target_sstable_bytes = 128;
    options.block_bytes = 64;
    options
}

#[test]
fn faulty_fs_records_and_fails_the_selected_durable_operation() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FaultyFs::recording());
    let db = Engine::open_with_fs(options(dir.path()), fs.clone()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    drop(db);

    let events = fs.events();
    assert!(
        events
            .iter()
            .any(|event| event.operation == FaultOperation::Write)
    );
    assert!(
        events
            .iter()
            .any(|event| event.operation == FaultOperation::FileSync)
    );
    assert!(
        events
            .iter()
            .any(|event| event.operation == FaultOperation::AtomicInstall)
    );
    assert!(
        events
            .iter()
            .any(|event| event.operation == FaultOperation::DirectorySync)
    );

    let failed = Arc::new(FaultyFs::fail_at(1));
    let failed_dir = tempfile::tempdir().unwrap();
    let error = match Engine::open_with_fs(options(failed_dir.path()), failed) {
        Ok(_) => panic!("selected crash point unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("injected crash"));
}

#[test]
fn explicit_crash_loses_buffered_writes_without_a_successful_sync() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FaultyFs::recording());
    let mut configured = options(dir.path());
    configured.durability = Durability::Buffered;
    configured.memtable_bytes = usize::MAX;
    let db = Engine::open_with_fs(configured, fs.clone()).unwrap();

    db.put(b"buffered", b"lost").unwrap();
    fs.crash().unwrap();
    drop(db);

    let reopened = Engine::open(options(dir.path())).unwrap();
    assert_eq!(reopened.get(b"buffered").unwrap(), None);
}

#[test]
fn explicit_crash_preserves_acknowledged_synchronous_writes() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FaultyFs::recording());
    let mut configured = options(dir.path());
    configured.memtable_bytes = usize::MAX;
    let db = Engine::open_with_fs(configured, fs.clone()).unwrap();

    db.put(b"synced", b"preserved").unwrap();
    fs.crash().unwrap();
    drop(db);

    let reopened = Engine::open(options(dir.path())).unwrap();
    assert_eq!(
        reopened.get(b"synced").unwrap().as_deref(),
        Some(&b"preserved"[..])
    );
}

#[test]
fn explicit_crash_discards_unsynced_truncation_and_rename() {
    let dir = tempfile::tempdir().unwrap();
    let fs = FaultyFs::recording();
    let data = dir.path().join("data");
    let mut file = fs.create(&data).unwrap();
    file.write_all(b"durable-data").unwrap();
    file.sync_all().unwrap();
    fs.sync_directory(dir.path()).unwrap();
    drop(file);

    fs.truncate_file(&data, 3).unwrap();
    fs.crash().unwrap();
    assert_eq!(std::fs::read(&data).unwrap(), b"durable-data");

    let replacement = dir.path().join("replacement");
    let mut file = fs.create(&replacement).unwrap();
    file.write_all(b"replacement-data").unwrap();
    file.sync_all().unwrap();
    drop(file);
    fs.atomic_replace(&replacement, &data).unwrap();
    fs.crash().unwrap();

    assert_eq!(std::fs::read(&data).unwrap(), b"durable-data");
    assert!(!replacement.exists());
}

#[test]
fn corrupt_interrupted_bootstrap_manifest_does_not_publish_current() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(options(dir.path())).unwrap();
    drop(db);
    std::fs::remove_file(dir.path().join("CURRENT")).unwrap();
    let manifest = dir.path().join("MANIFEST-000001");
    let mut bytes = std::fs::read(&manifest).unwrap();
    let corrupt_at = bytes.len() / 2;
    bytes[corrupt_at] ^= 0x80;
    std::fs::write(&manifest, bytes).unwrap();

    let error = match Engine::open(options(dir.path())) {
        Ok(_) => panic!("corrupt interrupted bootstrap unexpectedly opened"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        Error::Corruption { .. } | Error::UnsupportedFormat { .. }
    ));
    assert!(!dir.path().join("CURRENT").exists());
}

#[test]
fn every_recorded_crash_boundary_recovers_acknowledged_and_atomic_batches() {
    let baseline_dir = tempfile::tempdir().unwrap();
    let baseline_fs = Arc::new(FaultyFs::recording());
    let baseline = run_workload(baseline_dir.path(), baseline_fs.clone());
    assert!(baseline.error.is_none());
    let events = baseline_fs.events();
    assert_required_crash_classes(&events);

    for event in events {
        let dir = tempfile::tempdir().unwrap();
        let outcome = run_workload(dir.path(), Arc::new(FaultyFs::fail_at(event.index)));
        assert!(
            outcome.error.is_some(),
            "seed=0xc2a5_0010 crash event {event:?} was not reached"
        );

        let reopened = match Engine::open(options(dir.path())) {
            Ok(db) => db,
            Err(Error::Corruption { .. }) if outcome.acknowledged.is_empty() => continue,
            Err(error) => panic!(
                "seed=0xc2a5_0010 crash event {event:?} failed reopen after {:?}: {error}",
                outcome.error
            ),
        };

        for batch in &outcome.acknowledged {
            for (key, value) in batch {
                assert_eq!(
                    reopened.get(key).unwrap().as_deref(),
                    Some(value.as_slice()),
                    "seed=0xc2a5_0010 lost acknowledged key {:?} at {event:?}",
                    String::from_utf8_lossy(key)
                );
            }
        }
        for batch in &outcome.issued {
            let visible = batch
                .iter()
                .filter(|(key, value)| {
                    reopened.get(key).unwrap().as_deref() == Some(value.as_slice())
                })
                .count();
            assert!(
                visible == 0 || visible == batch.len(),
                "seed=0xc2a5_0010 exposed a partial batch at {event:?}: {visible}/{}",
                batch.len()
            );
        }
    }
}

struct WorkloadOutcome {
    acknowledged: Vec<Vec<(Vec<u8>, Vec<u8>)>>,
    issued: Vec<Vec<(Vec<u8>, Vec<u8>)>>,
    error: Option<String>,
}

fn run_workload(path: &Path, fs: Arc<FaultyFs>) -> WorkloadOutcome {
    let mut outcome = WorkloadOutcome {
        acknowledged: Vec::new(),
        issued: Vec::new(),
        error: None,
    };
    let db = match Engine::open_with_fs(options(path), fs) {
        Ok(db) => db,
        Err(error) => {
            outcome.error = Some(error.to_string());
            return outcome;
        }
    };

    for generation in 0..5 {
        let entries = vec![
            (
                format!("g{generation}-a").into_bytes(),
                format!("value-{generation}-a").into_bytes(),
            ),
            (
                format!("g{generation}-b").into_bytes(),
                format!("value-{generation}-b").into_bytes(),
            ),
        ];
        let mut batch = WriteBatch::default();
        for (key, value) in &entries {
            batch.put(key, value);
        }
        outcome.issued.push(entries.clone());
        if let Err(error) = db.write(batch) {
            outcome.error = Some(error.to_string());
            break;
        }
        outcome.acknowledged.push(entries);
        if let Err(error) = db.flush() {
            outcome.error = Some(error.to_string());
            break;
        }
    }

    if outcome.error.is_none() {
        if let Err(error) = db.compact() {
            outcome.error = Some(error.to_string());
        }
    }
    drop(db);
    outcome
}

fn assert_required_crash_classes(events: &[FaultEvent]) {
    let classes = events
        .iter()
        .map(|event| {
            let name = event
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            (event.operation, name.to_owned())
        })
        .collect::<BTreeSet<_>>();

    for expected in [
        (FaultOperation::Write, ".wal"),
        (FaultOperation::FileSync, ".wal"),
        (FaultOperation::FileSync, ".sst.tmp"),
        (FaultOperation::AtomicInstall, ".sst"),
        (FaultOperation::Write, "MANIFEST-"),
        (FaultOperation::FileSync, "MANIFEST-"),
        (FaultOperation::AtomicInstall, "CURRENT"),
        (FaultOperation::DirectorySync, ""),
        (FaultOperation::Remove, ".wal"),
    ] {
        assert!(
            classes.iter().any(|(operation, name)| {
                *operation == expected.0
                    && (expected.1.is_empty()
                        || name.starts_with(expected.1)
                        || name.ends_with(expected.1))
            }),
            "baseline omitted required crash class {expected:?}: {classes:?}"
        );
    }
}
