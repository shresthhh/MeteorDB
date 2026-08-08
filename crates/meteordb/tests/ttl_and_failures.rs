use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use meteordb::{
    Clock, DurableFile, DurableFs, Engine, Error, ManualClock, Options, OsDurableFs, ScanBounds,
    TableReader, WriteBatch,
};

#[test]
fn ttl_uses_wall_clock_at_each_read_and_never_resurrects_an_older_version() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let db = Engine::open_with_clock(Options::new(dir.path()), clock.clone()).unwrap();

    db.put(b"key", b"older").unwrap();
    db.put_with_ttl(b"key", b"newer", 10).unwrap();
    let snapshot = db.snapshot().unwrap();

    assert_eq!(db.get(b"key").unwrap().as_deref(), Some(&b"newer"[..]));
    assert_eq!(
        snapshot.get(b"key").unwrap().as_deref(),
        Some(&b"newer"[..])
    );

    clock.set(1_010).unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);
    assert_eq!(snapshot.get(b"key").unwrap(), None);
    assert!(collect(db.scan(ScanBounds::all(), usize::MAX).unwrap()).is_empty());
    assert!(collect(snapshot.scan(ScanBounds::all(), usize::MAX).unwrap()).is_empty());

    db.put_with_ttl(b"immediate", b"value", 0).unwrap();
    assert_eq!(db.get(b"immediate").unwrap(), None);
}

#[test]
fn a_snapshot_before_the_ttl_version_still_reads_its_older_mvcc_version() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(20));
    let db = Engine::open_with_clock(Options::new(dir.path()), clock.clone()).unwrap();

    db.put(b"key", b"older").unwrap();
    let before_ttl = db.snapshot().unwrap();
    db.put_with_ttl(b"key", b"newer", 5).unwrap();
    clock.set(25).unwrap();

    assert_eq!(db.get(b"key").unwrap(), None);
    assert_eq!(
        before_ttl.get(b"key").unwrap().as_deref(),
        Some(&b"older"[..])
    );
}

#[test]
fn ttl_rejects_negative_and_overflowing_expiration_calculations() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(100));
    let db = Engine::open_with_clock(Options::new(dir.path()), clock.clone()).unwrap();

    assert!(matches!(
        db.put_with_ttl(b"negative", b"value", -1),
        Err(Error::InvalidArgument(message)) if message.contains("ttl_ms")
    ));

    clock.set(u64::MAX - 1).unwrap();
    assert!(matches!(
        db.put_with_ttl(b"overflow", b"value", 2),
        Err(Error::InvalidArgument(message)) if message.contains("overflow")
    ));
    assert_eq!(db.get(b"negative").unwrap(), None);
    assert_eq!(db.get(b"overflow").unwrap(), None);
}

#[test]
fn expiration_persists_across_flush_and_restart_with_an_injected_clock() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(500));
    let options = Options::new(dir.path());
    {
        let db = Engine::open_with_clock(options.clone(), clock.clone()).unwrap();
        db.put_with_ttl(b"key", b"value", 10).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    clock.set(510).unwrap();
    let db = Engine::open_with_clock(options, clock).unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);
    assert!(collect(db.scan(ScanBounds::all(), usize::MAX).unwrap()).is_empty());
}

#[test]
fn compaction_reclaims_an_expired_version_and_the_history_it_hides() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut options = Options::new(dir.path());
    options.target_sstable_bytes = 128;
    let db = Engine::open_with_clock(options, clock.clone()).unwrap();

    db.put(b"expired", b"older").unwrap();
    db.flush().unwrap();
    db.put_with_ttl(b"expired", b"newer", 5).unwrap();
    db.flush().unwrap();
    for index in 0..3 {
        db.put(format!("filler-{index}"), vec![b'x'; 96]).unwrap();
        db.flush().unwrap();
    }

    clock.set(1_005).unwrap();
    assert!(db.compact().unwrap());
    assert_eq!(db.get(b"expired").unwrap(), None);
    assert!(
        sstable_internal_keys(dir.path())
            .iter()
            .all(|key| key.as_slice() != b"expired")
    );
}

#[test]
fn point_reads_reject_clock_rollback_and_do_not_resurrect_expired_values() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let db = Engine::open_with_clock(Options::new(dir.path()), clock.clone()).unwrap();

    db.put_with_ttl(b"key", b"value", 10).unwrap();
    clock.set(1_010).unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);

    assert!(matches!(
        clock.set(1_009),
        Err(Error::InvalidArgument(message)) if message.contains("backward")
    ));
    assert_eq!(clock.now_unix_ms(), 1_010);
    assert_eq!(db.get(b"key").unwrap(), None);
}

#[test]
fn compaction_reclamation_rejects_clock_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(2_000));
    let mut options = Options::new(dir.path());
    options.target_sstable_bytes = 128;
    let db = Engine::open_with_clock(options, clock.clone()).unwrap();

    db.put(b"expired", b"older").unwrap();
    db.flush().unwrap();
    db.put_with_ttl(b"expired", b"newer", 5).unwrap();
    db.flush().unwrap();
    for index in 0..3 {
        db.put(format!("filler-{index}"), vec![b'x'; 96]).unwrap();
        db.flush().unwrap();
    }

    clock.set(2_005).unwrap();
    assert_eq!(db.get(b"expired").unwrap(), None);
    assert!(clock.set(2_004).is_err());
    assert!(db.compact().unwrap());
    assert!(
        sstable_internal_keys(dir.path())
            .iter()
            .all(|key| key.as_slice() != b"expired")
    );
}

#[test]
fn a_flush_failure_is_terminal_for_every_later_result_returning_operation() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FailingFlushFs(OsDurableFs));
    let clock = Arc::new(ManualClock::new(10));
    let db = Engine::open_with_fs_and_clock(Options::new(dir.path()), fs, clock).unwrap();
    db.put(b"key", b"value").unwrap();

    let stored = db.flush().unwrap_err().to_string();
    assert!(stored.contains("injected flush install failure"));
    assert_terminal_public_operations(&db, &stored);
}

#[test]
fn a_compaction_failure_is_terminal_for_every_later_result_returning_operation() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FailingCompactionFs::default());
    let clock = Arc::new(ManualClock::new(10));
    let mut options = Options::new(dir.path());
    options.target_sstable_bytes = 128;
    let db = Engine::open_with_fs_and_clock(options, fs.clone(), clock).unwrap();
    for index in 0..5 {
        db.put(format!("key-{index}"), vec![b'x'; 96]).unwrap();
        db.flush().unwrap();
    }
    fs.fail_next_compaction_sync();

    let stored = db.compact().unwrap_err().to_string();
    assert!(stored.contains("injected compaction sync failure"));
    assert_terminal_public_operations(&db, &stored);
}

fn assert_terminal_public_operations(db: &Engine, stored: &str) {
    assert_eq!(
        db.put_with_ttl(b"ttl", b"value", 1)
            .unwrap_err()
            .to_string(),
        stored
    );
    assert_eq!(
        db.put_with_ttl(b"ttl", b"value", -1)
            .unwrap_err()
            .to_string(),
        stored
    );
    assert_eq!(db.put(b"put", b"value").unwrap_err().to_string(), stored);
    assert_eq!(db.delete(b"key").unwrap_err().to_string(), stored);
    assert_eq!(
        db.write(WriteBatch::default()).unwrap_err().to_string(),
        stored
    );
    assert_eq!(db.get(b"key").unwrap_err().to_string(), stored);
    assert_eq!(result_error(db.scan(ScanBounds::all(), 1)), stored);
    assert_eq!(result_error(db.scan(ScanBounds::all(), 0)), stored);
    assert_eq!(result_error(db.scan_prefix(b"k", 1)), stored);
    assert_eq!(result_error(db.snapshot()), stored);
    assert_eq!(db.sync().unwrap_err().to_string(), stored);
    assert_eq!(db.flush().unwrap_err().to_string(), stored);
    assert_eq!(db.compact().unwrap_err().to_string(), stored);
    assert_eq!(db.close().unwrap_err().to_string(), stored);
}

fn collect(
    iterator: impl Iterator<Item = meteordb::Result<(Vec<u8>, Vec<u8>)>>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    iterator.collect::<meteordb::Result<Vec<_>>>().unwrap()
}

fn sstable_internal_keys(directory: &Path) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    for path in std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "sst"))
    {
        let reader = TableReader::open(path).unwrap();
        keys.extend(
            reader
                .iter()
                .map(|entry| entry.unwrap().0.user_key().to_vec()),
        );
    }

    keys
}

fn result_error<T>(result: meteordb::Result<T>) -> String {
    match result {
        Ok(_) => panic!("operation unexpectedly succeeded"),
        Err(error) => error.to_string(),
    }
}

struct FailingFlushFs(OsDurableFs);

impl DurableFs for FailingFlushFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.0.create(path)
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.0.append(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.0.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.0.atomic_replace(source, destination)
    }

    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        if destination
            .extension()
            .is_some_and(|extension| extension == "sst")
        {
            Err(std::io::Error::other("injected flush install failure"))
        } else {
            self.0.atomic_install(source, destination)
        }
    }
}

#[derive(Default)]
struct FailingCompactionFs {
    inner: OsDurableFs,
    fail_sync: Arc<AtomicBool>,
}

impl FailingCompactionFs {
    fn fail_next_compaction_sync(&self) {
        self.fail_sync.store(true, Ordering::Release);
    }
}

struct FailingSyncFile {
    inner: Box<dyn DurableFile>,
    fail_sync: Arc<AtomicBool>,
}

impl DurableFile for FailingSyncFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        if self.fail_sync.swap(false, Ordering::AcqRel) {
            return Err(std::io::Error::other("injected compaction sync failure"));
        }
        self.inner.sync_all()
    }
}

impl DurableFs for FailingCompactionFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let file = self.inner.create(path)?;
        if path.to_string_lossy().ends_with(".sst.tmp") {
            Ok(Box::new(FailingSyncFile {
                inner: file,
                fail_sync: self.fail_sync.clone(),
            }))
        } else {
            Ok(file)
        }
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.inner.append(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_replace(source, destination)
    }
}
