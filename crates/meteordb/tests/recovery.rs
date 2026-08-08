use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use meteordb::{
    Durability, DurableFile, DurableFs, Engine, Error, Options, OsDurableFs, WalWriter, WriteBatch,
};

fn options(path: &Path) -> Options {
    let mut options = Options::new(path);
    options.memtable_bytes = 64;
    options.block_bytes = 64;
    options.max_key_bytes = 64;
    options.max_value_bytes = 1024;
    options.max_batch_bytes = 2048;
    options
}

#[test]
fn synchronous_writes_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let options = options(dir.path());
    let db = Engine::open(options.clone()).unwrap();
    db.put(b"key", b"value").unwrap();
    drop(db);

    let reopened = Engine::open(options).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn buffered_writes_survive_restart_after_explicit_sync() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = options(dir.path());
    options.durability = Durability::Buffered;
    let db = Engine::open(options.clone()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.sync().unwrap();
    drop(db);

    let reopened = Engine::open(options).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn explicit_sync_synchronizes_every_owned_wal_after_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(SyncGate::default());
    let fs = Arc::new(BlockingTrackingFs::new(gate.clone(), None));
    let mut configured = options(dir.path());
    configured.durability = Durability::Buffered;
    configured.memtable_bytes = 1;
    configured.max_immutable_memtables = 4;
    let db = Engine::open_with_fs(configured, fs.clone()).unwrap();
    fs.clear();

    db.put(b"first", b"value").unwrap();
    gate.wait_until_blocked();
    db.put(b"second", b"value").unwrap();
    let required = database_files(dir.path(), ".wal");
    db.sync().unwrap();
    let events = fs.events();
    gate.release();
    drop(db);

    for wal in required {
        let wal = file_name(&wal);
        assert!(
            events.iter().any(|event| event == &format!("sync {wal}")),
            "{wal} was not synchronized: {events:?}"
        );
    }
}

#[test]
fn explicit_sync_propagates_an_immutable_wal_sync_failure_terminally() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(SyncGate::default());
    let fs = Arc::new(BlockingTrackingFs::new(gate.clone(), Some("000002.wal")));
    let mut configured = options(dir.path());
    configured.durability = Durability::Buffered;
    configured.memtable_bytes = 1;
    let db = Engine::open_with_fs(configured, fs).unwrap();

    db.put(b"first", b"value").unwrap();
    gate.wait_until_blocked();
    let first = db.sync().unwrap_err().to_string();
    let later = db.put(b"later", b"value").unwrap_err().to_string();
    gate.release();
    drop(db);

    assert!(first.contains("sync WAL"));
    assert!(first.contains("injected WAL sync failure"));
    assert_eq!(later, first);
}

#[test]
fn successful_close_synchronizes_every_owned_wal_after_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(SyncGate::default());
    let fs = Arc::new(BlockingTrackingFs::new(gate.clone(), None));
    let mut configured = options(dir.path());
    configured.durability = Durability::Buffered;
    configured.memtable_bytes = 1;
    configured.max_immutable_memtables = 4;
    let db = Engine::open_with_fs(configured, fs.clone()).unwrap();
    fs.clear();

    db.put(b"first", b"value").unwrap();
    gate.wait_until_blocked();
    db.put(b"second", b"value").unwrap();
    let required = database_files(dir.path(), ".wal");
    db.close().unwrap();
    let events = fs.events();
    gate.release();
    drop(db);

    for wal in required {
        let wal = file_name(&wal);
        assert!(
            events.iter().any(|event| event == &format!("sync {wal}")),
            "{wal} was not synchronized: {events:?}"
        );
    }
}

#[test]
fn close_propagates_an_immutable_wal_sync_failure_terminally() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(SyncGate::default());
    let fs = Arc::new(BlockingTrackingFs::new(gate.clone(), Some("000002.wal")));
    let mut configured = options(dir.path());
    configured.durability = Durability::Buffered;
    configured.memtable_bytes = 1;
    let db = Engine::open_with_fs(configured, fs).unwrap();

    db.put(b"first", b"value").unwrap();
    gate.wait_until_blocked();
    let error = db.close().unwrap_err().to_string();
    gate.release();
    drop(db);

    assert!(error.contains("sync WAL"));
    assert!(error.contains("injected WAL sync failure"));
}

#[test]
fn explicit_flush_creates_l0_file_and_recovery_reads_it() {
    let dir = tempfile::tempdir().unwrap();
    let options = options(dir.path());
    let db = Engine::open(options.clone()).unwrap();
    db.put(b"key", b"value").unwrap();

    db.flush().unwrap();

    assert!(database_files(dir.path(), ".sst").len() == 1);
    drop(db);
    let reopened = Engine::open(options).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn close_without_flush_leaves_a_replayable_wal() {
    let dir = tempfile::tempdir().unwrap();
    let options = options(dir.path());
    let db = Engine::open(options.clone()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.close().unwrap();
    drop(db);

    assert!(!database_files(dir.path(), ".wal").is_empty());
    let reopened = Engine::open(options).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn obsolete_wal_is_removed_only_after_manifest_install() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(TrackingFs::default());
    let mut configured = options(dir.path());
    configured.memtable_bytes = 1;
    let db = Engine::open_with_fs(configured, fs.clone()).unwrap();
    fs.clear();

    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();

    let events = fs.events();
    let manifest_sync = events
        .iter()
        .rposition(|event| event.starts_with("sync MANIFEST-"))
        .expect("flush must synchronize a manifest edit");
    let wal_remove = events
        .iter()
        .position(|event| event.starts_with("remove ") && event.ends_with(".wal"))
        .expect("flush must retire its obsolete WAL");
    assert!(
        manifest_sync < wal_remove,
        "WAL was removed before manifest synchronization: {events:?}"
    );
}

#[test]
fn writes_stall_at_the_immutable_memtable_limit() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(SyncGate::default());
    let fs = Arc::new(BlockingSstableFs {
        gate: gate.clone(),
        inner: OsDurableFs,
    });
    let mut configured = options(dir.path());
    configured.memtable_bytes = 1;
    configured.max_immutable_memtables = 1;
    let db = Engine::open_with_fs(configured, fs).unwrap();

    db.put(b"first", b"value").unwrap();
    gate.wait_until_blocked();
    assert!(matches!(
        db.put(b"second", b"value"),
        Err(Error::WriteStall {
            immutable_memtables: 1
        })
    ));

    gate.release();
    db.flush().unwrap();
    db.put(b"second", b"value").unwrap();
}

#[test]
fn background_flush_failures_are_returned_to_waiters_and_later_writes() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FailingInstallFs(OsDurableFs));
    let db = Engine::open_with_fs(options(dir.path()), fs).unwrap();
    db.put(b"key", b"value").unwrap();

    let first = db.flush().unwrap_err().to_string();
    let later = db.put(b"later", b"value").unwrap_err().to_string();
    let read = db.get(b"key").unwrap_err().to_string();

    assert!(first.contains("install SSTable"));
    assert_eq!(later, first);
    assert_eq!(read, first);
}

#[test]
fn recovery_removes_an_unreferenced_final_sstable() {
    let dir = tempfile::tempdir().unwrap();
    let configured = options(dir.path());
    drop(Engine::open(configured.clone()).unwrap());
    let orphan = dir.path().join("000099.sst");
    std::fs::write(&orphan, b"unpublished table").unwrap();

    drop(Engine::open(configured).unwrap());

    assert!(!orphan.exists());
}

#[test]
fn recovery_schedules_an_oversized_replayed_memtable() {
    let dir = tempfile::tempdir().unwrap();
    let mut initial = options(dir.path());
    initial.memtable_bytes = 4096;
    let db = Engine::open(initial).unwrap();
    db.put(b"key", vec![b'x'; 256]).unwrap();
    drop(db);

    let mut recovered = options(dir.path());
    recovered.memtable_bytes = 64;
    let db = Engine::open(recovered).unwrap();
    db.flush().unwrap();

    assert_eq!(database_files(dir.path(), ".sst").len(), 1);
    assert_eq!(db.get(b"key").unwrap(), Some(vec![b'x'; 256]));
}

#[test]
fn flushed_tombstone_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let configured = options(dir.path());
    let db = Engine::open(configured.clone()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    db.delete(b"key").unwrap();
    db.flush().unwrap();
    drop(db);

    let reopened = Engine::open(configured).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[cfg(unix)]
#[test]
fn recovery_does_not_follow_a_wal_symlink() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let configured = options(dir.path());
    let db = Engine::open(configured.clone()).unwrap();
    db.put(b"key", b"value").unwrap();
    drop(db);
    let wal = database_files(dir.path(), ".wal").pop().unwrap();
    std::fs::remove_file(&wal).unwrap();
    let target = dir.path().join("outside");
    std::fs::write(&target, b"not a WAL").unwrap();
    symlink(&target, &wal).unwrap();

    assert!(matches!(
        Engine::open(configured),
        Err(Error::Io {
            operation: "read WAL",
            ..
        })
    ));
    assert_eq!(std::fs::read(target).unwrap(), b"not a WAL");
}

#[test]
fn recovery_never_appends_after_a_torn_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let configured = options(dir.path());
    let db = Engine::open(configured.clone()).unwrap();
    db.put(b"torn", b"record").unwrap();
    drop(db);
    let wal = database_files(dir.path(), ".wal").pop().unwrap();
    let length = std::fs::metadata(&wal).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal)
        .unwrap()
        .set_len(length - 1)
        .unwrap();

    let recovered = Engine::open(configured.clone()).unwrap();
    recovered.put(b"new", b"value").unwrap();
    drop(recovered);

    let reopened = Engine::open(configured).unwrap();
    assert_eq!(
        reopened.get(b"new").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn recovery_rejects_a_missing_sole_required_wal() {
    let dir = tempfile::tempdir().unwrap();
    let configured = options(dir.path());
    let db = Engine::open(configured.clone()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.close().unwrap();
    drop(db);
    let wal = database_files(dir.path(), ".wal").pop().unwrap();
    std::fs::remove_file(wal).unwrap();

    assert!(matches!(
        Engine::open(configured),
        Err(Error::Corruption {
            context: "WAL recovery",
            detail,
        }) if detail.contains("missing required WAL")
    ));
}

#[test]
fn recovery_rejects_a_missing_middle_required_wal() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(SyncGate::default());
    let fs = Arc::new(BlockingTrackingFs::new(gate.clone(), None));
    let mut configured = options(dir.path());
    configured.memtable_bytes = 1;
    configured.max_immutable_memtables = 4;
    let db = Engine::open_with_fs(configured.clone(), fs).unwrap();

    db.put(b"first", b"value").unwrap();
    gate.wait_until_blocked();
    db.put(b"second", b"value").unwrap();
    db.put(b"third", b"value").unwrap();
    db.close().unwrap();
    gate.release();
    drop(db);

    let mut wals = database_files(dir.path(), ".wal");
    wals.sort();
    assert!(wals.len() >= 3, "expected at least three required WALs");
    std::fs::remove_file(&wals[wals.len() - 2]).unwrap();
    assert!(matches!(
        Engine::open(configured),
        Err(Error::Corruption {
            context: "WAL recovery",
            detail,
        }) if detail.contains("sequence")
    ));
}

#[test]
fn recovery_rejects_a_sequence_gap_inside_one_wal() {
    let dir = tempfile::tempdir().unwrap();
    let configured = options(dir.path());
    drop(Engine::open(configured.clone()).unwrap());
    let wal = dir.path().join("000002.wal");
    std::fs::remove_file(&wal).unwrap();
    let mut writer = WalWriter::create(&wal, configured.max_batch_bytes).unwrap();
    let mut first = WriteBatch::default();
    first.put(b"first", b"value");
    writer.append(1, &first, Durability::Sync).unwrap();
    let mut third = WriteBatch::default();
    third.put(b"third", b"value");
    writer.append(3, &third, Durability::Sync).unwrap();
    drop(writer);

    assert!(matches!(
        Engine::open(configured),
        Err(Error::Corruption {
            context: "WAL recovery",
            detail,
        }) if detail.contains("expected sequence 2")
    ));
}

#[test]
fn a_committed_write_stays_successful_when_successor_wal_creation_fails() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FailSecondWalCreateFs {
        wal_creates: AtomicUsize::new(0),
        inner: OsDurableFs,
    });
    let mut configured = options(dir.path());
    configured.memtable_bytes = 1;
    let db = Engine::open_with_fs(configured, fs).unwrap();

    db.put(b"key", b"value").unwrap();

    assert_eq!(db.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
    assert!(db.put(b"later", b"value").is_err());
}

fn database_files(path: &Path, suffix: &str) -> Vec<PathBuf> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(suffix))
        })
        .collect()
}

#[derive(Default)]
struct TrackingFs {
    inner: OsDurableFs,
    events: Arc<Mutex<Vec<String>>>,
}

struct BlockingTrackingFs {
    gate: Arc<SyncGate>,
    fail_wal_sync: Option<&'static str>,
    inner: OsDurableFs,
    events: Arc<Mutex<Vec<String>>>,
}

impl BlockingTrackingFs {
    fn new(gate: Arc<SyncGate>, fail_wal_sync: Option<&'static str>) -> Self {
        Self {
            gate,
            fail_wal_sync,
            inner: OsDurableFs,
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }

    fn tracked(&self, path: &Path, block_sync: bool) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(BlockingTrackingFile {
            inner: self.inner.create(path)?,
            name: file_name(path),
            block_sync,
            gate: self.gate.clone(),
            events: self.events.clone(),
        }))
    }
}

struct BlockingTrackingFile {
    inner: Box<dyn DurableFile>,
    name: String,
    block_sync: bool,
    gate: Arc<SyncGate>,
    events: Arc<Mutex<Vec<String>>>,
}

impl DurableFile for BlockingTrackingFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("sync {}", self.name));
        if self.block_sync {
            self.gate.block();
        }
        self.inner.sync_all()
    }
}

impl DurableFs for BlockingTrackingFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.tracked(path, file_name(path).ends_with(".sst.tmp"))
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(BlockingTrackingFile {
            inner: self.inner.append(path)?,
            name: file_name(path),
            block_sync: false,
            gate: self.gate.clone(),
            events: self.events.clone(),
        }))
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(BlockingTrackingFile {
            inner: self.inner.append_existing(path)?,
            name: file_name(path),
            block_sync: false,
            gate: self.gate.clone(),
            events: self.events.clone(),
        }))
    }

    fn sync_file(&self, path: &Path) -> std::io::Result<()> {
        let name = file_name(path);
        self.events.lock().unwrap().push(format!("sync {name}"));
        if self.fail_wal_sync == Some(name.as_str()) {
            return Err(std::io::Error::other("injected WAL sync failure"));
        }
        self.inner.sync_file(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_replace(source, destination)
    }

    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_install(source, destination)
    }

    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.inner.remove_file(path)
    }
}

impl TrackingFs {
    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
}

struct TrackingFile {
    inner: Box<dyn DurableFile>,
    name: String,
    events: Arc<Mutex<Vec<String>>>,
}

impl DurableFile for TrackingFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("sync {}", self.name));
        self.inner.sync_all()
    }
}

impl DurableFs for TrackingFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(TrackingFile {
            inner: self.inner.create(path)?,
            name: file_name(path),
            events: self.events.clone(),
        }))
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(TrackingFile {
            inner: self.inner.append(path)?,
            name: file_name(path),
            events: self.events.clone(),
        }))
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(TrackingFile {
            inner: self.inner.append_existing(path)?,
            name: file_name(path),
            events: self.events.clone(),
        }))
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_replace(source, destination)
    }

    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_install(source, destination)
    }

    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("remove {}", file_name(path)));
        self.inner.remove_file(path)
    }
}

#[derive(Default)]
struct SyncGate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl SyncGate {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn wait_until_blocked(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.changed.notify_all();
    }
}

struct BlockingFile {
    inner: Box<dyn DurableFile>,
    block_sync: bool,
    gate: Arc<SyncGate>,
}

impl DurableFile for BlockingFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        if self.block_sync {
            self.gate.block();
        }
        self.inner.sync_all()
    }
}

struct BlockingSstableFs {
    gate: Arc<SyncGate>,
    inner: OsDurableFs,
}

impl DurableFs for BlockingSstableFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(BlockingFile {
            inner: self.inner.create(path)?,
            block_sync: file_name(path).ends_with(".sst.tmp"),
            gate: self.gate.clone(),
        }))
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.inner.append(path)
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.inner.append_existing(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_replace(source, destination)
    }

    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_install(source, destination)
    }
}

struct FailingInstallFs(OsDurableFs);

impl DurableFs for FailingInstallFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.0.create(path)
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.0.append(path)
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.0.append_existing(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.0.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.0.atomic_replace(source, destination)
    }

    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        if file_name(destination).ends_with(".sst") {
            Err(std::io::Error::other("injected SSTable install failure"))
        } else {
            self.0.atomic_install(source, destination)
        }
    }
}

struct FailSecondWalCreateFs {
    wal_creates: AtomicUsize,
    inner: OsDurableFs,
}

impl DurableFs for FailSecondWalCreateFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        if path.extension().is_some_and(|extension| extension == "wal")
            && self.wal_creates.fetch_add(1, Ordering::SeqCst) == 1
        {
            return Err(std::io::Error::other(
                "injected successor WAL creation failure",
            ));
        }
        self.inner.create(path)
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.inner.append(path)
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.inner.append_existing(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_replace(source, destination)
    }

    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_install(source, destination)
    }
}

fn file_name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}
