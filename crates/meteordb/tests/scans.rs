use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use meteordb::{
    Clock, Compression, DurableFile, DurableFs, Engine, Error, FileMeta, InternalKey, Options,
    OsDurableFs, ScanBounds, SystemClock, TableBuilder, ValueKind, VersionEdit, VersionSet,
    WriteBatch,
};

#[test]
fn scan_merges_memory_and_multiple_disk_levels_in_user_key_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let level_one = build_table(
        dir.path(),
        2,
        &[
            (b"a", 1, ValueKind::Value, b"level-one-a"),
            (b"c", 1, ValueKind::Value, b"level-one-c"),
            (b"e", 1, ValueKind::Value, b"level-one-e"),
        ],
    );
    let mut edit = VersionEdit::new();
    edit.add_file(1, level_one)
        .set_next_file_number(3)
        .set_last_sequence(1);
    versions.apply(edit).unwrap();
    drop(versions);

    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"b", b"level-zero-b").unwrap();
    db.put(b"c", b"level-zero-c").unwrap();
    db.put(b"d", b"level-zero-d").unwrap();
    db.flush().unwrap();
    db.put(b"c", b"memory-c").unwrap();
    db.put(b"f", b"memory-f").unwrap();

    assert_eq!(
        collect(db.scan(ScanBounds::all(), usize::MAX).unwrap()).unwrap(),
        vec![
            (b"a".to_vec(), b"level-one-a".to_vec()),
            (b"b".to_vec(), b"level-zero-b".to_vec()),
            (b"c".to_vec(), b"memory-c".to_vec()),
            (b"d".to_vec(), b"level-zero-d".to_vec()),
            (b"e".to_vec(), b"level-one-e".to_vec()),
            (b"f".to_vec(), b"memory-f".to_vec()),
        ]
    );
}

#[test]
fn scan_deduplicates_versions_and_hides_tombstones_and_expired_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"deleted", b"old").unwrap();
    db.put(b"replaced", b"old").unwrap();
    db.flush().unwrap();
    db.delete(b"deleted").unwrap();
    db.put(b"replaced", b"new").unwrap();
    let mut batch = WriteBatch::default();
    batch.put_with_expiration(b"expired", b"gone", Some(0));
    db.write(batch).unwrap();

    assert_eq!(
        collect(db.scan(ScanBounds::all(), usize::MAX).unwrap()).unwrap(),
        vec![(b"replaced".to_vec(), b"new".to_vec())]
    );
}

#[test]
fn flushed_expiration_hides_the_disk_value_without_exposing_an_older_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"k", b"older").unwrap();
    db.flush().unwrap();

    let expires_at = SystemClock.now_unix_ms().saturating_add(500);
    let mut batch = WriteBatch::default();
    batch.put_with_expiration(b"k", b"expiring", Some(expires_at));
    db.write(batch).unwrap();
    db.flush().unwrap();
    drop(db);

    let db = Engine::open(Options::new(dir.path())).unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"expiring"[..]));
    while SystemClock.now_unix_ms() <= expires_at {
        std::thread::sleep(Duration::from_millis(5));
    }

    assert_eq!(db.get(b"k").unwrap(), None);
    assert!(
        collect(db.scan(ScanBounds::all(), usize::MAX).unwrap())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn zero_limit_avoids_engine_state_and_sstable_setup() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"k", b"value").unwrap();
    db.flush().unwrap();
    let table = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .unwrap();
    std::fs::remove_file(table).unwrap();

    assert!(db.scan(ScanBounds::all(), 0).unwrap().next().is_none());
    assert!(db.scan(ScanBounds::all(), 1).is_err());

    db.close().unwrap();
    assert!(db.scan(ScanBounds::all(), 0).unwrap().next().is_none());
}

#[test]
fn scan_merges_mutable_and_immutable_memtables_while_flush_is_blocked() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(FlushGate::default());
    let fs = Arc::new(BlockingFlushFs { gate: gate.clone() });
    let mut options = Options::new(dir.path());
    options.memtable_bytes = 1;
    let db = Engine::open_with_fs(options, fs).unwrap();
    db.put(b"a", b"immutable").unwrap();
    gate.wait_until_blocked();
    db.put(b"b", b"mutable").unwrap();

    assert_eq!(
        collect(db.scan(ScanBounds::all(), usize::MAX).unwrap()).unwrap(),
        vec![
            (b"a".to_vec(), b"immutable".to_vec()),
            (b"b".to_vec(), b"mutable".to_vec()),
        ]
    );

    gate.release();
    db.flush().unwrap();
}

#[test]
fn snapshot_scan_keeps_its_sequence_after_new_writes_and_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"a", b"old-a").unwrap();
    db.put(b"b", b"old-b").unwrap();
    let snapshot = db.snapshot().unwrap();

    db.put(b"a", b"new-a").unwrap();
    db.delete(b"b").unwrap();
    db.put(b"c", b"new-c").unwrap();
    db.flush().unwrap();

    let snapshot_iterator = snapshot.scan(ScanBounds::all(), usize::MAX).unwrap();
    drop(snapshot);
    assert_eq!(
        collect(snapshot_iterator).unwrap(),
        vec![
            (b"a".to_vec(), b"old-a".to_vec()),
            (b"b".to_vec(), b"old-b".to_vec()),
        ]
    );
    assert_eq!(
        collect(db.scan(ScanBounds::all(), usize::MAX).unwrap()).unwrap(),
        vec![
            (b"a".to_vec(), b"new-a".to_vec()),
            (b"c".to_vec(), b"new-c".to_vec()),
        ]
    );
}

#[test]
fn scan_honors_every_inclusive_and_exclusive_bound_combination() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    for key in [b"a", b"b", b"c", b"d"] {
        db.put(key, key).unwrap();
    }

    let cases = [
        (
            Bound::Included(b"b".to_vec()),
            Bound::Included(b"c".to_vec()),
            vec![b"b".to_vec(), b"c".to_vec()],
        ),
        (
            Bound::Excluded(b"b".to_vec()),
            Bound::Included(b"c".to_vec()),
            vec![b"c".to_vec()],
        ),
        (
            Bound::Included(b"b".to_vec()),
            Bound::Excluded(b"c".to_vec()),
            vec![b"b".to_vec()],
        ),
        (
            Bound::Excluded(b"b".to_vec()),
            Bound::Excluded(b"d".to_vec()),
            vec![b"c".to_vec()],
        ),
    ];

    for (start, end, expected) in cases {
        let actual = collect(db.scan(ScanBounds::new(start, end), usize::MAX).unwrap())
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

#[test]
fn prefix_scans_handle_empty_and_all_ff_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    for key in [
        b"".as_slice(),
        b"a",
        b"ab",
        b"b",
        b"\xff",
        b"\xff\0",
        b"\xff\xff",
    ] {
        db.put(key, key).unwrap();
    }

    assert_eq!(
        keys(db.scan_prefix(b"", usize::MAX).unwrap()).unwrap(),
        vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"ab".to_vec(),
            b"b".to_vec(),
            b"\xff".to_vec(),
            b"\xff\0".to_vec(),
            b"\xff\xff".to_vec(),
        ]
    );
    assert_eq!(
        keys(db.scan_prefix(b"a", usize::MAX).unwrap()).unwrap(),
        vec![b"a".to_vec(), b"ab".to_vec()]
    );
    assert_eq!(
        keys(db.scan_prefix(b"\xff", usize::MAX).unwrap()).unwrap(),
        vec![b"\xff".to_vec(), b"\xff\0".to_vec(), b"\xff\xff".to_vec()]
    );
}

#[test]
fn scan_limit_counts_only_emitted_live_user_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"a", b"old-a").unwrap();
    db.put(b"b", b"old-b").unwrap();
    db.put(b"c", b"old-c").unwrap();
    db.flush().unwrap();
    db.put(b"a", b"new-a").unwrap();
    db.delete(b"b").unwrap();
    db.put(b"d", b"new-d").unwrap();

    assert!(
        collect(db.scan(ScanBounds::all(), 0).unwrap())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        collect(db.scan(ScanBounds::all(), 2).unwrap()).unwrap(),
        vec![
            (b"a".to_vec(), b"new-a".to_vec()),
            (b"c".to_vec(), b"old-c".to_vec()),
        ]
    );
}

#[test]
fn returned_iterator_does_not_keep_the_engine_write_state_locked() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"a", b"before").unwrap();
    let iterator = db.scan(ScanBounds::all(), usize::MAX).unwrap();

    db.put(b"b", b"after").unwrap();
    db.flush().unwrap();

    assert_eq!(
        collect(iterator).unwrap(),
        vec![(b"a".to_vec(), b"before".to_vec())]
    );
}

#[test]
fn data_block_corruption_is_returned_by_the_scan_iterator() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"k", b"value").unwrap();
    db.flush().unwrap();
    let path = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .unwrap();
    corrupt_byte(&path, 0);

    let mut iterator = db.scan(ScanBounds::all(), usize::MAX).unwrap();
    assert!(matches!(
        iterator.next(),
        Some(Err(Error::Corruption { .. }))
    ));
    assert!(iterator.next().is_none());
}

fn collect(
    iterator: impl Iterator<Item = meteordb::Result<(Vec<u8>, Vec<u8>)>>,
) -> meteordb::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    iterator.collect()
}

fn keys(
    iterator: impl Iterator<Item = meteordb::Result<(Vec<u8>, Vec<u8>)>>,
) -> meteordb::Result<Vec<Vec<u8>>> {
    iterator.map(|entry| entry.map(|(key, _)| key)).collect()
}

fn build_table(path: &Path, number: u64, entries: &[(&[u8], u64, ValueKind, &[u8])]) -> FileMeta {
    let table_path = path.join(format!("{number:06}.sst"));
    let mut builder =
        TableBuilder::create(&table_path, number, 64, 2, 10, Compression::None).unwrap();
    let mut entries = entries
        .iter()
        .map(|(key, sequence, kind, value)| {
            (InternalKey::try_new(key, *sequence, *kind).unwrap(), *value)
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in entries {
        builder.add(&key, value).unwrap();
    }
    let built = builder.finish().unwrap();
    FileMeta::new(
        built.file_number,
        built.file_size,
        built.smallest,
        built.largest,
    )
    .unwrap()
}

fn corrupt_byte(path: &Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x80;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

#[derive(Default)]
struct FlushGate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl FlushGate {
    fn wait_until_blocked(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.changed.notify_all();
    }
}

struct BlockingFlushFs {
    gate: Arc<FlushGate>,
}

impl DurableFs for BlockingFlushFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        if path.to_string_lossy().ends_with(".sst.tmp") {
            self.gate.block();
        }
        OsDurableFs.create(path)
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        OsDurableFs.append(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        OsDurableFs.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        OsDurableFs.atomic_replace(source, destination)
    }
}
