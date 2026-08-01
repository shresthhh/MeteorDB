use std::path::Path;
use std::sync::Arc;

use meteordb::{
    Durability, DurableFile, DurableFs, Engine, Error, MemTable, Options, ValueRecord, WriteBatch,
};

fn test_options(path: &Path) -> Options {
    let mut options = Options::new(path);
    options.max_key_bytes = 16;
    options.max_value_bytes = 32;
    options.max_batch_bytes = 64;
    options
}

fn test_engine() -> (tempfile::TempDir, Engine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(test_options(dir.path())).unwrap();
    (dir, engine)
}

#[test]
fn memtable_selects_the_newest_visible_version_and_preserves_zero_bytes() {
    let mut memtable = MemTable::default();
    let mut first = WriteBatch::default();
    first.put(b"a\0z", b"old");
    let mut second = WriteBatch::default();
    second.put(b"a\0z", b"new");

    memtable.apply(1, first).unwrap();
    memtable.apply(2, second).unwrap();

    assert_eq!(
        memtable.get(b"a\0z", 1).unwrap(),
        Some(&ValueRecord::value(b"old", None))
    );
    assert_eq!(
        memtable.get(b"a\0z", 2).unwrap(),
        Some(&ValueRecord::value(b"new", None))
    );
    assert_eq!(memtable.iter().count(), 2);
}

#[test]
fn later_operations_for_the_same_key_win_within_one_batch() {
    let mut memtable = MemTable::default();
    let mut batch = WriteBatch::default();
    batch.delete(b"k").put(b"k", b"last");

    memtable.apply(7, batch).unwrap();

    assert_eq!(
        memtable.get(b"k", 7).unwrap(),
        Some(&ValueRecord::value(b"last", None))
    );
    assert_eq!(memtable.iter().count(), 1);
}

#[test]
fn a_batch_is_visible_at_one_sequence() {
    let (_dir, db) = test_engine();
    let before = db.snapshot().unwrap();
    let mut batch = WriteBatch::default();
    batch.put(b"a", b"1").put(b"b", b"2");

    db.write(batch).unwrap();

    assert_eq!(before.get(b"a").unwrap(), None);
    assert_eq!(before.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
}

#[test]
fn snapshots_keep_their_old_version_after_overwrite_and_delete() {
    let (_dir, db) = test_engine();
    db.put(b"k", b"old").unwrap();
    let old = db.snapshot().unwrap();

    db.put(b"k", b"new").unwrap();
    let new = db.snapshot().unwrap();
    db.delete(b"k").unwrap();

    assert_eq!(old.get(b"k").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(new.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(db.get(b"k").unwrap(), None);
}

struct FailingFile;

impl DurableFile for FailingFile {
    fn write_all(&mut self, _bytes: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("injected WAL append failure"))
    }

    fn sync_all(&self) -> std::io::Result<()> {
        Ok(())
    }
}

struct FailingWalFs;

impl DurableFs for FailingWalFs {
    fn create(&self, _path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(FailingFile))
    }

    fn append(&self, _path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(FailingFile))
    }

    fn sync_directory(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    fn atomic_replace(&self, _source: &Path, _destination: &Path) -> std::io::Result<()> {
        unreachable!("the Task 4 engine does not replace files")
    }
}

#[test]
fn wal_failure_publishes_neither_operation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_fs(test_options(dir.path()), Arc::new(FailingWalFs)).unwrap();
    let before = db.snapshot().unwrap();
    let mut batch = WriteBatch::default();
    batch.put(b"a", b"1").put(b"b", b"2");

    assert!(matches!(
        db.write(batch),
        Err(Error::Io {
            operation: "append WAL",
            ..
        })
    ));

    assert_eq!(before.get(b"a").unwrap(), None);
    assert_eq!(before.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap(), None);
}

#[test]
fn validates_batches_before_appending_to_the_wal() {
    let (_dir, db) = test_engine();
    let mut oversized_key = WriteBatch::default();
    oversized_key.put([b'k'; 17], b"value");

    assert!(matches!(
        db.write(WriteBatch::default()),
        Err(Error::InvalidArgument(message)) if message.contains("empty")
    ));
    assert!(matches!(
        db.write(oversized_key),
        Err(Error::InvalidArgument(message)) if message.contains("max_key_bytes")
    ));
}

#[test]
fn explicit_sync_supports_buffered_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = test_options(dir.path());
    options.durability = Durability::Buffered;
    let db = Engine::open(options).unwrap();

    db.put(b"k", b"v").unwrap();
    db.sync().unwrap();

    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn close_is_idempotent_and_rejects_later_operations() {
    let (_dir, db) = test_engine();
    let snapshot = db.snapshot().unwrap();

    db.close().unwrap();
    db.close().unwrap();

    assert!(matches!(db.get(b"k"), Err(Error::Closed)));
    assert!(matches!(db.put(b"k", b"v"), Err(Error::Closed)));
    assert!(matches!(db.delete(b"k"), Err(Error::Closed)));
    assert!(matches!(
        db.write(WriteBatch::default()),
        Err(Error::Closed)
    ));
    assert!(matches!(db.snapshot(), Err(Error::Closed)));
    assert!(matches!(db.sync(), Err(Error::Closed)));
    assert!(matches!(snapshot.get(b"k"), Err(Error::Closed)));
}
