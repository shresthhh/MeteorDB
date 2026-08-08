use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

use meteordb::{
    BlockCache, BlockKind, CachePartition, Compression, DurableFile, DurableFs, Engine, Error,
    FileMeta, InternalKey, Options, OsDurableFs, SSTABLE_FORMAT_VERSION, TableBuilder, ValueKind,
    VersionEdit, VersionSet,
};

#[test]
fn cache_partitions_reserve_independent_twenty_eighty_budgets() {
    let cache = BlockCache::new(100).unwrap();

    assert_eq!(cache.capacity_bytes(CachePartition::Metadata), 20);
    assert_eq!(cache.capacity_bytes(CachePartition::Data), 80);

    cache
        .insert(1, 10, BlockKind::Index, Arc::from([1_u8; 15]))
        .unwrap();
    cache
        .insert(1, 20, BlockKind::Data, Arc::from([2_u8; 70]))
        .unwrap();
    cache
        .insert(2, 30, BlockKind::Filter, Arc::from([3_u8; 10]))
        .unwrap();

    assert!(cache.get(1, 10, BlockKind::Index).unwrap().is_none());
    assert!(cache.get(1, 20, BlockKind::Data).unwrap().is_some());
    assert!(cache.get(2, 30, BlockKind::Filter).unwrap().is_some());
    assert_eq!(cache.usage_bytes(CachePartition::Metadata), 10);
    assert_eq!(cache.usage_bytes(CachePartition::Data), 70);
}

#[test]
fn cache_hits_refresh_deterministic_lru_recency() {
    let cache = BlockCache::new(10).unwrap();
    cache
        .insert(1, 10, BlockKind::Data, Arc::from([1_u8; 4]))
        .unwrap();
    cache
        .insert(1, 20, BlockKind::Data, Arc::from([2_u8; 4]))
        .unwrap();

    assert!(cache.get(1, 10, BlockKind::Data).unwrap().is_some());
    cache
        .insert(1, 30, BlockKind::Data, Arc::from([3_u8; 4]))
        .unwrap();

    assert!(cache.get(1, 10, BlockKind::Data).unwrap().is_some());
    assert!(cache.get(1, 20, BlockKind::Data).unwrap().is_none());
    assert!(cache.get(1, 30, BlockKind::Data).unwrap().is_some());
}

#[test]
fn cache_statistics_describe_hits_misses_admissions_and_evictions() {
    let cache = BlockCache::new(10).unwrap();
    assert!(cache.get(1, 10, BlockKind::Data).unwrap().is_none());
    cache
        .insert(1, 10, BlockKind::Data, Arc::from([1_u8; 8]))
        .unwrap();
    assert!(cache.get(1, 10, BlockKind::Data).unwrap().is_some());
    cache
        .insert(1, 20, BlockKind::Data, Arc::from([2_u8; 8]))
        .unwrap();

    let snapshot = cache.snapshot();
    assert_eq!(snapshot.data.hits, 1);
    assert_eq!(snapshot.data.misses, 1);
    assert_eq!(snapshot.data.admissions, 2);
    assert_eq!(snapshot.data.evictions, 1);
    assert_eq!(snapshot.data.capacity_bytes, 8);
    assert_eq!(snapshot.data.usage_bytes, 8);
}

#[test]
fn engine_statistics_start_with_configured_cache_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path());
    options.block_cache_bytes = 100;
    let db = Engine::open(options).unwrap();

    let snapshot = db.stats();
    assert_eq!(snapshot.cache.metadata.capacity_bytes, 20);
    assert_eq!(snapshot.cache.data.capacity_bytes, 80);
    assert_eq!(snapshot.bloom_checks, 0);
    assert_eq!(snapshot.bloom_useful_negatives, 0);
    assert_eq!(snapshot.point_reads, 0);
    assert_eq!(snapshot.sstable_probes, 0);
    assert!(snapshot.level_table_probes.iter().all(|&count| count == 0));
}

#[test]
fn statistics_report_point_read_amplification() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"k", b"value").unwrap();
    db.flush().unwrap();

    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"value"[..]));
    assert_eq!(db.get(b"outside").unwrap(), None);

    let stats = db.stats();
    assert_eq!(stats.point_reads, 2);
    assert_eq!(stats.sstable_probes, 1);
    assert_eq!(stats.read_amplification(), 0.5);
}

#[test]
fn disk_point_reads_preserve_snapshots_and_use_the_data_cache() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path());
    options.block_bytes = 64;
    options.block_cache_bytes = 4096;
    let db = Engine::open(options).unwrap();
    db.put(b"k", b"old").unwrap();
    let snapshot = db.snapshot().unwrap();
    db.flush().unwrap();
    db.put(b"k", b"new").unwrap();

    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(snapshot.get(b"k").unwrap().as_deref(), Some(&b"old"[..]));
    let after_first = db.stats();
    assert_eq!(snapshot.get(b"k").unwrap().as_deref(), Some(&b"old"[..]));
    let after_second = db.stats();
    assert!(after_second.cache.data.hits > after_first.cache.data.hits);
    assert!(after_second.cache.metadata.hits > after_first.cache.metadata.hits);
}

#[test]
fn mutable_memory_is_checked_before_newest_immutable_memory() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(FlushGate::default());
    let fs = Arc::new(BlockingFlushFs { gate: gate.clone() });
    let mut options = Options::new(dir.path());
    options.memtable_bytes = 50;
    let db = Engine::open_with_fs(options, fs).unwrap();
    let old = vec![b'x'; 100];
    db.put(b"k", &old).unwrap();
    gate.wait_until_blocked();

    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(old.as_slice()));
    db.put(b"k", b"new").unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));

    gate.release();
    db.flush().unwrap();
}

#[test]
fn bloom_useful_negative_avoids_a_data_block_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path());
    options.block_cache_bytes = 4096;
    let db = Engine::open(options).unwrap();
    db.put(b"a", b"first").unwrap();
    db.put(b"z", b"last").unwrap();
    db.flush().unwrap();
    let before = db.stats();

    assert_eq!(db.get(b"middle").unwrap(), None);

    let after = db.stats();
    assert!(after.bloom_checks > before.bloom_checks);
    assert!(after.bloom_useful_negatives > before.bloom_useful_negatives);
    assert_eq!(after.cache.data.misses, before.cache.data.misses);
}

#[test]
fn overlapping_l0_files_are_searched_newest_first_with_mvcc_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(Options::new(dir.path())).unwrap();
    db.put(b"k", b"old").unwrap();
    db.flush().unwrap();
    let snapshot = db.snapshot().unwrap();
    db.put(b"k", b"new").unwrap();
    db.flush().unwrap();

    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
    let before_snapshot = db.stats();
    assert_eq!(snapshot.get(b"k").unwrap().as_deref(), Some(&b"old"[..]));
    let after_snapshot = db.stats();
    assert_eq!(
        after_snapshot.level_table_probes[0] - before_snapshot.level_table_probes[0],
        2
    );

    db.delete(b"k").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn non_overlapping_lower_level_probes_only_one_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let mut edit = VersionEdit::new();
    for (number, entries) in [
        (
            2,
            vec![
                (b"a".as_slice(), 3, b"left".as_slice()),
                (b"b", 3, b"left-end"),
            ],
        ),
        (
            3,
            vec![
                (b"m".as_slice(), 2, b"middle".as_slice()),
                (b"n", 2, b"middle-end"),
            ],
        ),
        (
            4,
            vec![
                (b"y".as_slice(), 1, b"right".as_slice()),
                (b"z", 1, b"right-end"),
            ],
        ),
    ] {
        let file = build_table(dir.path(), number, &entries);
        edit.add_file(1, file);
    }
    edit.set_next_file_number(5).set_last_sequence(3);
    versions.apply(edit).unwrap();
    drop(versions);

    let db = Engine::open(Options::new(dir.path())).unwrap();
    let before = db.stats();
    assert_eq!(db.get(b"m").unwrap().as_deref(), Some(&b"middle"[..]));
    let after = db.stats();
    assert_eq!(
        after.level_table_probes[1] - before.level_table_probes[1],
        1
    );
}

#[test]
fn selected_data_block_checksum_corruption_reaches_the_caller() {
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

    assert!(matches!(db.get(b"k"), Err(Error::Corruption { .. })));
}

#[test]
fn maximum_configured_value_remains_readable_after_flush() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path());
    options.max_key_bytes = 64;
    options.max_value_bytes = 1024;
    options.max_batch_bytes = 2048;
    options.block_bytes = 64;
    let db = Engine::open(options).unwrap();
    let value = vec![7_u8; 1024];
    db.put(b"k", &value).unwrap();
    db.flush().unwrap();

    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(value.as_slice()));
}

#[test]
fn user_key_bloom_semantics_have_a_distinct_sstable_format_version() {
    assert_eq!(SSTABLE_FORMAT_VERSION, 2);
}

#[test]
fn legacy_v1_tables_skip_unsafe_user_key_bloom_negatives() {
    let dir = tempfile::tempdir().unwrap();
    let file = build_table(dir.path(), 2, &[(b"k".as_slice(), 1, b"legacy".as_slice())]);
    let path = dir.path().join("000002.sst");
    let version_offset = file.file_size() - 12;
    write_at(&path, version_offset, &1_u32.to_le_bytes());
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let mut edit = VersionEdit::new();
    edit.add_file(1, file)
        .set_next_file_number(3)
        .set_last_sequence(1);
    versions.apply(edit).unwrap();
    drop(versions);

    let db = Engine::open(Options::new(dir.path())).unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"legacy"[..]));
    assert_eq!(db.stats().bloom_checks, 0);
}

fn build_table(path: &Path, number: u64, entries: &[(&[u8], u64, &[u8])]) -> FileMeta {
    let table_path = path.join(format!("{number:06}.sst"));
    let mut builder =
        TableBuilder::create(&table_path, number, 64, 2, 10, Compression::None).unwrap();
    let mut keys = entries
        .iter()
        .map(|(key, sequence, value)| {
            (
                InternalKey::try_new(key, *sequence, ValueKind::Value).unwrap(),
                *value,
            )
        })
        .collect::<Vec<_>>();
    keys.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in keys {
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

fn write_at(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(bytes).unwrap();
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
