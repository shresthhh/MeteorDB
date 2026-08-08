use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use meteordb::{
    CompactionJob, CompactionPicker, DurableFile, DurableFs, Engine, FileMeta, InternalKey,
    Options, OsDurableFs, TableBuilder, ValueKind, VersionEdit, VersionSet,
};

fn build_table(
    directory: &Path,
    number: u64,
    entries: &[(&[u8], u64, ValueKind, &[u8])],
) -> FileMeta {
    let path = directory.join(format!("{number:06}.sst"));
    let mut builder = TableBuilder::create(&path, number, 64, 2, 8, Default::default()).unwrap();
    for (key, sequence, kind, value) in entries {
        let key = InternalKey::try_new(*key, *sequence, *kind).unwrap();
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

#[test]
fn picker_scores_l0_by_count_and_expands_next_level_overlaps() {
    let dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let mut edit = VersionEdit::new();
    for (number, key) in [
        (2, b"a".as_slice()),
        (3, b"c"),
        (4, b"e"),
        (5, b"g"),
        (6, b"i"),
    ] {
        edit.add_file(
            0,
            build_table(dir.path(), number, &[(key, 1, ValueKind::Value, b"value")]),
        );
    }
    edit.add_file(
        1,
        build_table(
            dir.path(),
            7,
            &[
                (b"b", 1, ValueKind::Value, b"b"),
                (b"h", 1, ValueKind::Value, b"h"),
            ],
        ),
    )
    .set_next_file_number(8)
    .set_last_sequence(1);
    versions.apply(edit).unwrap();

    let plan = CompactionPicker::new(4, 1_000_000)
        .pick(&versions.current())
        .expect("five L0 files exceed the trigger");
    assert_eq!(plan.input_level(), 0);
    assert_eq!(plan.output_level(), 1);
    assert_eq!(plan.input_files().len(), 5);
    assert_eq!(plan.overlap_files().len(), 1);
    assert!(plan.score() > 1.0);
}

#[test]
fn picker_chooses_the_highest_overfull_level_score() {
    let dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let mut edit = VersionEdit::new();
    for number in 2..=6 {
        edit.add_file(
            0,
            build_table(
                dir.path(),
                number,
                &[(format!("l0-{number}").as_bytes(), 1, ValueKind::Value, b"x")],
            ),
        );
    }
    edit.add_file(
        2,
        build_table(
            dir.path(),
            7,
            &[(b"level-two", 1, ValueKind::Value, &[b'x'; 256])],
        ),
    )
    .set_next_file_number(8)
    .set_last_sequence(1);
    versions.apply(edit).unwrap();

    let plan = CompactionPicker::new(4, 1)
        .pick(&versions.current())
        .unwrap();
    assert_eq!(plan.input_level(), 2);
    assert!(plan.score() > 1.25);
}

#[test]
fn compact_merges_versions_preserves_snapshot_history_and_splits_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path());
    options.target_sstable_bytes = 96;
    options.block_bytes = 64;
    let db = Engine::open(options).unwrap();

    for index in 0..5 {
        db.put(b"history", format!("value-{index}")).unwrap();
        db.put(
            format!("key-{index:02}"),
            vec![b'x' + u8::try_from(index).unwrap(); 80],
        )
        .unwrap();
        db.flush().unwrap();
    }
    let snapshot = db.snapshot().unwrap();
    db.put(b"history", b"newest").unwrap();
    db.flush().unwrap();

    assert!(db.compact().unwrap());
    assert_eq!(
        snapshot.get(b"history").unwrap().as_deref(),
        Some(&b"value-4"[..])
    );
    assert_eq!(db.get(b"history").unwrap().as_deref(), Some(&b"newest"[..]));

    let sstables = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            (path.extension().is_some_and(|extension| extension == "sst")).then_some(path)
        })
        .collect::<Vec<_>>();
    assert!(sstables.len() >= 2, "small target should split outputs");
}

#[test]
fn old_input_files_live_until_a_scan_releases_its_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path());
    options.target_sstable_bytes = 64;
    let db = Engine::open(options).unwrap();
    for index in 0..5 {
        db.put(format!("key-{index}"), b"value").unwrap();
        db.flush().unwrap();
    }
    let old_files = sstable_numbers(dir.path());
    let reader = db.scan(meteordb::ScanBounds::all(), usize::MAX).unwrap();

    assert!(db.compact().unwrap());
    for number in &old_files {
        assert!(dir.path().join(format!("{number:06}.sst")).exists());
    }

    drop(reader);
    let _ = db.compact().unwrap();
    for number in old_files {
        assert!(!dir.path().join(format!("{number:06}.sst")).exists());
    }
}

#[test]
fn an_older_version_protects_files_that_survive_one_compaction_then_retire() {
    let dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let protected = build_table(dir.path(), 2, &[(b"a", 1, ValueKind::Value, b"protected")]);
    let protected_number = protected.number();
    let mut edit = VersionEdit::new();
    edit.add_file(1, protected);
    for number in 3..=7 {
        edit.add_file(
            0,
            build_table(
                dir.path(),
                number,
                &[(
                    format!("x-{number}").as_bytes(),
                    1,
                    ValueKind::Value,
                    &[b'x'; 600],
                )],
            ),
        );
    }
    edit.set_next_file_number(8).set_last_sequence(1);
    versions.apply(edit).unwrap();
    drop(versions);

    let mut options = Options::new(dir.path());
    options.target_sstable_bytes = 500;
    let db = Engine::open(options).unwrap();
    let reader = db.scan(meteordb::ScanBounds::all(), usize::MAX).unwrap();

    assert!(db.compact().unwrap());
    assert!(db.compact().unwrap());
    assert!(
        dir.path()
            .join(format!("{protected_number:06}.sst"))
            .exists(),
        "the reader's older version still references this file"
    );

    drop(reader);
    let _ = db.compact().unwrap();
    assert!(
        !dir.path()
            .join(format!("{protected_number:06}.sst"))
            .exists()
    );
}

#[test]
fn bottom_level_compaction_drops_an_unneeded_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let tombstone = build_table(dir.path(), 2, &[(b"gone", 2, ValueKind::Deletion, b"")]);
    let mut edit = VersionEdit::new();
    edit.add_file(5, tombstone)
        .set_next_file_number(3)
        .set_last_sequence(2);
    versions.apply(edit).unwrap();
    let plan = CompactionPicker::new(4, 1)
        .pick(&versions.current())
        .expect("the tiny target makes level five overfull");
    assert_eq!(plan.output_level(), 6);
    drop(versions);

    let db = Engine::open(Options::new(dir.path())).unwrap();
    CompactionJob::new(plan).execute(&db).unwrap();

    assert_eq!(db.get(b"gone").unwrap(), None);
    assert!(sstable_numbers(dir.path()).is_empty());
}

#[test]
fn compaction_syncs_outputs_before_manifest_install_and_reclaims_inputs_afterward() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(TrackingFs::default());
    let mut options = Options::new(dir.path());
    options.target_sstable_bytes = 128;
    let db = Engine::open_with_fs(options, fs.clone()).unwrap();
    for index in 0..5 {
        db.put(format!("key-{index}"), b"value").unwrap();
        db.flush().unwrap();
    }
    fs.clear();

    assert!(db.compact().unwrap());

    let events = fs.events();
    let output_sync = events
        .iter()
        .position(|event| event.starts_with("sync ") && event.ends_with(".sst.tmp"))
        .expect("output temporary file must be synchronized");
    let published_sync = events
        .iter()
        .position(|event| event.starts_with("sync ") && event.ends_with(".sst"))
        .expect("published output must be synchronized");
    let manifest_sync = events
        .iter()
        .position(|event| event.starts_with("sync MANIFEST-"))
        .expect("manifest edit must be synchronized");
    let first_remove = events
        .iter()
        .position(|event| event.starts_with("remove ") && event.ends_with(".sst"))
        .expect("obsolete inputs must be reclaimed");
    assert!(output_sync < published_sync);
    assert!(published_sync < manifest_sync);
    assert!(manifest_sync < first_remove, "{events:?}");
}

#[test]
fn failed_output_sync_removes_temporary_sstables_and_is_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FailingCompactionFs::default());
    let db = compaction_database(dir.path(), fs.clone());
    let original_files = sstable_numbers(dir.path());
    fs.fail_next_temp_sync();

    let stored = db.compact().unwrap_err().to_string();
    assert_eq!(sstable_numbers(dir.path()), original_files);
    assert!(temporary_sstable_numbers(dir.path()).is_empty());

    assert_eq!(db.compact().unwrap_err().to_string(), stored);
}

#[test]
fn failed_manifest_file_sync_removes_unpublished_outputs_and_is_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FailingCompactionFs::default());
    let db = compaction_database(dir.path(), fs.clone());
    let original_files = sstable_numbers(dir.path());
    fs.fail_next_manifest_sstable_sync();

    let stored = db.compact().unwrap_err().to_string();
    assert_eq!(sstable_numbers(dir.path()), original_files);
    assert!(temporary_sstable_numbers(dir.path()).is_empty());

    assert_eq!(db.compact().unwrap_err().to_string(), stored);
}

#[test]
fn partial_atomic_install_removes_unpublished_destination_and_is_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FailingCompactionFs::default());
    let db = compaction_database(dir.path(), fs.clone());
    let original_files = sstable_numbers(dir.path());
    fs.fail_next_atomic_install_after_link();

    let stored = db.compact().unwrap_err().to_string();
    assert_eq!(sstable_numbers(dir.path()), original_files);
    assert!(temporary_sstable_numbers(dir.path()).is_empty());

    assert_eq!(db.compact().unwrap_err().to_string(), stored);
}

#[test]
fn uncertain_manifest_sync_preserves_outputs_that_recovery_may_reference() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(FailingCompactionFs::default());
    let db = compaction_database(dir.path(), fs.clone());
    let original_files = sstable_numbers(dir.path());
    fs.fail_next_manifest_sync();

    assert!(db.compact().is_err());
    let remaining_files = sstable_numbers(dir.path());
    assert!(
        remaining_files.len() > original_files.len(),
        "installed outputs must remain when the manifest edit may be recoverable"
    );
    assert!(
        original_files
            .iter()
            .all(|number| remaining_files.contains(number))
    );
    assert!(temporary_sstable_numbers(dir.path()).is_empty());
}

fn compaction_database(directory: &Path, fs: Arc<dyn DurableFs>) -> Engine {
    let mut options = Options::new(directory);
    options.target_sstable_bytes = 128;
    let db = Engine::open_with_fs(options, fs).unwrap();
    for index in 0..5 {
        db.put(format!("key-{index}"), vec![b'x'; 80]).unwrap();
        db.flush().unwrap();
    }
    db
}

fn sstable_numbers(directory: &Path) -> Vec<u64> {
    let mut numbers = std::fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_str()?;
            name.strip_suffix(".sst")?.parse().ok()
        })
        .collect::<Vec<_>>();
    numbers.sort_unstable();
    numbers
}

fn temporary_sstable_numbers(directory: &Path) -> Vec<u64> {
    std::fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_str()?;
            name.strip_suffix(".sst.tmp")?.parse().ok()
        })
        .collect()
}

#[derive(Default)]
struct TrackingFs {
    inner: OsDurableFs,
    events: Arc<Mutex<Vec<String>>>,
}

impl TrackingFs {
    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }

    fn tracked(&self, file: Box<dyn DurableFile>, path: &Path) -> Box<dyn DurableFile> {
        Box::new(TrackingFile {
            inner: file,
            name: path.file_name().unwrap().to_string_lossy().into_owned(),
            events: self.events.clone(),
        })
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
        Ok(self.tracked(self.inner.create(path)?, path))
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(self.tracked(self.inner.append(path)?, path))
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(self.tracked(self.inner.append_existing(path)?, path))
    }

    fn sync_file(&self, path: &Path) -> std::io::Result<()> {
        self.events.lock().unwrap().push(format!(
            "sync {}",
            path.file_name().unwrap().to_string_lossy()
        ));
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
        self.events.lock().unwrap().push(format!(
            "remove {}",
            path.file_name().unwrap().to_string_lossy()
        ));
        self.inner.remove_file(path)
    }
}

#[derive(Default)]
struct FailingCompactionFs {
    inner: OsDurableFs,
    fail_temp_sync: Arc<AtomicBool>,
    fail_atomic_install_after_link: AtomicBool,
    fail_manifest_sstable_sync: AtomicBool,
    fail_manifest_sync: Arc<AtomicBool>,
}

impl FailingCompactionFs {
    fn fail_next_temp_sync(&self) {
        self.fail_temp_sync.store(true, Ordering::Release);
    }

    fn fail_next_manifest_sstable_sync(&self) {
        self.fail_manifest_sstable_sync
            .store(true, Ordering::Release);
    }

    fn fail_next_atomic_install_after_link(&self) {
        self.fail_atomic_install_after_link
            .store(true, Ordering::Release);
    }

    fn fail_next_manifest_sync(&self) {
        self.fail_manifest_sync.store(true, Ordering::Release);
    }
}

struct FailingCompactionFile {
    inner: Box<dyn DurableFile>,
    fail_sync: Arc<AtomicBool>,
}

impl DurableFile for FailingCompactionFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        if self.fail_sync.swap(false, Ordering::AcqRel) {
            return Err(std::io::Error::other(
                "injected compacted SSTable sync failure",
            ));
        }
        self.inner.sync_all()
    }
}

impl DurableFs for FailingCompactionFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let file = self.inner.create(path)?;
        if path.to_string_lossy().ends_with(".sst.tmp") {
            return Ok(Box::new(FailingCompactionFile {
                inner: file,
                fail_sync: self.fail_temp_sync.clone(),
            }));
        }
        Ok(file)
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.inner.append(path)
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let file = self.inner.append_existing(path)?;
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("MANIFEST-"))
        {
            return Ok(Box::new(FailingCompactionFile {
                inner: file,
                fail_sync: self.fail_manifest_sync.clone(),
            }));
        }
        Ok(file)
    }

    fn sync_file(&self, path: &Path) -> std::io::Result<()> {
        if path.extension().is_some_and(|extension| extension == "sst")
            && self
                .fail_manifest_sstable_sync
                .swap(false, Ordering::AcqRel)
        {
            return Err(std::io::Error::other(
                "injected manifest SSTable sync failure",
            ));
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
        if self
            .fail_atomic_install_after_link
            .swap(false, Ordering::AcqRel)
        {
            std::fs::hard_link(source, destination)?;
            return Err(std::io::Error::other(
                "injected temporary removal failure after destination creation",
            ));
        }
        self.inner.atomic_install(source, destination)
    }

    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.inner.remove_file(path)
    }
}
