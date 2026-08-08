use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use meteordb::{
    DurableFile, DurableFs, Error, FileMeta, InternalKey, OsDurableFs, VersionEdit, VersionSet,
};

fn meta(number: u64, smallest: &[u8], largest: &[u8]) -> FileMeta {
    FileMeta::new(
        number,
        100,
        InternalKey::value(smallest, 7),
        InternalKey::value(largest, 1),
    )
    .unwrap()
}

fn create_sstable(dir: &Path, number: u64) {
    let path = dir.join(format!("{number:06}.sst"));
    let file = std::fs::File::create(path).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn edits_publish_copy_on_write_versions_and_l0_may_overlap() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    create_sstable(dir.path(), 3);
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let empty = versions.current();

    let mut add = VersionEdit::new();
    add.add_file(0, meta(2, b"a", b"m"));
    add.add_file(0, meta(3, b"h", b"z"));
    add.set_next_file_number(4);
    add.set_last_sequence(9);
    versions.apply(add).unwrap();

    let with_files = versions.current();
    assert!(empty.files(0).is_empty());
    assert_eq!(
        with_files
            .files(0)
            .iter()
            .map(|file| file.number())
            .collect::<Vec<_>>(),
        [3, 2]
    );

    let mut delete = VersionEdit::new();
    delete.delete_file(0, 2);
    versions.apply(delete).unwrap();
    assert_eq!(with_files.files(0).len(), 2);
    assert_eq!(versions.current().files(0)[0].number(), 3);
}

#[test]
fn wal_recovery_counters_round_trip_and_cannot_move_backward() {
    let dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let mut edit = VersionEdit::new();
    edit.set_next_file_number(8)
        .set_log_number(2)
        .set_active_log_number(7)
        .set_wal_sequence(19);
    versions.apply(edit).unwrap();
    drop(versions);

    let mut recovered = VersionSet::recover(dir.path()).unwrap();
    assert_eq!(recovered.log_number(), 2);
    assert_eq!(recovered.active_log_number(), 7);
    assert_eq!(recovered.wal_sequence(), 19);

    let mut regression = VersionEdit::new();
    regression.set_log_number(1);
    assert!(matches!(
        recovered.apply(regression),
        Err(Error::InvalidArgument(message)) if message.contains("log number moved backward")
    ));
}

#[test]
fn levels_one_and_above_reject_overlapping_user_key_ranges() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    create_sstable(dir.path(), 3);
    let mut versions = VersionSet::create(dir.path()).unwrap();

    let mut first = VersionEdit::new();
    first.add_file(1, meta(2, b"a", b"m"));
    first.set_next_file_number(4);
    versions.apply(first).unwrap();
    let before = versions.current();

    let mut overlap = VersionEdit::new();
    overlap.add_file(1, meta(3, b"m", b"z"));
    assert!(matches!(
        versions.apply(overlap),
        Err(Error::InvalidArgument(message)) if message.contains("overlap")
    ));
    assert!(Arc::ptr_eq(&before, &versions.current()));
}

#[test]
fn create_replaces_current_only_after_manifest_and_directory_are_durable() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(TrackingFs::default());

    let versions = VersionSet::create_with_fs(dir.path(), fs.clone()).unwrap();

    assert_eq!(versions.manifest_number(), 1);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("CURRENT")).unwrap(),
        "MANIFEST-000001\n"
    );
    assert_eq!(
        fs.events(),
        [
            "create MANIFEST-000001.tmp",
            "write MANIFEST-000001.tmp",
            "write MANIFEST-000001.tmp",
            "sync MANIFEST-000001.tmp",
            "install MANIFEST-000001.tmp -> MANIFEST-000001",
            "sync directory",
            "create CURRENT.tmp",
            "write CURRENT.tmp",
            "sync CURRENT.tmp",
            "install CURRENT.tmp -> CURRENT",
            "sync directory",
            "append MANIFEST-000001",
        ]
    );
}

#[test]
fn create_does_not_replace_an_existing_database() {
    let dir = tempfile::tempdir().unwrap();
    let versions = VersionSet::create(dir.path()).unwrap();
    drop(versions);
    let current_path = dir.path().join("CURRENT");
    let manifest_path = dir.path().join("MANIFEST-000001");
    let current_before = std::fs::read(&current_path).unwrap();
    let manifest_before = std::fs::read(&manifest_path).unwrap();

    assert!(matches!(
        VersionSet::create(dir.path()),
        Err(Error::InvalidArgument(message)) if message.contains("already exists")
    ));
    assert_eq!(std::fs::read(current_path).unwrap(), current_before);
    assert_eq!(std::fs::read(manifest_path).unwrap(), manifest_before);
}

#[cfg(unix)]
#[test]
fn create_rejects_a_dangling_current_symlink_without_replacing_it() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let current_path = dir.path().join("CURRENT");
    let target = Path::new("missing-current-target");
    symlink(target, &current_path).unwrap();

    assert!(matches!(
        VersionSet::create(dir.path()),
        Err(Error::InvalidArgument(message)) if message.contains("already exists")
    ));
    assert!(
        std::fs::symlink_metadata(&current_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read_link(&current_path).unwrap(), target);
    assert!(!dir.path().join(target).exists());
}

#[cfg(unix)]
#[test]
fn create_rejects_a_dangling_initial_manifest_symlink_without_replacing_it() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let manifest_path = dir.path().join("MANIFEST-000001");
    let target = Path::new("missing-manifest-target");
    symlink(target, &manifest_path).unwrap();

    assert!(matches!(
        VersionSet::create(dir.path()),
        Err(Error::InvalidArgument(message)) if message.contains("already exists")
    ));
    assert!(
        std::fs::symlink_metadata(&manifest_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read_link(&manifest_path).unwrap(), target);
    assert!(!dir.path().join(target).exists());
}

#[test]
fn apply_syncs_each_referenced_sstable_then_manifest_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    let fs = Arc::new(TrackingFs::default());
    let mut versions = VersionSet::create_with_fs(dir.path(), fs.clone()).unwrap();
    fs.clear();

    let mut edit = VersionEdit::new();
    edit.add_file(0, meta(2, b"a", b"z"));
    edit.set_next_file_number(3);
    versions.apply(edit).unwrap();

    assert_eq!(
        fs.events(),
        [
            "sync file 000002.sst",
            "write MANIFEST-000001",
            "write MANIFEST-000001",
            "sync MANIFEST-000001",
        ]
    );
    assert_eq!(versions.current().files(0)[0].number(), 2);
}

#[test]
fn referenced_missing_file_fails_before_manifest_append_or_publication() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(TrackingFs::default());
    let mut versions = VersionSet::create_with_fs(dir.path(), fs.clone()).unwrap();
    fs.clear();
    let before = versions.current();

    let mut edit = VersionEdit::new();
    edit.add_file(0, meta(2, b"a", b"z"));
    edit.set_next_file_number(3);
    assert!(matches!(
        versions.apply(edit),
        Err(Error::Io {
            operation: "sync referenced SSTable",
            ..
        })
    ));

    assert_eq!(fs.events(), ["sync file 000002.sst"]);
    assert!(Arc::ptr_eq(&before, &versions.current()));
}

#[test]
fn failed_manifest_sync_does_not_publish_the_candidate_version() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    let fs = Arc::new(TrackingFs::default());
    let mut versions = VersionSet::create_with_fs(dir.path(), fs.clone()).unwrap();
    fs.clear();
    fs.fail_sync_for("MANIFEST-000001");
    let before = versions.current();

    let mut edit = VersionEdit::new();
    edit.add_file(0, meta(2, b"a", b"z"));
    edit.set_next_file_number(3);
    assert!(matches!(
        versions.apply(edit),
        Err(Error::Io {
            operation: "sync manifest edit",
            ..
        })
    ));

    assert!(Arc::ptr_eq(&before, &versions.current()));
}

#[test]
fn recovery_ignores_a_structurally_truncated_final_edit() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    create_sstable(dir.path(), 3);
    let mut versions = VersionSet::create(dir.path()).unwrap();

    let mut first = VersionEdit::new();
    first.add_file(0, meta(2, b"a", b"m"));
    first.set_next_file_number(3);
    first.set_last_sequence(7);
    versions.apply(first).unwrap();

    let mut second = VersionEdit::new();
    second.add_file(0, meta(3, b"n", b"z"));
    second.set_next_file_number(4);
    second.set_last_sequence(8);
    versions.apply(second).unwrap();
    drop(versions);

    let manifest = dir.path().join("MANIFEST-000001");
    let length = std::fs::metadata(&manifest).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&manifest)
        .unwrap()
        .set_len(length - 3)
        .unwrap();

    let recovered = VersionSet::recover(dir.path()).unwrap();
    assert_eq!(recovered.current().files(0)[0].number(), 2);
    assert_eq!(recovered.current().files(0).len(), 1);
    assert_eq!(recovered.next_file_number(), 3);
    assert_eq!(recovered.last_sequence(), 7);
}

#[test]
fn recovery_removes_a_torn_tail_before_appending_later_edits() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    create_sstable(dir.path(), 3);
    create_sstable(dir.path(), 4);
    let mut versions = VersionSet::create(dir.path()).unwrap();

    let mut first = VersionEdit::new();
    first.add_file(0, meta(2, b"a", b"g"));
    first.set_next_file_number(3);
    versions.apply(first).unwrap();
    let mut torn = VersionEdit::new();
    torn.add_file(0, meta(3, b"h", b"m"));
    torn.set_next_file_number(4);
    versions.apply(torn).unwrap();
    drop(versions);

    let manifest = dir.path().join("MANIFEST-000001");
    let length = std::fs::metadata(&manifest).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&manifest)
        .unwrap()
        .set_len(length - 3)
        .unwrap();

    let mut recovered = VersionSet::recover(dir.path()).unwrap();
    let mut later = VersionEdit::new();
    later.add_file(0, meta(4, b"n", b"z"));
    later.set_next_file_number(5);
    recovered.apply(later).unwrap();
    drop(recovered);

    let recovered_again = VersionSet::recover(dir.path()).unwrap();
    assert_eq!(
        recovered_again
            .current()
            .files(0)
            .iter()
            .map(|file| file.number())
            .collect::<Vec<_>>(),
        [4, 2]
    );
}

#[test]
fn recovery_rejects_a_manifest_that_references_a_missing_sstable() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let mut edit = VersionEdit::new();
    edit.add_file(0, meta(2, b"a", b"z"));
    edit.set_next_file_number(3);
    versions.apply(edit).unwrap();
    drop(versions);
    std::fs::remove_file(dir.path().join("000002.sst")).unwrap();

    assert!(matches!(
        VersionSet::recover(dir.path()),
        Err(Error::Corruption {
            context: "manifest",
            detail,
        }) if detail.contains("missing SSTable")
    ));
}

#[test]
fn file_and_sequence_numbers_never_move_backward() {
    let dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(dir.path()).unwrap();

    let mut advance = VersionEdit::new();
    advance.set_next_file_number(10);
    advance.set_last_sequence(20);
    versions.apply(advance).unwrap();

    let mut file_regression = VersionEdit::new();
    file_regression.set_next_file_number(9);
    assert!(matches!(
        versions.apply(file_regression),
        Err(Error::InvalidArgument(message)) if message.contains("next file number")
    ));

    let mut sequence_regression = VersionEdit::new();
    sequence_regression.set_last_sequence(19);
    assert!(matches!(
        versions.apply(sequence_regression),
        Err(Error::InvalidArgument(message)) if message.contains("sequence")
    ));
    assert_eq!(versions.next_file_number(), 10);
    assert_eq!(versions.last_sequence(), 20);

    drop(versions);
    let recovered = VersionSet::recover(dir.path()).unwrap();
    assert_eq!(recovered.next_file_number(), 10);
    assert_eq!(recovered.last_sequence(), 20);
}

#[test]
fn added_file_must_be_below_the_resulting_high_water_mark() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    let mut versions = VersionSet::create(dir.path()).unwrap();

    let mut edit = VersionEdit::new();
    edit.add_file(0, meta(2, b"a", b"z"));
    assert!(matches!(
        versions.apply(edit),
        Err(Error::InvalidArgument(message)) if message.contains("next file number")
    ));
}

#[test]
fn one_edit_cannot_delete_and_readd_or_add_a_number_twice() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    create_sstable(dir.path(), 3);
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let mut initial = VersionEdit::new();
    initial
        .add_file(0, meta(2, b"a", b"m"))
        .set_next_file_number(4);
    versions.apply(initial).unwrap();

    let mut readd = VersionEdit::new();
    readd.delete_file(0, 2).add_file(0, meta(2, b"n", b"z"));
    assert!(matches!(
        versions.apply(readd),
        Err(Error::InvalidArgument(message)) if message.contains("same edit")
    ));

    let mut duplicate = VersionEdit::new();
    duplicate
        .add_file(0, meta(3, b"a", b"m"))
        .add_file(1, meta(3, b"n", b"z"));
    assert!(matches!(
        versions.apply(duplicate),
        Err(Error::InvalidArgument(message)) if message.contains("more than once")
    ));
}

#[test]
fn deleted_file_number_can_never_be_reused() {
    let dir = tempfile::tempdir().unwrap();
    create_sstable(dir.path(), 2);
    let mut versions = VersionSet::create(dir.path()).unwrap();
    let mut add = VersionEdit::new();
    add.add_file(0, meta(2, b"a", b"z")).set_next_file_number(3);
    versions.apply(add).unwrap();
    let mut delete = VersionEdit::new();
    delete.delete_file(0, 2);
    versions.apply(delete).unwrap();

    let mut reuse = VersionEdit::new();
    reuse.add_file(1, meta(2, b"a", b"z"));
    assert!(matches!(
        versions.apply(reuse),
        Err(Error::InvalidArgument(message)) if message.contains("already been used")
    ));
}

#[test]
fn recovery_rejects_historical_high_water_and_reuse_violations() {
    let high_water_dir = tempfile::tempdir().unwrap();
    create_sstable(high_water_dir.path(), 2);
    drop(VersionSet::create(high_water_dir.path()).unwrap());
    append_raw_edit(
        &high_water_dir.path().join("MANIFEST-000001"),
        Some(2),
        &[],
        &[(0, meta(2, b"a", b"z"))],
    );
    assert!(matches!(
        VersionSet::recover(high_water_dir.path()),
        Err(Error::Corruption {
            context: "manifest",
            detail,
        }) if detail.contains("next file number")
    ));

    let reuse_dir = tempfile::tempdir().unwrap();
    create_sstable(reuse_dir.path(), 2);
    let mut versions = VersionSet::create(reuse_dir.path()).unwrap();
    let mut add = VersionEdit::new();
    add.add_file(0, meta(2, b"a", b"z")).set_next_file_number(3);
    versions.apply(add).unwrap();
    let mut delete = VersionEdit::new();
    delete.delete_file(0, 2);
    versions.apply(delete).unwrap();
    drop(versions);
    append_raw_edit(
        &reuse_dir.path().join("MANIFEST-000001"),
        None,
        &[],
        &[(1, meta(2, b"a", b"z"))],
    );
    assert!(matches!(
        VersionSet::recover(reuse_dir.path()),
        Err(Error::Corruption {
            context: "manifest",
            detail,
        }) if detail.contains("already been used")
    ));

    let same_edit_dir = tempfile::tempdir().unwrap();
    create_sstable(same_edit_dir.path(), 2);
    let mut versions = VersionSet::create(same_edit_dir.path()).unwrap();
    let mut add = VersionEdit::new();
    add.add_file(0, meta(2, b"a", b"z")).set_next_file_number(3);
    versions.apply(add).unwrap();
    drop(versions);
    append_raw_edit(
        &same_edit_dir.path().join("MANIFEST-000001"),
        None,
        &[(0, 2)],
        &[(1, meta(2, b"a", b"z"))],
    );
    assert!(matches!(
        VersionSet::recover(same_edit_dir.path()),
        Err(Error::Corruption {
            context: "manifest",
            detail,
        }) if detail.contains("same edit")
    ));

    let regression_dir = tempfile::tempdir().unwrap();
    let mut versions = VersionSet::create(regression_dir.path()).unwrap();
    let mut advance = VersionEdit::new();
    advance.set_next_file_number(10);
    versions.apply(advance).unwrap();
    drop(versions);
    append_raw_edit(
        &regression_dir.path().join("MANIFEST-000001"),
        Some(9),
        &[],
        &[],
    );
    assert!(matches!(
        VersionSet::recover(regression_dir.path()),
        Err(Error::Corruption {
            context: "manifest",
            detail,
        }) if detail.contains("moved backward")
    ));
}

#[test]
fn a_second_manifest_writer_is_rejected_until_the_first_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let first = VersionSet::create(dir.path()).unwrap();
    assert!(matches!(
        VersionSet::recover(dir.path()),
        Err(Error::Locked(path)) if path == dir.path().join("LOCK")
    ));
    drop(first);
    VersionSet::recover(dir.path()).unwrap();
}

#[test]
fn recovery_never_recreates_a_manifest_removed_before_append() {
    let dir = tempfile::tempdir().unwrap();
    drop(VersionSet::create(dir.path()).unwrap());
    let fs = Arc::new(RemoveBeforeAppendFs::default());

    assert!(matches!(
        VersionSet::recover_with_fs(dir.path(), fs),
        Err(Error::Io {
            operation: "open manifest for append",
            ..
        })
    ));
    assert!(!dir.path().join("MANIFEST-000001").exists());
}

#[cfg(unix)]
#[test]
fn recovery_rejects_symlink_current_manifest_and_sstable() {
    use std::os::unix::fs::symlink;

    let current_dir = tempfile::tempdir().unwrap();
    std::fs::write(current_dir.path().join("target"), b"MANIFEST-000001\n").unwrap();
    symlink("target", current_dir.path().join("CURRENT")).unwrap();
    assert!(VersionSet::recover(current_dir.path()).is_err());

    let manifest_dir = tempfile::tempdir().unwrap();
    drop(VersionSet::create(manifest_dir.path()).unwrap());
    std::fs::rename(
        manifest_dir.path().join("MANIFEST-000001"),
        manifest_dir.path().join("manifest-target"),
    )
    .unwrap();
    symlink(
        "manifest-target",
        manifest_dir.path().join("MANIFEST-000001"),
    )
    .unwrap();
    assert!(VersionSet::recover(manifest_dir.path()).is_err());

    let sstable_dir = tempfile::tempdir().unwrap();
    create_sstable(sstable_dir.path(), 2);
    let mut versions = VersionSet::create(sstable_dir.path()).unwrap();
    let mut add = VersionEdit::new();
    add.add_file(0, meta(2, b"a", b"z")).set_next_file_number(3);
    versions.apply(add).unwrap();
    drop(versions);
    std::fs::rename(
        sstable_dir.path().join("000002.sst"),
        sstable_dir.path().join("table-target"),
    )
    .unwrap();
    symlink("table-target", sstable_dir.path().join("000002.sst")).unwrap();
    assert!(VersionSet::recover(sstable_dir.path()).is_err());
}

#[cfg(unix)]
#[test]
fn manifests_and_sstables_must_be_regular_files() {
    let current_dir = tempfile::tempdir().unwrap();
    drop(VersionSet::create(current_dir.path()).unwrap());
    std::fs::remove_file(current_dir.path().join("CURRENT")).unwrap();
    std::fs::create_dir(current_dir.path().join("CURRENT")).unwrap();
    assert!(VersionSet::recover(current_dir.path()).is_err());

    let manifest_dir = tempfile::tempdir().unwrap();
    drop(VersionSet::create(manifest_dir.path()).unwrap());
    std::fs::remove_file(manifest_dir.path().join("MANIFEST-000001")).unwrap();
    std::fs::create_dir(manifest_dir.path().join("MANIFEST-000001")).unwrap();
    assert!(VersionSet::recover(manifest_dir.path()).is_err());

    let sstable_dir = tempfile::tempdir().unwrap();
    create_sstable(sstable_dir.path(), 2);
    let mut versions = VersionSet::create(sstable_dir.path()).unwrap();
    let mut add = VersionEdit::new();
    add.add_file(0, meta(2, b"a", b"z")).set_next_file_number(3);
    versions.apply(add).unwrap();
    drop(versions);
    std::fs::remove_file(sstable_dir.path().join("000002.sst")).unwrap();
    std::fs::create_dir(sstable_dir.path().join("000002.sst")).unwrap();
    assert!(VersionSet::recover(sstable_dir.path()).is_err());

    let special_dir = tempfile::tempdir().unwrap();
    create_sstable(special_dir.path(), 2);
    let mut versions = VersionSet::create(special_dir.path()).unwrap();
    let mut add = VersionEdit::new();
    add.add_file(0, meta(2, b"a", b"z")).set_next_file_number(3);
    versions.apply(add).unwrap();
    drop(versions);
    std::fs::remove_file(special_dir.path().join("000002.sst")).unwrap();
    let _socket =
        std::os::unix::net::UnixListener::bind(special_dir.path().join("000002.sst")).unwrap();
    assert!(VersionSet::recover(special_dir.path()).is_err());
}

#[test]
fn checksum_damage_in_a_complete_manifest_record_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let versions = VersionSet::create(dir.path()).unwrap();
    drop(versions);
    let manifest = dir.path().join("MANIFEST-000001");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&manifest)
        .unwrap();
    file.seek(SeekFrom::Start(7)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();

    assert!(matches!(
        VersionSet::recover(dir.path()),
        Err(Error::Corruption {
            context: "manifest",
            ..
        })
    ));
}

#[derive(Default)]
struct RemoveBeforeAppendFs {
    inner: OsDurableFs,
}

impl DurableFs for RemoveBeforeAppendFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.inner.create(path)
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        self.inner.append(path)
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        std::fs::remove_file(path)?;
        self.inner.append_existing(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_replace(source, destination)
    }
}

struct TrackingFs {
    inner: OsDurableFs,
    events: Arc<Mutex<Vec<String>>>,
    fail_sync_path: Arc<Mutex<Option<String>>>,
}

impl Default for TrackingFs {
    fn default() -> Self {
        Self {
            inner: OsDurableFs,
            events: Arc::default(),
            fail_sync_path: Arc::default(),
        }
    }
}

impl TrackingFs {
    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }

    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    fn fail_sync_for(&self, file_name: &str) {
        *self.fail_sync_path.lock().unwrap() = Some(file_name.to_owned());
    }
}

struct TrackingFile {
    inner: Box<dyn DurableFile>,
    name: String,
    events: Arc<Mutex<Vec<String>>>,
    fail_sync_path: Arc<Mutex<Option<String>>>,
}

impl DurableFile for TrackingFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("write {}", self.name));
        self.inner.write_all(bytes)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("sync {}", self.name));
        if self.fail_sync_path.lock().unwrap().as_deref() == Some(self.name.as_str()) {
            return Err(std::io::Error::other("injected sync failure"));
        }
        self.inner.sync_all()
    }
}

impl DurableFs for TrackingFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let name = file_name(path);
        self.events.lock().unwrap().push(format!("create {name}"));
        Ok(Box::new(TrackingFile {
            inner: self.inner.create(path)?,
            name,
            events: self.events.clone(),
            fail_sync_path: self.fail_sync_path.clone(),
        }))
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let name = file_name(path);
        self.events.lock().unwrap().push(format!("append {name}"));
        Ok(Box::new(TrackingFile {
            inner: self.inner.append(path)?,
            name,
            events: self.events.clone(),
            fail_sync_path: self.fail_sync_path.clone(),
        }))
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let name = file_name(path);
        self.events.lock().unwrap().push(format!("append {name}"));
        Ok(Box::new(TrackingFile {
            inner: self.inner.append_existing(path)?,
            name,
            events: self.events.clone(),
            fail_sync_path: self.fail_sync_path.clone(),
        }))
    }

    fn sync_file(&self, path: &Path) -> std::io::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("sync file {}", file_name(path)));
        self.inner.sync_file(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push("sync directory".to_owned());
        self.inner.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.events.lock().unwrap().push(format!(
            "replace {} -> {}",
            file_name(source),
            file_name(destination)
        ));
        self.inner.atomic_replace(source, destination)
    }

    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.events.lock().unwrap().push(format!(
            "install {} -> {}",
            file_name(source),
            file_name(destination)
        ));
        self.inner.atomic_install(source, destination)
    }
}

fn file_name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}

fn append_raw_edit(
    manifest: &Path,
    next_file_number: Option<u64>,
    deleted: &[(usize, u64)],
    added: &[(usize, FileMeta)],
) {
    let mut edit = vec![1];
    put_optional_u64(&mut edit, next_file_number);
    put_optional_u64(&mut edit, None);
    edit.extend_from_slice(&(deleted.len() as u32).to_le_bytes());
    for &(level, number) in deleted {
        edit.extend_from_slice(&(level as u32).to_le_bytes());
        edit.extend_from_slice(&number.to_le_bytes());
    }
    edit.extend_from_slice(&(added.len() as u32).to_le_bytes());
    for (level, file) in added {
        edit.extend_from_slice(&(*level as u32).to_le_bytes());
        edit.extend_from_slice(&file.number().to_le_bytes());
        edit.extend_from_slice(&file.file_size().to_le_bytes());
        put_bytes(&mut edit, file.smallest().as_bytes());
        put_bytes(&mut edit, file.largest().as_bytes());
    }

    let mut record = Vec::with_capacity(7 + edit.len());
    let checksum = crc32c::crc32c(&[&[1], edit.as_slice()].concat());
    let masked = checksum.rotate_right(15).wrapping_add(0xa282_ead8);
    record.extend_from_slice(&masked.to_le_bytes());
    record.extend_from_slice(&(edit.len() as u16).to_le_bytes());
    record.push(1);
    record.extend_from_slice(&edit);
    let mut file = OpenOptions::new().append(true).open(manifest).unwrap();
    file.write_all(&record).unwrap();
    file.sync_all().unwrap();
}

fn put_optional_u64(encoded: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(&value.to_le_bytes());
        }
        None => encoded.push(0),
    }
}

fn put_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) {
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded.extend_from_slice(bytes);
}
