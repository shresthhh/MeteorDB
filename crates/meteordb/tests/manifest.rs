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
            "replace MANIFEST-000001.tmp -> MANIFEST-000001",
            "sync directory",
            "create CURRENT.tmp",
            "write CURRENT.tmp",
            "sync CURRENT.tmp",
            "replace CURRENT.tmp -> CURRENT",
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
}

fn file_name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}
