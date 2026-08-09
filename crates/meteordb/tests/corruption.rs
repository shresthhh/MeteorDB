use std::fs;
use std::path::{Path, PathBuf};

use meteordb::{
    Durability, Error, Options, TableReader, WalWriter, WriteBatch, inspect_manifest, inspect_wal,
    replay_wal,
};

#[derive(Clone, Copy, Debug)]
enum PersistedClass {
    Current,
    Manifest,
    Wal,
    Sstable,
}

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Flip,
    Truncate,
}

#[test]
fn flips_and_truncations_cover_every_persisted_format_without_panics() {
    let baseline = tempfile::tempdir().unwrap();
    create_database(baseline.path());

    for class in [
        PersistedClass::Current,
        PersistedClass::Manifest,
        PersistedClass::Wal,
        PersistedClass::Sstable,
    ] {
        for mutation in [Mutation::Flip, Mutation::Truncate] {
            let case = tempfile::tempdir().unwrap();
            copy_directory(baseline.path(), case.path());
            let path = persisted_path(case.path(), class);
            mutate(&path, mutation);

            let result = std::panic::catch_unwind(|| exercise(case.path(), &path, class, mutation));
            assert!(
                result.is_ok(),
                "seed=0xc022_0010 panicked for {class:?} {mutation:?}"
            );
        }
    }
}

#[test]
fn every_wal_torn_tail_recovers_the_exact_complete_atomic_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("oracle.wal");
    let mut writer = WalWriter::create(&wal_path, 64 * 1024).unwrap();
    let batches = [
        batch(&[(b"a-1", b"value-a-1"), (b"a-2", b"value-a-2")]),
        batch(&[
            (b"b-1", b"value-b-1"),
            (b"b-2", b"value-b-2"),
            (b"b-3", b"value-b-3"),
        ]),
        batch(&[(b"c-1", b"value-c-1"), (b"c-2", b"value-c-2")]),
    ];
    let mut complete_ends = Vec::new();
    for (index, batch) in batches.iter().enumerate() {
        writer
            .append(
                u64::try_from(index + 1).unwrap(),
                batch,
                Durability::Buffered,
            )
            .unwrap();
        complete_ends.push(std::fs::metadata(&wal_path).unwrap().len());
    }
    drop(writer);
    let complete = std::fs::read(&wal_path).unwrap();

    for truncated_at in 0..=complete.len() {
        let candidate = dir.path().join("candidate.wal");
        std::fs::write(&candidate, &complete[..truncated_at]).unwrap();
        let recovered = replay_wal(&candidate, 64 * 1024).unwrap();
        let expected_len = complete_ends
            .iter()
            .take_while(|&&end| end <= u64::try_from(truncated_at).unwrap())
            .count();
        assert_eq!(
            recovered.len(),
            expected_len,
            "truncation at byte {truncated_at}"
        );
        for (actual, expected) in recovered.iter().zip(&batches) {
            assert_eq!(&actual.batch, expected, "truncation at byte {truncated_at}");
        }
    }
}

fn batch(entries: &[(&[u8], &[u8])]) -> WriteBatch {
    let mut batch = WriteBatch::default();
    for (key, value) in entries {
        batch.put(key, value);
    }
    batch
}

fn create_database(path: &Path) {
    let mut options = Options::new(path);
    options.memtable_bytes = 128;
    options.target_sstable_bytes = 128;
    options.block_bytes = 64;
    let db = meteordb::Engine::open(options).unwrap();
    for generation in 0..5 {
        db.put(
            format!("table-{generation}"),
            vec![b'a' + u8::try_from(generation).unwrap(); 96],
        )
        .unwrap();
        db.flush().unwrap();
    }
    db.put(b"wal-a", b"value-a").unwrap();
    db.put(b"wal-b", b"value-b").unwrap();
    db.sync().unwrap();
    drop(db);
}

fn exercise(directory: &Path, path: &Path, class: PersistedClass, mutation: Mutation) {
    match class {
        PersistedClass::Current | PersistedClass::Manifest => {
            assert_typed_corruption(inspect_manifest(directory).unwrap_err(), class, mutation);
        }
        PersistedClass::Sstable => {
            let error = match TableReader::open(path) {
                Err(error) => error,
                Ok(reader) => reader
                    .iter()
                    .find_map(Result::err)
                    .expect("mutated SSTable unexpectedly decoded without an error"),
            };
            assert_typed_corruption(error, class, mutation);
        }
        PersistedClass::Wal if matches!(mutation, Mutation::Flip) => {
            assert_typed_corruption(
                inspect_wal(path, 64 * 1024 * 1024).unwrap_err(),
                class,
                mutation,
            );
        }
        PersistedClass::Wal => {
            let recovered = replay_wal(path, 64 * 1024 * 1024).unwrap();
            assert!(
                recovered.len() <= 2,
                "torn WAL exposed more than the complete acknowledged prefix"
            );
        }
    }
}

fn assert_typed_corruption(error: Error, class: PersistedClass, mutation: Mutation) {
    assert!(
        matches!(
            error,
            Error::Corruption { .. } | Error::UnsupportedFormat { .. }
        ),
        "seed=0xc022_0010 expected typed corruption for {class:?} {mutation:?}, got {error}"
    );
}

fn persisted_path(directory: &Path, class: PersistedClass) -> PathBuf {
    match class {
        PersistedClass::Current => directory.join("CURRENT"),
        PersistedClass::Manifest => {
            let current = fs::read_to_string(directory.join("CURRENT")).unwrap();
            directory.join(current.trim())
        }
        PersistedClass::Wal => find_extension(directory, "wal"),
        PersistedClass::Sstable => find_extension(directory, "sst"),
    }
}

fn find_extension(directory: &Path, extension: &str) -> PathBuf {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|actual| actual == extension))
        .unwrap()
}

fn mutate(path: &Path, mutation: Mutation) {
    let mut bytes = fs::read(path).unwrap();
    assert!(
        !bytes.is_empty(),
        "{} is unexpectedly empty",
        path.display()
    );
    match mutation {
        Mutation::Flip => {
            let index = bytes.len() / 2;
            bytes[index] ^= 0x80;
        }
        Mutation::Truncate => bytes.truncate(bytes.len() / 2),
    }
    fs::write(path, bytes).unwrap();
}

fn copy_directory(source: &Path, destination: &Path) {
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
    }
}
