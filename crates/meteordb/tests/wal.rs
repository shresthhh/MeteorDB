use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use meteordb::{
    Durability, DurableFs, Error, OsDurableFs, WalWriter, WriteBatch, WriteOp, replay_wal,
};

const BLOCK_BYTES: usize = 32 * 1024;
const HEADER_BYTES: usize = 7;

fn masked_crc(fragment_type: u8, payload: &[u8]) -> u32 {
    let mut bytes = Vec::with_capacity(payload.len() + 1);
    bytes.push(fragment_type);
    bytes.extend_from_slice(payload);
    crc32c::crc32c(&bytes)
        .rotate_right(15)
        .wrapping_add(0xa282_ead8)
}

fn batch_with_put(key: &[u8], value: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::default();
    batch.put(key, value);
    batch
}

fn truncate_tail(path: &Path, bytes: u64) {
    let file = OpenOptions::new().write(true).open(path).unwrap();
    let length = file.metadata().unwrap().len();
    file.set_len(length - bytes).unwrap();
}

#[test]
fn replay_returns_only_complete_batches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    let mut wal = WalWriter::create(&path, 128).unwrap();
    wal.append(4, &batch_with_put(b"a", b"1"), Durability::Sync)
        .unwrap();
    wal.append(5, &batch_with_put(b"b", b"2"), Durability::Sync)
        .unwrap();
    drop(wal);
    truncate_tail(&path, 3);

    let recovered = replay_wal(&path).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].sequence, 4);
    assert_eq!(
        recovered[0].batch.operations(),
        [WriteOp::Put {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            expires_at_unix_ms: None,
        }]
    );
}

#[test]
fn torn_header_at_an_exact_block_boundary_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    let value = vec![b'x'; 32_731];
    let mut wal = WalWriter::create(&path, value.len() + 1).unwrap();
    wal.append(4, &batch_with_put(b"k", &value), Durability::Sync)
        .unwrap();
    drop(wal);

    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&[1, 2, 3, 4, 5, 6]).unwrap();
    file.sync_all().unwrap();
    assert_eq!(file.metadata().unwrap().len() as usize, BLOCK_BYTES);

    let recovered = replay_wal(path).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].sequence, 4);
}

#[test]
fn fragmented_batch_round_trips_across_physical_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    let value = vec![b'x'; BLOCK_BYTES * 2];
    let mut batch = WriteBatch::default();
    batch
        .put_with_expiration(b"large", &value, Some(9_876))
        .delete(b"gone");

    let mut wal = WalWriter::create(&path, value.len() + 64).unwrap();
    wal.append(11, &batch, Durability::Sync).unwrap();
    drop(wal);

    let recovered = replay_wal(&path).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].sequence, 11);
    assert_eq!(recovered[0].batch, batch);
    assert!(std::fs::metadata(path).unwrap().len() as usize > BLOCK_BYTES * 2);
}

#[test]
fn checksum_corruption_before_the_final_record_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    let mut wal = WalWriter::create(&path, 128).unwrap();
    wal.append(4, &batch_with_put(b"a", b"1"), Durability::Sync)
        .unwrap();
    wal.append(5, &batch_with_put(b"b", b"2"), Durability::Sync)
        .unwrap();
    drop(wal);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(HEADER_BYTES as u64)).unwrap();
    let mut byte = [0; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x80;
    file.seek(SeekFrom::Start(HEADER_BYTES as u64)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();

    assert!(matches!(
        replay_wal(&path),
        Err(Error::Corruption { context: "WAL", .. })
    ));
}

#[test]
fn empty_and_oversized_batches_are_rejected_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    let mut wal = WalWriter::create(&path, 4).unwrap();

    assert!(matches!(
        wal.append(1, &WriteBatch::default(), Durability::Buffered),
        Err(Error::InvalidArgument(message)) if message.contains("empty")
    ));
    assert!(matches!(
        wal.append(2, &batch_with_put(b"abc", b"de"), Durability::Buffered),
        Err(Error::InvalidArgument(message)) if message.contains("max_batch_bytes")
    ));
    drop(wal);

    assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
}

#[derive(Default)]
struct TrackingFs {
    inner: OsDurableFs,
    syncs: AtomicUsize,
}

impl DurableFs for TrackingFs {
    fn create(&self, path: &Path) -> std::io::Result<File> {
        self.inner.create(path)
    }

    fn append(&self, path: &Path) -> std::io::Result<File> {
        self.inner.append(path)
    }

    fn sync_file(&self, file: &File) -> std::io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync_file(file)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.inner.atomic_replace(source, destination)
    }
}

#[test]
fn buffered_append_waits_for_explicit_sync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    let fs = Arc::new(TrackingFs::default());
    let mut wal = WalWriter::create_with_fs(&path, 128, fs.clone()).unwrap();

    wal.append(
        7,
        &batch_with_put(b"buffered", b"value"),
        Durability::Buffered,
    )
    .unwrap();
    assert_eq!(fs.syncs.load(Ordering::SeqCst), 0);

    wal.sync().unwrap();
    assert_eq!(fs.syncs.load(Ordering::SeqCst), 1);
    drop(wal);
    assert_eq!(replay_wal(path).unwrap()[0].sequence, 7);
}

#[test]
fn sync_append_synchronizes_before_returning() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    let fs = Arc::new(TrackingFs::default());
    let mut wal = WalWriter::create_with_fs(&path, 128, fs.clone()).unwrap();

    wal.append(7, &batch_with_put(b"sync", b"value"), Durability::Sync)
        .unwrap();

    assert_eq!(fs.syncs.load(Ordering::SeqCst), 1);
}

#[test]
fn filesystem_abstraction_atomically_replaces_and_syncs_directory() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("CURRENT.new");
    let destination = dir.path().join("CURRENT");
    std::fs::write(&source, b"MANIFEST-2\n").unwrap();
    std::fs::write(&destination, b"MANIFEST-1\n").unwrap();
    let fs = OsDurableFs;

    fs.atomic_replace(&source, &destination).unwrap();
    fs.sync_directory(dir.path()).unwrap();

    assert_eq!(std::fs::read(destination).unwrap(), b"MANIFEST-2\n");
    assert!(!source.exists());
}

#[test]
fn invalid_fragment_order_before_a_later_record_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    let mut wal = WalWriter::create(&path, BLOCK_BYTES * 2).unwrap();
    wal.append(
        1,
        &batch_with_put(b"large", &vec![b'z'; BLOCK_BYTES]),
        Durability::Sync,
    )
    .unwrap();
    wal.append(2, &batch_with_put(b"last", b"value"), Durability::Sync)
        .unwrap();
    drop(wal);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(6)).unwrap();
    file.write_all(&[3]).unwrap();
    file.seek(SeekFrom::Start(HEADER_BYTES as u64)).unwrap();
    let mut payload = vec![0; BLOCK_BYTES - HEADER_BYTES];
    file.read_exact(&mut payload).unwrap();
    let checksum = masked_crc(3, &payload);
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&checksum.to_le_bytes()).unwrap();
    file.sync_all().unwrap();

    assert!(matches!(
        replay_wal(path),
        Err(Error::Corruption { context: "WAL", .. })
    ));
}

#[test]
fn replay_rejects_unsupported_batch_format_version() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("000001.wal");
    let mut wal = WalWriter::create(&path, 128).unwrap();
    wal.append(1, &batch_with_put(b"k", b"v"), Durability::Sync)
        .unwrap();
    drop(wal);

    let mut bytes = std::fs::read(&path).unwrap();
    bytes[HEADER_BYTES] = 99;
    let length = u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
    let checksum = masked_crc(bytes[6], &bytes[HEADER_BYTES..HEADER_BYTES + length]);
    bytes[..4].copy_from_slice(&checksum.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();

    assert!(matches!(
        replay_wal(path),
        Err(Error::UnsupportedFormat {
            kind: "WAL batch",
            version: 99
        })
    ));
}
