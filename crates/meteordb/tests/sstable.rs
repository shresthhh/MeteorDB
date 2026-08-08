use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use meteordb::{
    BlockHandle, Compression, DurableFile, DurableFs, Error, InternalKey, SSTABLE_FOOTER_BYTES,
    SSTABLE_FORMAT_VERSION, SSTABLE_MAGIC, TableBuilder, TableReader,
};

fn build_table(
    path: &std::path::Path,
    compression: Compression,
) -> (Vec<(InternalKey, Vec<u8>)>, meteordb::TableBuildResult) {
    let entries: Vec<_> = (0..40)
        .map(|number| {
            (
                InternalKey::value(format!("key-{number:03}"), 7),
                format!("value-{number:03}-{}", "x".repeat(24)).into_bytes(),
            )
        })
        .collect();
    let mut builder = TableBuilder::create(path, 42, 160, 4, 12, compression).unwrap();
    for (key, value) in &entries {
        builder.add(key, value).unwrap();
    }
    let result = builder.finish().unwrap();
    (entries, result)
}

#[test]
fn multi_block_table_supports_properties_point_lookup_and_ordered_iteration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000042.sst.tmp");
    let (entries, result) = build_table(&path, Compression::None);

    assert_eq!(result.file_number, 42);
    assert_eq!(result.entries, entries.len() as u64);
    assert_eq!(result.smallest, entries.first().unwrap().0.clone());
    assert_eq!(result.largest, entries.last().unwrap().0.clone());
    assert_eq!(result.file_size, std::fs::metadata(&path).unwrap().len());

    let reader = TableReader::open(&path).unwrap();
    assert!(reader.properties().data_blocks > 1);
    assert_eq!(reader.properties().file_number, 42);
    assert_eq!(reader.properties().entries, entries.len() as u64);
    assert_eq!(
        reader.get(&entries[0].0).unwrap(),
        Some(entries[0].1.clone())
    );
    assert_eq!(
        reader.get(&entries[19].0).unwrap(),
        Some(entries[19].1.clone())
    );
    assert_eq!(
        reader.get(&entries[39].0).unwrap(),
        Some(entries[39].1.clone())
    );

    let iterated = reader.iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(iterated, entries);
}

#[test]
fn bloom_negative_avoids_reading_a_corrupt_data_block() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000042.sst.tmp");
    let (entries, _) = build_table(&path, Compression::None);

    let reader = TableReader::open(&path).unwrap();
    let absent = (0..10_000)
        .map(|number| InternalKey::value(format!("absent-{number}"), 7))
        .find(|key| !reader.may_contain(key))
        .expect("the Bloom filter should reject at least one absent key");
    drop(reader);

    corrupt_byte(&path, 0);
    let reader = TableReader::open(&path).unwrap();
    assert_eq!(reader.get(&absent).unwrap(), None);
    assert!(matches!(
        reader.get(&entries[0].0),
        Err(Error::Corruption {
            context: "SSTable block",
            ..
        })
    ));
}

#[test]
fn snappy_blocks_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000042.sst.tmp");
    let (entries, result) = build_table(&path, Compression::Snappy);

    let reader = TableReader::open(&path).unwrap();
    assert_eq!(reader.properties().compression, Compression::Snappy);
    assert_eq!(
        reader.iter().collect::<Result<Vec<_>, _>>().unwrap(),
        entries
    );
    assert_eq!(reader.file_size(), result.file_size);
}

#[test]
fn data_block_codec_must_match_checked_table_properties() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000042.sst.tmp");
    let (entries, _) = build_table(&path, Compression::None);

    let properties = footer_handle(&path, 40);
    let mut stored = read_at(&path, properties.offset(), properties.size() as usize);
    let payload_end = stored.len() - 5;
    let mut cursor = 0;
    for _ in 0..3 {
        cursor = skip_varint(&stored[..payload_end], cursor);
    }
    stored[cursor] = 1;
    rewrite_stored_checksum(&mut stored);
    write_at(&path, properties.offset(), &stored);

    let reader = TableReader::open(&path).unwrap();
    assert!(matches!(
        reader.get(&entries[0].0),
        Err(Error::Corruption {
            context: "SSTable data block",
            ..
        })
    ));
}

#[test]
fn footer_magic_and_version_are_checked() {
    let dir = tempfile::tempdir().unwrap();
    let valid = dir.path().join("valid.sst.tmp");
    build_table(&valid, Compression::None);

    let bad_magic = dir.path().join("bad-magic.sst");
    std::fs::copy(&valid, &bad_magic).unwrap();
    let length = std::fs::metadata(&bad_magic).unwrap().len();
    corrupt_byte(&bad_magic, length - SSTABLE_MAGIC.len() as u64);
    assert!(matches!(
        TableReader::open(&bad_magic),
        Err(Error::Corruption {
            context: "SSTable footer",
            ..
        })
    ));

    let bad_version = dir.path().join("bad-version.sst");
    std::fs::copy(&valid, &bad_version).unwrap();
    write_at(
        &bad_version,
        length - SSTABLE_MAGIC.len() as u64 - 4,
        &(SSTABLE_FORMAT_VERSION + 1).to_le_bytes(),
    );
    assert!(matches!(
        TableReader::open(&bad_version),
        Err(Error::UnsupportedFormat {
            kind: "SSTable",
            version
        }) if version == SSTABLE_FORMAT_VERSION + 1
    ));
}

#[test]
fn truncated_and_out_of_range_footer_handles_are_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let valid = dir.path().join("valid.sst.tmp");
    build_table(&valid, Compression::None);

    let truncated = dir.path().join("truncated.sst");
    std::fs::write(&truncated, [0_u8; 12]).unwrap();
    assert!(matches!(
        TableReader::open(&truncated),
        Err(Error::Corruption {
            context: "SSTable footer",
            ..
        })
    ));

    let bad_handle = dir.path().join("bad-handle.sst");
    std::fs::copy(&valid, &bad_handle).unwrap();
    let length = std::fs::metadata(&bad_handle).unwrap().len();
    write_at(
        &bad_handle,
        length - SSTABLE_FOOTER_BYTES as u64,
        &[0xff; 20],
    );
    assert!(matches!(
        TableReader::open(&bad_handle),
        Err(Error::Corruption {
            context: "SSTable footer",
            ..
        })
    ));
}

#[test]
fn builder_requires_internal_key_order_and_nonempty_tables() {
    let dir = tempfile::tempdir().unwrap();
    let empty_path = dir.path().join("empty.sst.tmp");
    let empty = TableBuilder::create(&empty_path, 1, 128, 4, 10, Compression::None)
        .unwrap()
        .finish();
    assert!(matches!(empty, Err(Error::InvalidArgument(message)) if message.contains("empty")));

    let path = dir.path().join("unordered.sst.tmp");
    let mut builder = TableBuilder::create(&path, 2, 128, 4, 10, Compression::None).unwrap();
    let later = InternalKey::value(b"later", 1);
    let earlier = InternalKey::value(b"earlier", 1);
    builder.add(&later, b"1").unwrap();
    assert!(matches!(
        builder.add(&earlier, b"2"),
        Err(Error::InvalidArgument(message)) if message.contains("strictly increasing")
    ));
}

#[test]
fn finish_reports_temporary_file_sync_failure() {
    let syncs = Arc::new(AtomicUsize::new(0));
    let fs = Arc::new(FailingSyncFs {
        syncs: Arc::clone(&syncs),
    });
    let mut builder =
        TableBuilder::create_with_fs("injected.sst.tmp", 9, 128, 4, 10, Compression::None, fs)
            .unwrap();
    builder
        .add(&InternalKey::value(b"key", 1), b"value")
        .unwrap();

    assert!(matches!(
        builder.finish(),
        Err(Error::Io {
            operation: "sync SSTable",
            ..
        })
    ));
    assert_eq!(syncs.load(Ordering::SeqCst), 1);
}

fn corrupt_byte(path: &std::path::Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x80;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

fn write_at(path: &std::path::Path, offset: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn footer_handle(path: &std::path::Path, slot_start: usize) -> BlockHandle {
    let length = std::fs::metadata(path).unwrap().len();
    let footer = read_at(
        path,
        length - SSTABLE_FOOTER_BYTES as u64,
        SSTABLE_FOOTER_BYTES,
    );
    BlockHandle::decode(&footer[slot_start..slot_start + 20])
        .unwrap()
        .0
}

fn read_at(path: &std::path::Path, offset: u64, length: usize) -> Vec<u8> {
    let mut file = OpenOptions::new().read(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes).unwrap();
    bytes
}

fn skip_varint(bytes: &[u8], mut cursor: usize) -> usize {
    while bytes[cursor] & 0x80 != 0 {
        cursor += 1;
    }
    cursor + 1
}

fn rewrite_stored_checksum(stored: &mut [u8]) {
    let payload_end = stored.len() - 5;
    let marker = stored[payload_end];
    let checksum = crc32c::crc32c_append(crc32c::crc32c(&stored[..payload_end]), &[marker])
        .rotate_right(15)
        .wrapping_add(0xa282_ead8);
    stored[payload_end + 1..].copy_from_slice(&checksum.to_le_bytes());
}

struct FailingSyncFs {
    syncs: Arc<AtomicUsize>,
}

impl DurableFs for FailingSyncFs {
    fn create(&self, _path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(FailingSyncFile {
            syncs: Arc::clone(&self.syncs),
        }))
    }

    fn append(&self, _path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        unreachable!()
    }

    fn sync_directory(&self, _path: &Path) -> std::io::Result<()> {
        unreachable!()
    }

    fn atomic_replace(&self, _source: &Path, _destination: &Path) -> std::io::Result<()> {
        unreachable!()
    }
}

struct FailingSyncFile {
    syncs: Arc<AtomicUsize>,
}

impl DurableFile for FailingSyncFile {
    fn write_all(&mut self, _bytes: &[u8]) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("injected sync failure"))
    }
}
