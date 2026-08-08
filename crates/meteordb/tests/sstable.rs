use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use meteordb::{
    BlockHandle, Compression, DurableFile, DurableFs, Error, InternalKey, SSTABLE_FOOTER_BYTES,
    SSTABLE_FORMAT_VERSION, SSTABLE_MAGIC, TableBuilder, TableReader, TableReaderOptions,
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
fn reader_limit_rejects_untrusted_snappy_size_declarations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oversized-snappy.sst.tmp");
    let mut builder = TableBuilder::create(&path, 42, 1024, 4, 10, Compression::Snappy).unwrap();
    builder
        .add(&InternalKey::value(b"key", 7), b"value")
        .unwrap();
    builder.finish().unwrap();

    let properties = footer_handle(&path, 40);
    let mut stored = read_at(&path, properties.offset(), properties.size() as usize);
    let payload_end = stored.len() - 5;
    let mut cursor = 0;
    for _ in 0..3 {
        cursor = skip_varint(&stored[..payload_end], cursor);
    }
    cursor += 1;
    assert_eq!(stored[cursor] & 0x80, 0);
    stored[cursor] = 127;
    rewrite_stored_checksum(&mut stored);
    write_at(&path, properties.offset(), &stored);

    let filter = footer_handle(&path, 20);
    let mut data = read_at(&path, 0, filter.offset() as usize);
    data[0] = 127;
    rewrite_stored_checksum(&mut data);
    write_at(&path, 0, &data);

    let options = TableReaderOptions {
        max_uncompressed_data_block_bytes: 64,
    };
    assert!(matches!(
        TableReader::open_with_options(&path, options),
        Err(Error::Corruption {
            context: "SSTable properties",
            detail,
        }) if detail.contains("reader limit")
    ));
}

#[test]
fn snappy_block_header_cannot_exceed_reader_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oversized-snappy-block.sst.tmp");
    let mut builder = TableBuilder::create(&path, 42, 1024, 4, 10, Compression::Snappy).unwrap();
    let key = InternalKey::value(b"key", 7);
    builder.add(&key, b"value").unwrap();
    builder.finish().unwrap();

    let properties = footer_handle(&path, 40);
    let mut stored = read_at(&path, properties.offset(), properties.size() as usize);
    let payload_end = stored.len() - 5;
    let mut cursor = 0;
    for _ in 0..3 {
        cursor = skip_varint(&stored[..payload_end], cursor);
    }
    cursor += 1;
    assert_eq!(stored[cursor] & 0x80, 0);
    stored[cursor] = 64;
    rewrite_stored_checksum(&mut stored);
    write_at(&path, properties.offset(), &stored);

    let filter = footer_handle(&path, 20);
    let mut data = read_at(&path, 0, filter.offset() as usize);
    data[0] = 127;
    rewrite_stored_checksum(&mut data);
    write_at(&path, 0, &data);

    let reader = TableReader::open_with_options(
        &path,
        TableReaderOptions {
            max_uncompressed_data_block_bytes: 64,
        },
    )
    .unwrap();
    assert!(matches!(
        reader.get(&key),
        Err(Error::Corruption {
            context: "SSTable data block",
            detail,
        }) if detail.contains("reader limit")
    ));
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
fn metadata_roles_must_follow_canonical_physical_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("swapped-metadata.sst");
    build_table(&path, Compression::None);

    let bytes = std::fs::read(&path).unwrap();
    let footer_start = bytes.len() - SSTABLE_FOOTER_BYTES;
    let index = footer_handle(&path, 0);
    let filter = footer_handle(&path, 20);
    let properties = footer_handle(&path, 40);
    let data_end = filter.offset() as usize;
    let filter_bytes = block_bytes(&bytes, filter);
    let index_bytes = block_bytes(&bytes, index);
    let properties_bytes = block_bytes(&bytes, properties);

    let swapped_index = BlockHandle::new(data_end as u64, index.size());
    let swapped_filter = BlockHandle::new(data_end as u64 + index.size(), filter.size());
    let mut swapped = bytes[..data_end].to_vec();
    swapped.extend_from_slice(index_bytes);
    swapped.extend_from_slice(filter_bytes);
    swapped.extend_from_slice(properties_bytes);
    swapped.extend_from_slice(&test_footer(swapped_index, swapped_filter, properties));
    assert_eq!(swapped.len(), footer_start + SSTABLE_FOOTER_BYTES);
    std::fs::write(&path, swapped).unwrap();

    assert!(matches!(
        TableReader::open(&path),
        Err(Error::Corruption {
            context: "SSTable footer",
            detail,
        }) if detail.contains("canonical")
    ));
}

#[test]
fn canonical_layout_rejects_gaps_between_data_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gapped-metadata.sst");
    build_table(&path, Compression::None);

    let bytes = std::fs::read(&path).unwrap();
    let footer_start = bytes.len() - SSTABLE_FOOTER_BYTES;
    let index = footer_handle(&path, 0);
    let filter = footer_handle(&path, 20);
    let properties = footer_handle(&path, 40);
    let gap_at = filter.offset() as usize;
    let shifted_index = BlockHandle::new(index.offset() + 1, index.size());
    let shifted_filter = BlockHandle::new(filter.offset() + 1, filter.size());
    let shifted_properties = BlockHandle::new(properties.offset() + 1, properties.size());

    let mut gapped = bytes[..gap_at].to_vec();
    gapped.push(0);
    gapped.extend_from_slice(&bytes[gap_at..footer_start]);
    gapped.extend_from_slice(&test_footer(
        shifted_index,
        shifted_filter,
        shifted_properties,
    ));
    std::fs::write(&path, gapped).unwrap();

    assert!(matches!(
        TableReader::open(&path),
        Err(Error::Corruption {
            context: "SSTable index",
            detail,
        }) if detail.contains("immediately followed")
    ));
}

#[test]
fn iterator_internal_key_corruption_is_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("malformed-internal-key.sst");
    let mut builder = TableBuilder::create(&path, 42, 1024, 4, 10, Compression::None).unwrap();
    builder
        .add(&InternalKey::value(b"first", 7), b"value-1")
        .unwrap();
    builder
        .add(&InternalKey::value(b"second", 7), b"value-2")
        .unwrap();
    builder.finish().unwrap();

    let filter = footer_handle(&path, 20);
    let mut stored = read_at(&path, 0, filter.offset() as usize);
    let payload_end = stored.len() - 5;
    let (shared, shared_bytes) = read_test_varint(&stored[..payload_end], 0);
    assert_eq!(shared, 0);
    let (unshared, unshared_bytes) = read_test_varint(&stored[..payload_end], shared_bytes);
    let value_cursor = shared_bytes + unshared_bytes;
    let (_, value_bytes) = read_test_varint(&stored[..payload_end], value_cursor);
    let key_start = value_cursor + value_bytes;
    stored[key_start + unshared - 1] = 2;
    rewrite_stored_checksum(&mut stored);
    write_at(&path, 0, &stored);

    let reader = TableReader::open(&path).unwrap();
    let mut iter = reader.iter();
    assert!(matches!(
        iter.next(),
        Some(Err(Error::Corruption {
            context: "internal key",
            ..
        }))
    ));
    assert!(iter.next().is_none());
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

fn block_bytes(bytes: &[u8], handle: BlockHandle) -> &[u8] {
    &bytes[handle.offset() as usize..(handle.offset() + handle.size()) as usize]
}

fn test_footer(
    index: BlockHandle,
    filter: BlockHandle,
    properties: BlockHandle,
) -> [u8; SSTABLE_FOOTER_BYTES] {
    let mut footer = [0; SSTABLE_FOOTER_BYTES];
    for (slot, handle) in [index, filter, properties].into_iter().enumerate() {
        let encoded = handle.encode();
        let start = slot * 20;
        footer[start..start + encoded.len()].copy_from_slice(&encoded);
    }
    footer[60..64].copy_from_slice(&SSTABLE_FORMAT_VERSION.to_le_bytes());
    footer[64..].copy_from_slice(&SSTABLE_MAGIC);
    footer
}

fn skip_varint(bytes: &[u8], mut cursor: usize) -> usize {
    while bytes[cursor] & 0x80 != 0 {
        cursor += 1;
    }
    cursor + 1
}

fn read_test_varint(bytes: &[u8], start: usize) -> (usize, usize) {
    let mut value = 0_usize;
    for index in 0..10 {
        let byte = bytes[start + index];
        value |= usize::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return (value, index + 1);
        }
    }
    panic!("test varint exceeds ten bytes");
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
