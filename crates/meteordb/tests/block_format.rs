use meteordb::{
    BLOCK_TRAILER_BYTES, Block, BlockBuilder, BlockHandle, BloomFilter, Error, InternalKey,
    ValueKind, decode_stored_block, encode_stored_block,
};

#[test]
fn bloom_filter_is_deterministic_and_has_no_false_negatives() {
    let keys = [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()];
    let first = BloomFilter::from_keys(keys, 10).unwrap();
    let second = BloomFilter::from_keys(keys, 10).unwrap();

    assert_eq!(first.as_bytes(), second.as_bytes());
    for key in keys {
        assert!(first.may_contain(key));
    }
}

#[test]
fn bloom_filter_round_trips_and_can_reject_an_absent_key() {
    let filter = BloomFilter::from_keys([b"present".as_slice()], 20).unwrap();
    let decoded = BloomFilter::decode(filter.as_bytes()).unwrap();

    assert!(decoded.may_contain(b"present"));
    assert!(!decoded.may_contain(b"definitely absent"));
}

#[test]
fn bloom_filter_rejects_invalid_configuration_and_encoding() {
    assert!(matches!(
        BloomFilter::from_keys([b"k".as_slice()], 0),
        Err(Error::InvalidArgument(message)) if message.contains("bits_per_key")
    ));
    assert!(matches!(
        BloomFilter::decode([]),
        Err(Error::Corruption {
            context: "Bloom filter",
            ..
        })
    ));
}

#[test]
fn block_seek_reconstructs_prefix_compressed_keys() {
    let mut builder = BlockBuilder::new(2);
    builder.add(b"feature/a", b"1").unwrap();
    builder.add(b"feature/b", b"2").unwrap();
    builder.add(b"feature/c", b"3").unwrap();
    let block = Block::decode(builder.finish()).unwrap();

    assert_eq!(block.seek(b"feature/b").unwrap().unwrap().1, b"2");
    assert_eq!(block.seek(b"feature/bb").unwrap().unwrap().0, b"feature/c");
    assert!(block.seek(b"feature/z").unwrap().is_none());
}

#[test]
fn block_iteration_preserves_order_and_values() {
    let mut builder = BlockBuilder::new(3);
    builder.add(b"apple", b"red").unwrap();
    builder.add(b"application", b"binary").unwrap();
    builder.add(b"apply", b"verb").unwrap();
    let block = Block::decode(builder.finish()).unwrap();

    let entries = block.iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        entries,
        [
            (b"apple".to_vec(), b"red".to_vec()),
            (b"application".to_vec(), b"binary".to_vec()),
            (b"apply".to_vec(), b"verb".to_vec()),
        ]
    );
}

#[test]
fn block_builder_requires_a_nonzero_interval_and_strict_key_order() {
    assert!(matches!(
        BlockBuilder::try_new(0),
        Err(Error::InvalidArgument(message)) if message.contains("restart_interval")
    ));

    let mut builder = BlockBuilder::new(2);
    builder.add(b"b", b"1").unwrap();
    assert!(matches!(
        builder.add(b"b", b"2"),
        Err(Error::InvalidArgument(message)) if message.contains("strictly increasing")
    ));
    assert!(matches!(
        builder.add(b"a", b"3"),
        Err(Error::InvalidArgument(message)) if message.contains("strictly increasing")
    ));
}

#[test]
fn internal_key_encoded_bytes_keep_block_order_and_seek_semantics() {
    let newest = InternalKey::try_new(b"k", 9, ValueKind::Value).unwrap();
    let older = InternalKey::try_new(b"k", 4, ValueKind::Value).unwrap();
    let next = InternalKey::try_new(b"next", 7, ValueKind::Deletion).unwrap();
    let mut builder = BlockBuilder::new(2);
    builder.add(newest.as_bytes(), b"new").unwrap();
    builder.add(older.as_bytes(), b"old").unwrap();
    builder.add(next.as_bytes(), b"").unwrap();
    let block = Block::decode(builder.finish()).unwrap();

    assert_eq!(
        block.seek(older.as_bytes()).unwrap().unwrap(),
        (older.into_bytes(), b"old".to_vec())
    );
}

#[test]
fn empty_block_is_valid_and_iterates_no_entries() {
    let block = Block::decode(BlockBuilder::new(4).finish()).unwrap();

    assert!(block.is_empty());
    assert_eq!(block.len(), 0);
    assert!(block.seek(b"anything").unwrap().is_none());
    assert_eq!(block.iter().count(), 0);
}

#[test]
fn malformed_restart_arrays_are_rejected() {
    let mut builder = BlockBuilder::new(1);
    builder.add(b"a", b"1").unwrap();
    builder.add(b"b", b"2").unwrap();
    let encoded = builder.finish();

    let mut descending = encoded.clone();
    let restart_count =
        u32::from_le_bytes(descending[descending.len() - 4..].try_into().unwrap()) as usize;
    let restarts_start = descending.len() - 4 - restart_count * 4;
    descending[restarts_start + 4..restarts_start + 8].copy_from_slice(&0_u32.to_le_bytes());
    assert!(matches!(
        Block::decode(descending),
        Err(Error::Corruption {
            context: "SSTable data block",
            ..
        })
    ));

    let mut out_of_bounds = encoded;
    let length = out_of_bounds.len() as u32;
    out_of_bounds[restarts_start..restarts_start + 4].copy_from_slice(&length.to_le_bytes());
    assert!(matches!(
        Block::decode(out_of_bounds),
        Err(Error::Corruption {
            context: "SSTable data block",
            ..
        })
    ));
}

#[test]
fn malformed_entries_and_overflowing_lengths_are_rejected() {
    let malformed = block_bytes_with_entries(&[
        0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
    ]);
    assert!(matches!(
        Block::decode(malformed),
        Err(Error::Corruption {
            context: "SSTable data block",
            ..
        })
    ));

    let overflowing = block_bytes_with_entries(&[
        0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x02,
    ]);
    assert!(matches!(
        Block::decode(overflowing),
        Err(Error::Corruption {
            context: "SSTable data block",
            ..
        })
    ));
}

#[test]
fn blocks_reject_non_canonical_varints() {
    for entries in [
        &[0x80, 0x00, 0x00, 0x00][..],
        &[0x00, 0x81, 0x00, 0x00][..],
        &[0x00, 0x00, 0x80, 0x80, 0x00][..],
        &[
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00, 0x00, 0x00,
        ][..],
    ] {
        assert!(matches!(
            Block::decode(block_bytes_with_entries(entries)),
            Err(Error::Corruption {
                context: "SSTable data block",
                ..
            })
        ));
    }
}

#[test]
fn decoded_blocks_reject_non_increasing_keys() {
    let mut entries = Vec::new();
    entries.extend_from_slice(&[0, 1, 0, b'b']);
    entries.extend_from_slice(&[0, 1, 0, b'a']);

    assert!(matches!(
        Block::decode(block_bytes_with_entries(&entries)),
        Err(Error::Corruption {
            context: "SSTable data block",
            ..
        })
    ));
}

#[test]
fn stored_block_checksum_detects_corruption() {
    let encoded = encode_stored_block(b"checked bytes", 0).unwrap();
    assert_eq!(encoded.len(), b"checked bytes".len() + BLOCK_TRAILER_BYTES);
    assert_eq!(
        decode_stored_block(&encoded).unwrap(),
        (b"checked bytes".to_vec(), 0)
    );

    let mut corrupted = encoded;
    corrupted[0] ^= 0x80;
    assert!(matches!(
        decode_stored_block(&corrupted),
        Err(Error::Corruption {
            context: "SSTable block",
            ..
        })
    ));
}

#[test]
fn block_handle_uses_checked_varints() {
    let handle = BlockHandle::new(300, 16_384);
    let encoded = handle.encode();
    let (decoded, consumed) = BlockHandle::decode(&encoded).unwrap();
    assert_eq!(decoded, handle);
    assert_eq!(consumed, encoded.len());

    assert!(matches!(
        BlockHandle::decode(&[0xff; 10]),
        Err(Error::Corruption {
            context: "SSTable block handle",
            ..
        })
    ));

    let overflowing_range = BlockHandle::new(u64::MAX, 1).encode();
    assert!(matches!(
        BlockHandle::decode(&overflowing_range),
        Err(Error::Corruption {
            context: "SSTable block handle",
            ..
        })
    ));
}

#[test]
fn block_handles_reject_non_canonical_varints_and_accept_boundaries() {
    for encoded in [
        &[0x80, 0x00, 0x00][..],
        &[0x81, 0x00, 0x00][..],
        &[0x00, 0x80, 0x80, 0x00][..],
        &[
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00, 0x00,
        ][..],
    ] {
        assert!(matches!(
            BlockHandle::decode(encoded),
            Err(Error::Corruption {
                context: "SSTable block handle",
                ..
            })
        ));
    }

    for value in [
        0,
        0x7f,
        0x80,
        0x3fff,
        0x4000,
        (1_u64 << 63) - 1,
        1_u64 << 63,
        u64::MAX,
    ] {
        let handle = BlockHandle::new(value, 0);
        let encoded = handle.encode();
        assert_eq!(BlockHandle::decode(&encoded).unwrap().0, handle);
    }
}

fn block_bytes_with_entries(entries: &[u8]) -> Vec<u8> {
    let mut encoded = entries.to_vec();
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    encoded
}
