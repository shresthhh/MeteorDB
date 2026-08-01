use meteordb::{Error, InternalKey, SnapshotRegistry, ValueKind};
use proptest::prelude::*;

#[test]
fn newer_versions_sort_before_older_versions() {
    let old = InternalKey::value(b"k", 7);
    let new = InternalKey::value(b"k", 9);

    assert!(new < old);
    assert!(new.as_bytes() < old.as_bytes());
}

#[test]
fn user_keys_sort_in_ascending_byte_order() {
    let short = InternalKey::value(b"a", 7);
    let longer = InternalKey::value(b"aa", 7);
    let later = InternalKey::value(b"b", 7);

    assert!(short < longer);
    assert!(longer < later);
    assert_eq!(short.as_bytes().cmp(longer.as_bytes()), short.cmp(&longer));
    assert_eq!(longer.as_bytes().cmp(later.as_bytes()), longer.cmp(&later));
}

#[test]
fn encoded_prefix_keys_sort_in_user_key_order() {
    let keys = [
        InternalKey::value(b"", 7),
        InternalKey::value(b"a", 7),
        InternalKey::value(b"aa", 7),
        InternalKey::value(b"b", 7),
    ];

    for pair in keys.windows(2) {
        assert!(pair[0] < pair[1]);
        assert!(pair[0].as_bytes() < pair[1].as_bytes());
    }
}

#[test]
fn zero_bytes_are_escaped_without_changing_order() {
    let keys = [
        InternalKey::value(b"", 7),
        InternalKey::value(b"\0", 7),
        InternalKey::value(b"\0\0", 7),
        InternalKey::value(b"\0\x01", 7),
        InternalKey::value(b"\x01", 7),
    ];

    assert_eq!(
        InternalKey::value(b"a\0b", 7).as_bytes(),
        [
            b'a', 0x00, 0xff, b'b', 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf8,
            0x01,
        ]
    );

    for pair in keys.windows(2) {
        assert!(pair[0] < pair[1]);
        assert!(pair[0].as_bytes() < pair[1].as_bytes());
    }
}

#[test]
fn value_kind_breaks_ties_after_key_and_sequence() {
    let deletion = InternalKey::deletion(b"k", 7);
    let value = InternalKey::value(b"k", 7);

    assert!(deletion < value);
}

#[test]
fn malformed_internal_keys_are_rejected() {
    assert!(matches!(
        InternalKey::try_new(b"k", u64::MAX, ValueKind::Value),
        Err(Error::InvalidArgument(message))
            if message == "sequence number must be less than u64::MAX"
    ));
    assert!(matches!(
        InternalKey::decode(b"short"),
        Err(Error::Corruption {
            context: "internal key",
            ..
        })
    ));

    let mut unknown_kind = InternalKey::value(b"k", 7).into_bytes();
    *unknown_kind
        .last_mut()
        .expect("an internal key has a kind byte") = 2;
    assert!(matches!(
        InternalKey::decode(&unknown_kind),
        Err(Error::Corruption {
            context: "internal key",
            ..
        })
    ));

    let missing_terminator = [b'k', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf8, 0x01];
    assert!(matches!(
        InternalKey::decode(missing_terminator),
        Err(Error::Corruption {
            context: "internal key",
            ..
        })
    ));

    let invalid_escape = [
        b'k', 0x00, 0x01, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf8, 0x01,
    ];
    assert!(matches!(
        InternalKey::decode(invalid_escape),
        Err(Error::Corruption {
            context: "internal key",
            ..
        })
    ));

    let truncated_trailer = [
        b'k', 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf8,
    ];
    assert!(matches!(
        InternalKey::decode(truncated_trailer),
        Err(Error::Corruption {
            context: "internal key",
            ..
        })
    ));

    let reserved_sequence = [
        b'k', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
    ];
    assert!(matches!(
        InternalKey::decode(reserved_sequence),
        Err(Error::Corruption {
            context: "internal key",
            ..
        })
    ));

    let mut trailing_bytes = InternalKey::value(b"k", 7).into_bytes();
    trailing_bytes.push(0x01);
    assert!(matches!(
        InternalKey::decode(trailing_bytes),
        Err(Error::Corruption {
            context: "internal key",
            ..
        })
    ));
}

#[test]
fn snapshot_tracks_the_oldest_active_sequence() {
    let registry = SnapshotRegistry::default();
    let first = registry.acquire(10);
    let second = registry.acquire(20);

    assert_eq!(registry.oldest_active(), Some(10));
    drop(first);
    assert_eq!(registry.oldest_active(), Some(20));
    drop(second);
    assert_eq!(registry.oldest_active(), None);
}

#[test]
fn snapshots_reference_count_duplicate_sequences() {
    let registry = SnapshotRegistry::default();
    let first = registry.acquire(10);
    let second = registry.acquire(10);

    assert_eq!(first.sequence(), 10);
    assert_eq!(registry.oldest_active(), Some(10));
    drop(first);
    assert_eq!(registry.oldest_active(), Some(10));
    drop(second);
    assert_eq!(registry.oldest_active(), None);
}

#[test]
fn cloned_registries_share_snapshot_state() {
    let registry = SnapshotRegistry::default();
    let clone = registry.clone();
    let guard = clone.acquire(42);

    assert_eq!(registry.oldest_active(), Some(42));
    drop(guard);
    assert_eq!(clone.oldest_active(), None);
}

proptest! {
    #[test]
    fn internal_key_encoding_round_trips(
        user_key in prop::collection::vec(any::<u8>(), 0..256),
        sequence in 0..u64::MAX,
        kind in prop_oneof![Just(ValueKind::Deletion), Just(ValueKind::Value)],
    ) {
        let key = InternalKey::try_new(&user_key, sequence, kind)
            .expect("generated sequences are valid");
        let decoded = InternalKey::decode(key.as_bytes())
            .expect("encoded valid keys must decode");

        prop_assert_eq!(decoded.user_key(), user_key.as_slice());
        prop_assert_eq!(decoded.sequence(), sequence);
        prop_assert_eq!(decoded.kind(), kind);
        prop_assert_eq!(decoded, key);
    }

    #[test]
    fn encoded_comparison_matches_internal_key_order(
        left_user_key in prop::collection::vec(any::<u8>(), 0..256),
        left_sequence in 0..u64::MAX,
        left_kind in prop_oneof![Just(ValueKind::Deletion), Just(ValueKind::Value)],
        right_user_key in prop::collection::vec(any::<u8>(), 0..256),
        right_sequence in 0..u64::MAX,
        right_kind in prop_oneof![Just(ValueKind::Deletion), Just(ValueKind::Value)],
    ) {
        let left = InternalKey::try_new(left_user_key, left_sequence, left_kind)
            .expect("generated sequences are valid");
        let right = InternalKey::try_new(right_user_key, right_sequence, right_kind)
            .expect("generated sequences are valid");

        prop_assert_eq!(left.as_bytes().cmp(right.as_bytes()), left.cmp(&right));
    }
}
