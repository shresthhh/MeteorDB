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
            detail,
        }) if detail == "encoded key is shorter than 9 bytes"
    ));

    let mut unknown_kind = InternalKey::value(b"k", 7).into_bytes();
    *unknown_kind
        .last_mut()
        .expect("an internal key has a kind byte") = 2;
    assert!(matches!(
        InternalKey::decode(&unknown_kind),
        Err(Error::Corruption {
            context: "internal key",
            detail,
        }) if detail == "unknown value kind byte 2"
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
}
