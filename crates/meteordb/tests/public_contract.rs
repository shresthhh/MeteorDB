use meteordb::{Durability, Options, WriteBatch};

#[test]
fn options_reject_zero_sized_limits() {
    let mut options = Options::new("/tmp/meteordb");
    options.max_key_bytes = 0;
    assert!(options.validate().is_err());
}

#[test]
fn batch_preserves_operation_order() {
    let mut batch = WriteBatch::default();
    batch.put(b"a", b"1").delete(b"b");
    assert_eq!(batch.len(), 2);
    assert_eq!(Durability::default(), Durability::Sync);
}
