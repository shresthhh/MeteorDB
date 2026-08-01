use meteordb::{Durability, Error, Options, WriteBatch, WriteOp};

#[test]
fn options_report_the_exact_invalid_field() {
    let mut options = Options::new("/tmp/meteordb");
    options.max_key_bytes = 0;
    assert!(matches!(
        options.validate(),
        Err(Error::InvalidArgument(message))
            if message == "max_key_bytes must be greater than zero"
    ));
}

#[test]
fn batch_exposes_owned_ordered_operations_and_byte_accounting() {
    let mut batch = WriteBatch::default();
    let mut key = b"alpha".to_vec();
    let mut value = b"one".to_vec();

    batch
        .put(&key, &value)
        .delete(b"beta")
        .put_with_expiration(b"gamma", b"three", Some(1_234));
    key.fill(b'x');
    value.fill(b'y');

    assert_eq!(
        batch.operations(),
        [
            WriteOp::Put {
                key: b"alpha".to_vec(),
                value: b"one".to_vec(),
                expires_at_unix_ms: None,
            },
            WriteOp::Delete {
                key: b"beta".to_vec(),
            },
            WriteOp::Put {
                key: b"gamma".to_vec(),
                value: b"three".to_vec(),
                expires_at_unix_ms: Some(1_234),
            },
        ]
    );
    assert_eq!(batch.len(), 3);
    assert_eq!(batch.approximate_bytes(), 22);
    assert_eq!(Durability::default(), Durability::Sync);
}
