use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use meteordb::{
    CacheLookup, Embedding, EmbeddingKey, EmbeddingStore, Engine, Error, FeatureKey, FeatureRecord,
    FeatureStore, FeatureValue, InferenceCache, InferenceEntry, InferenceKey, ManualClock, Options,
    ScalarType,
};

fn key(version: &[u8], input: &[u8]) -> InferenceKey {
    InferenceKey::new(b"acme/model", version, input)
}

#[test]
fn inference_cache_canonicalizes_parameter_order_without_ambiguous_concatenation() {
    let dir = tempfile::tempdir().unwrap();
    let cache =
        InferenceCache::new(Engine::open(Options::new(dir.path())).unwrap(), b"tenant-a").unwrap();
    let first = key(b"v1", b"prompt")
        .with_parameter(b"temperature", b"0.2")
        .with_parameter(b"top_p", b"0.95");
    let reordered = key(b"v1", b"prompt")
        .with_parameter(b"top_p", b"0.95")
        .with_parameter(b"temperature", b"0.2");

    cache
        .put(&first, InferenceEntry::new(b"result"), None)
        .unwrap();

    assert_eq!(
        cache.get(&reordered).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"result"))
    );

    let left = InferenceKey::new(b"ab", b"c", b"input");
    let right = InferenceKey::new(b"a", b"bc", b"input");
    cache
        .put(&left, InferenceEntry::new(b"left"), None)
        .unwrap();
    cache
        .put(&right, InferenceEntry::new(b"right"), None)
        .unwrap();
    assert_eq!(
        cache.get(&left).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"left"))
    );
    assert_eq!(
        cache.get(&right).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"right"))
    );

    let parameter_left = key(b"v1", b"parameter boundaries").with_parameter(b"a", b"bc");
    let parameter_right = key(b"v1", b"parameter boundaries").with_parameter(b"ab", b"c");
    cache
        .put(
            &parameter_left,
            InferenceEntry::new(b"parameter-left"),
            None,
        )
        .unwrap();
    cache
        .put(
            &parameter_right,
            InferenceEntry::new(b"parameter-right"),
            None,
        )
        .unwrap();
    assert_eq!(
        cache.get(&parameter_left).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"parameter-left"))
    );
    assert_eq!(
        cache.get(&parameter_right).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"parameter-right"))
    );
}

#[test]
fn inference_cache_separates_model_versions_and_namespaces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(Options::new(dir.path())).unwrap();
    let tenant_a = InferenceCache::new(engine.clone(), b"tenant-a").unwrap();
    let tenant_b = InferenceCache::new(engine, b"tenant-b").unwrap();
    let version_one = key(b"v1", b"same prompt");
    let version_two = key(b"v2", b"same prompt");

    tenant_a
        .put(&version_one, InferenceEntry::new(b"a-v1"), None)
        .unwrap();
    tenant_a
        .put(&version_two, InferenceEntry::new(b"a-v2"), None)
        .unwrap();
    tenant_b
        .put(&version_one, InferenceEntry::new(b"b-v1"), None)
        .unwrap();

    assert_eq!(
        tenant_a.get(&version_one).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"a-v1"))
    );
    assert_eq!(
        tenant_a.get(&version_two).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"a-v2"))
    );
    assert_eq!(
        tenant_b.get(&version_one).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"b-v1"))
    );
}

#[test]
fn inference_cache_ttl_expiry_is_a_miss_and_binary_payloads_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let engine = Engine::open_with_clock(Options::new(dir.path()), clock.clone()).unwrap();
    let cache = InferenceCache::new(engine, b"binary").unwrap();
    let request = key(b"v1", &[0, 1, 0xff, 0, 2]);
    let entry = InferenceEntry::new([0, 0xff, 1, 0, 2]);

    cache.put(&request, entry.clone(), Some(10)).unwrap();
    assert_eq!(cache.get(&request).unwrap(), CacheLookup::Hit(entry));

    clock.set(1_010).unwrap();
    assert_eq!(cache.get(&request).unwrap(), CacheLookup::Miss);
}

#[test]
fn inference_cache_entry_uses_schema_byte_and_postcard_body() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(Options::new(dir.path())).unwrap();
    let cache = InferenceCache::new(engine.clone(), b"format").unwrap();
    let request = key(b"v1", b"postcard framing");
    let entry = InferenceEntry::new([0, 0xff, 1, 0, 2]);

    cache.put(&request, entry.clone(), None).unwrap();

    let (raw_key, raw_entry) = raw_cache_entry(&engine);
    assert!(raw_key.starts_with(b"\0meteordb:inference-cache\0"));
    assert_eq!(raw_entry[0], 1);
    assert_eq!(&raw_entry[1..], postcard::to_allocvec(&entry).unwrap());
    assert_eq!(cache.get(&request).unwrap(), CacheLookup::Hit(entry),);
}

#[test]
fn inference_cache_rejects_malformed_postcard_entry_as_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(Options::new(dir.path())).unwrap();
    let cache = InferenceCache::new(engine.clone(), b"malformed").unwrap();
    let request = key(b"v1", b"malformed postcard");
    cache
        .put(&request, InferenceEntry::new(b"value"), None)
        .unwrap();
    let (raw_key, _) = raw_cache_entry(&engine);

    engine.put(raw_key, [1, 0x80]).unwrap();

    assert!(matches!(
        cache.get(&request),
        Err(Error::Corruption {
            context: "inference-cache entry",
            ..
        })
    ));
}

#[test]
fn inference_cache_rejects_trailing_entry_bytes_as_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(Options::new(dir.path())).unwrap();
    let cache = InferenceCache::new(engine.clone(), b"trailing").unwrap();
    let request = key(b"v1", b"trailing bytes");
    cache
        .put(&request, InferenceEntry::new([0, 1, 0xff]), None)
        .unwrap();
    let (raw_key, mut raw_entry) = raw_cache_entry(&engine);
    raw_entry.push(0);

    engine.put(raw_key, raw_entry).unwrap();

    assert!(matches!(
        cache.get(&request),
        Err(Error::Corruption {
            context: "inference-cache entry",
            ..
        })
    ));
}

#[test]
fn inference_cache_rejects_unknown_entry_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(Options::new(dir.path())).unwrap();
    let cache = InferenceCache::new(engine.clone(), b"schema").unwrap();
    let request = key(b"v1", b"unknown schema");
    cache
        .put(&request, InferenceEntry::new(b"value"), None)
        .unwrap();
    let (raw_key, mut raw_entry) = raw_cache_entry(&engine);
    raw_entry[0] = 2;

    engine.put(raw_key, raw_entry).unwrap();

    assert!(matches!(
        cache.get(&request),
        Err(Error::UnsupportedFormat {
            kind: "inference-cache entry",
            version: 2,
        })
    ));
}

#[test]
fn inference_cache_batch_lookup_preserves_duplicates_and_request_order() {
    let dir = tempfile::tempdir().unwrap();
    let cache =
        InferenceCache::new(Engine::open(Options::new(dir.path())).unwrap(), b"batch").unwrap();
    let alpha = key(b"v1", b"alpha");
    let missing = key(b"v1", b"missing");
    let beta = key(b"v1", b"beta");
    cache.put(&alpha, InferenceEntry::new(b"A"), None).unwrap();
    cache.put(&beta, InferenceEntry::new(b"B"), None).unwrap();

    assert_eq!(
        cache.get_many([&beta, &missing, &alpha, &beta]).unwrap(),
        vec![
            CacheLookup::Hit(InferenceEntry::new(b"B")),
            CacheLookup::Miss,
            CacheLookup::Hit(InferenceEntry::new(b"A")),
            CacheLookup::Hit(InferenceEntry::new(b"B")),
        ]
    );
}

#[test]
fn inference_cache_delete_removes_only_the_exact_canonical_key() {
    let dir = tempfile::tempdir().unwrap();
    let cache =
        InferenceCache::new(Engine::open(Options::new(dir.path())).unwrap(), b"delete").unwrap();
    let first = key(b"v1", b"first");
    let second = key(b"v1", b"second");
    cache
        .put(&first, InferenceEntry::new(b"one"), None)
        .unwrap();
    cache
        .put(&second, InferenceEntry::new(b"two"), None)
        .unwrap();

    cache.delete(&first).unwrap();

    assert_eq!(cache.get(&first).unwrap(), CacheLookup::Miss);
    assert_eq!(
        cache.get(&second).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"two"))
    );
}

#[test]
fn inference_cache_validates_names_keys_payloads_and_ttls_before_writing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(Options::new(dir.path())).unwrap();

    assert!(matches!(
        InferenceCache::new(engine.clone(), b""),
        Err(Error::InvalidArgument(message)) if message.contains("namespace")
    ));
    assert!(matches!(
        InferenceCache::new(engine.clone(), vec![b'n'; 16 * 1024 + 1]),
        Err(Error::InvalidArgument(message)) if message.contains("namespace")
    ));
    let cache = InferenceCache::new(engine, b"validated").unwrap();

    assert!(matches!(
        cache.get(&InferenceKey::new(b"", b"v1", b"input")),
        Err(Error::InvalidArgument(message)) if message.contains("model")
    ));
    assert!(matches!(
        cache.get(&InferenceKey::new(b"model", b"", b"input")),
        Err(Error::InvalidArgument(message)) if message.contains("model_version")
    ));
    assert!(matches!(
        cache.get(&key(b"v1", b"input").with_parameter(b"", b"value")),
        Err(Error::InvalidArgument(message)) if message.contains("parameter name")
    ));
    assert!(matches!(
        cache.get(&key(b"v1", &vec![b'i'; 16 * 1024 * 1024 + 1])),
        Err(Error::InvalidArgument(message)) if message.contains("input")
    ));
    assert!(matches!(
        cache.put(
            &key(b"v1", b"input"),
            InferenceEntry::new(vec![0; 16 * 1024 * 1024 + 1]),
            None,
        ),
        Err(Error::InvalidArgument(message)) if message.contains("payload")
    ));
    assert!(matches!(
        cache.put(
            &key(b"v1", b"input"),
            InferenceEntry::new(b"value"),
            Some(-1),
        ),
        Err(Error::InvalidArgument(message)) if message.contains("ttl_ms")
    ));
}

#[test]
fn inference_cache_singleflight_computes_one_value_for_32_concurrent_misses() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        InferenceCache::new(
            Engine::open(Options::new(dir.path())).unwrap(),
            b"singleflight",
        )
        .unwrap(),
    );
    let request = Arc::new(key(b"v1", b"hot prompt"));
    let start = Arc::new(Barrier::new(33));
    let computations = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();

    for _ in 0..32 {
        let cache = cache.clone();
        let request = request.clone();
        let start = start.clone();
        let computations = computations.clone();
        threads.push(thread::spawn(move || {
            start.wait();
            cache
                .get_or_compute(&request, Some(1_000), || {
                    computations.fetch_add(1, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(50));
                    Ok(InferenceEntry::new(b"shared"))
                })
                .unwrap()
        }));
    }
    start.wait();

    for thread in threads {
        assert_eq!(thread.join().unwrap(), InferenceEntry::new(b"shared"));
    }
    assert_eq!(computations.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache.get(&request).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"shared"))
    );
}

#[test]
fn inference_cache_singleflight_shares_the_leader_error_with_followers() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        InferenceCache::new(
            Engine::open(Options::new(dir.path())).unwrap(),
            b"singleflight-errors",
        )
        .unwrap(),
    );
    let request = Arc::new(key(b"v1", b"failing prompt"));
    let start = Arc::new(Barrier::new(17));
    let computations = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();

    for _ in 0..16 {
        let cache = cache.clone();
        let request = request.clone();
        let start = start.clone();
        let computations = computations.clone();
        threads.push(thread::spawn(move || {
            start.wait();
            cache
                .get_or_compute(&request, None, || {
                    computations.fetch_add(1, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(50));
                    Err(Error::InvalidArgument(
                        "model service rejected input".into(),
                    ))
                })
                .unwrap_err()
                .to_string()
        }));
    }
    start.wait();

    for thread in threads {
        assert_eq!(
            thread.join().unwrap(),
            "invalid argument: model service rejected input"
        );
    }
    assert_eq!(computations.load(Ordering::SeqCst), 1);
    assert_eq!(cache.get(&request).unwrap(), CacheLookup::Miss);
}

#[test]
fn inference_cache_reentrant_same_key_returns_a_structured_error() {
    let dir = tempfile::tempdir().unwrap();
    let cache = InferenceCache::new(
        Engine::open(Options::new(dir.path())).unwrap(),
        b"reentrant",
    )
    .unwrap();
    let request = key(b"v1", b"same key");

    let error = cache
        .get_or_compute(&request, None, || {
            cache.get_or_compute(&request, None, || Ok(InferenceEntry::new(b"nested")))
        })
        .unwrap_err();

    assert!(matches!(
        error,
        Error::Reentrant {
            operation: "inference-cache get_or_compute",
        }
    ));
    assert_eq!(cache.get(&request).unwrap(), CacheLookup::Miss);
}

#[test]
fn inference_cache_allows_same_thread_computation_for_an_unrelated_key() {
    let dir = tempfile::tempdir().unwrap();
    let cache = InferenceCache::new(
        Engine::open(Options::new(dir.path())).unwrap(),
        b"nested-unrelated",
    )
    .unwrap();
    let outer = key(b"v1", b"outer");
    let inner = key(b"v1", b"inner");

    let outer_entry = cache
        .get_or_compute(&outer, None, || {
            let inner_entry =
                cache.get_or_compute(&inner, None, || Ok(InferenceEntry::new(b"inner value")))?;
            assert_eq!(inner_entry, InferenceEntry::new(b"inner value"));
            Ok(InferenceEntry::new(b"outer value"))
        })
        .unwrap();

    assert_eq!(outer_entry, InferenceEntry::new(b"outer value"));
    assert_eq!(
        cache.get(&inner).unwrap(),
        CacheLookup::Hit(InferenceEntry::new(b"inner value"))
    );
}

fn raw_cache_entry(engine: &Engine) -> (Vec<u8>, Vec<u8>) {
    let entries = engine
        .scan_prefix(b"\0meteordb:inference-cache\0", 2)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    entries.into_iter().next().unwrap()
}

fn feature_record(score: f64, label: &[u8]) -> FeatureRecord {
    let mut record = FeatureRecord::new();
    record
        .insert(b"score", FeatureValue::F64(score))
        .unwrap()
        .insert(b"label", FeatureValue::Bytes(label.to_vec()))
        .unwrap();
    record
}

#[test]
fn feature_store_keys_are_delimiter_safe_ordered_and_namespace_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(Options::new(dir.path())).unwrap();
    let left = FeatureStore::new(engine.clone(), b"tenant\0a").unwrap();
    let right = FeatureStore::new(engine, b"tenant").unwrap();
    let ambiguous_left = FeatureKey::new(b"ab", b"c", b"group\0x", -1);
    let ambiguous_right = FeatureKey::new(b"a", b"bc", b"group\0x", -1);

    left.put(&ambiguous_left, &feature_record(1.0, b"left"), None)
        .unwrap();
    left.put(&ambiguous_right, &feature_record(2.0, b"right"), None)
        .unwrap();
    right
        .put(&ambiguous_left, &feature_record(3.0, b"other"), None)
        .unwrap();

    assert_eq!(
        left.get(&ambiguous_left).unwrap().unwrap().get(b"label"),
        Some(&FeatureValue::Bytes(b"left".to_vec()))
    );
    assert_eq!(
        left.get(&ambiguous_right).unwrap().unwrap().get(b"label"),
        Some(&FeatureValue::Bytes(b"right".to_vec()))
    );
    assert_eq!(
        right.get(&ambiguous_left).unwrap().unwrap().get(b"label"),
        Some(&FeatureValue::Bytes(b"other".to_vec()))
    );
}

#[test]
fn feature_store_latest_as_of_and_history_follow_signed_event_time() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        FeatureStore::new(Engine::open(Options::new(dir.path())).unwrap(), b"events").unwrap();
    for (time, score) in [(-10, 1.0), (0, 2.0), (25, 3.0)] {
        store
            .put(
                &FeatureKey::new(b"user", b"42", b"ranking", time),
                &feature_record(score, b"row"),
                None,
            )
            .unwrap();
    }

    assert_eq!(
        store
            .latest(b"user", b"42", b"ranking")
            .unwrap()
            .unwrap()
            .0
            .event_time(),
        25
    );
    assert_eq!(
        store
            .as_of(b"user", b"42", b"ranking", 7)
            .unwrap()
            .unwrap()
            .0
            .event_time(),
        0
    );
    assert_eq!(
        store
            .history(b"user", b"42", b"ranking", -10..=0, 10)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key.event_time())
            .collect::<Vec<_>>(),
        vec![-10, 0]
    );
    assert_eq!(
        store
            .history(b"user", b"42", b"ranking", i64::MIN..=i64::MAX, 2)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key.event_time())
            .collect::<Vec<_>>(),
        vec![-10, 0]
    );
}

#[test]
fn feature_store_group_scan_and_batch_read_use_complete_atomic_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        FeatureStore::new(Engine::open(Options::new(dir.path())).unwrap(), b"rows").unwrap();
    let first = FeatureKey::new(b"user", b"1", b"profile", 100);
    let second = FeatureKey::new(b"user", b"1", b"profile", 200);
    store
        .put(&first, &feature_record(1.0, b"first"), None)
        .unwrap();
    store
        .put(&second, &feature_record(2.0, b"second"), None)
        .unwrap();

    let rows = store.scan_group(b"user", b"1", b"profile", 10).unwrap();
    assert_eq!(
        rows.iter()
            .map(|(key, _)| key.event_time())
            .collect::<Vec<_>>(),
        vec![100, 200]
    );
    assert_eq!(
        store.get_many([&second, &first, &second]).unwrap(),
        vec![
            Some(feature_record(2.0, b"second")),
            Some(feature_record(1.0, b"first")),
            Some(feature_record(2.0, b"second")),
        ]
    );
}

#[test]
fn feature_store_ttl_and_typed_encoding_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(5_000));
    let engine = Engine::open_with_clock(Options::new(dir.path()), clock.clone()).unwrap();
    let store = FeatureStore::new(engine, b"ttl").unwrap();
    let key = FeatureKey::new(b"user", b"1", b"live", 10);
    let mut record = FeatureRecord::new();
    assert!(matches!(
        record.insert(b"bad", FeatureValue::F64(f64::NAN)),
        Err(Error::InvalidArgument(message)) if message.contains("finite")
    ));
    record
        .insert(b"flag", FeatureValue::Bool(true))
        .unwrap()
        .insert(b"count", FeatureValue::I64(-7))
        .unwrap()
        .insert(b"text", FeatureValue::String("meteor".into()))
        .unwrap();

    store.put(&key, &record, Some(10)).unwrap();
    assert_eq!(store.get(&key).unwrap(), Some(record));
    clock.set(5_010).unwrap();
    assert_eq!(store.get(&key).unwrap(), None);
}

#[test]
fn feature_record_rejects_projected_aggregate_over_encoded_limit_before_mutation() {
    let mut record = FeatureRecord::new();
    record
        .insert(b"first", FeatureValue::Bytes(vec![0; 8 * 1024 * 1024]))
        .unwrap();

    assert!(matches!(
        record.insert(b"second", FeatureValue::Bytes(vec![0; 8 * 1024 * 1024])),
        Err(Error::InvalidArgument(message)) if message.contains("encoded feature row")
    ));
    assert!(record.get(b"second").is_none());
    assert!(matches!(
        record.get(b"first"),
        Some(FeatureValue::Bytes(bytes)) if bytes.len() == 8 * 1024 * 1024
    ));
}

#[test]
fn embedding_validates_lengths_dimensions_finiteness_and_little_endian_bytes() {
    assert!(matches!(
        Embedding::from_bytes(0, ScalarType::F32, Vec::new()),
        Err(Error::InvalidArgument(message)) if message.contains("dimension")
    ));
    assert!(matches!(
        Embedding::from_bytes(2, ScalarType::F32, vec![0; 7]),
        Err(Error::InvalidArgument(message)) if message.contains("8")
    ));
    assert!(matches!(
        Embedding::from_bytes(2, ScalarType::F16, vec![0; 6]),
        Err(Error::InvalidArgument(message)) if message.contains("4")
    ));
    assert!(matches!(
        Embedding::from_f32(&[1.0, f32::INFINITY]),
        Err(Error::InvalidArgument(message)) if message.contains("finite")
    ));
    assert!(matches!(
        Embedding::from_f16_bits(&[0x3c00, 0x7e00]),
        Err(Error::InvalidArgument(message)) if message.contains("finite")
    ));

    let embedding = Embedding::from_f32(&[1.0, -2.5]).unwrap();
    assert_eq!(embedding.dimension(), 2);
    assert_eq!(embedding.scalar_type(), ScalarType::F32);
    assert_eq!(
        embedding.vector_bytes(),
        [1.0f32.to_le_bytes(), (-2.5f32).to_le_bytes()].concat()
    );
    assert_eq!(embedding.to_f32().unwrap(), vec![1.0, -2.5]);
    assert!(matches!(
        embedding.to_f16_bits(),
        Err(Error::InvalidArgument(message)) if message.contains("F16")
    ));
}

#[test]
fn embedding_metadata_replacement_checks_projected_total_before_mutation() {
    let mut embedding = Embedding::from_f32(&[1.0]).unwrap();
    for index in 0_u8..16 {
        embedding
            .insert_metadata([index], vec![0; 1024 * 1024 - 1])
            .unwrap();
    }

    embedding
        .insert_metadata([0], vec![1; 1024 * 1024 - 1])
        .unwrap();
    assert!(matches!(
        embedding.insert_metadata([0], vec![2; 1024 * 1024]),
        Err(Error::InvalidArgument(message)) if message.contains("metadata")
    ));
    assert_eq!(
        embedding.metadata().get(&[0][..]).unwrap(),
        &vec![1; 1024 * 1024 - 1]
    );
}

#[test]
fn embedding_store_isolates_models_preserves_metadata_and_batch_order() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        EmbeddingStore::new(Engine::open(Options::new(dir.path())).unwrap(), b"tenant").unwrap();
    let v1 = EmbeddingKey::new(b"doc\0one").with_model(b"encoder", b"v1");
    let v2 = EmbeddingKey::new(b"doc\0one").with_model(b"encoder", b"v2");
    let missing = EmbeddingKey::new(b"missing").with_model(b"encoder", b"v1");
    let mut first = Embedding::from_f32(&[1.0, 2.0]).unwrap();
    first
        .set_model(b"encoder", b"v1")
        .insert_metadata(b"source", b"docs")
        .unwrap();
    let mut second = Embedding::from_f16_bits(&[0x3c00, 0xc000]).unwrap();
    second.set_model(b"encoder", b"v2");

    store.put(&v1, &first, None).unwrap();
    store.put(&v2, &second, None).unwrap();

    let results = store.get_many([&v2, &missing, &v1, &v2]).unwrap();
    assert_eq!(
        results,
        vec![
            Some(second.clone()),
            None,
            Some(first.clone()),
            Some(second)
        ]
    );
    assert_eq!(
        results[2]
            .as_ref()
            .unwrap()
            .metadata()
            .get(b"source".as_slice()),
        Some(&b"docs".to_vec())
    );
    assert!(matches!(
        store.put(&v2, &first, None),
        Err(Error::InvalidArgument(message)) if message.contains("model")
    ));
}

#[test]
fn embedding_batch_put_is_atomic_and_restart_round_trips_portable_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options::new(dir.path());
    let first_key = EmbeddingKey::new(b"a");
    let second_key = EmbeddingKey::new(b"b");
    let first = Embedding::from_f32(&[0.25, -0.5, 4.0]).unwrap();
    let second = Embedding::from_f16_bits(&[0x0000, 0x3c00, 0xc000]).unwrap();
    {
        let store =
            EmbeddingStore::new(Engine::open(options.clone()).unwrap(), b"restart").unwrap();
        store
            .put_many([(&first_key, &first), (&second_key, &second)])
            .unwrap();
    }

    let store = EmbeddingStore::new(Engine::open(options).unwrap(), b"restart").unwrap();
    assert_eq!(store.get(&first_key).unwrap(), Some(first));
    assert_eq!(store.get(&second_key).unwrap(), Some(second));
}
