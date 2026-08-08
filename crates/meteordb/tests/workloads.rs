use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use meteordb::{
    CacheLookup, Engine, Error, InferenceCache, InferenceEntry, InferenceKey, ManualClock, Options,
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
