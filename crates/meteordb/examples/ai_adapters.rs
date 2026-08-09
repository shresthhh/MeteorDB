use std::sync::Arc;

use meteordb::{
    CacheLookup, Embedding, EmbeddingKey, EmbeddingStore, Engine, FeatureKey, FeatureRecord,
    FeatureStore, FeatureValue, InferenceCache, InferenceEntry, InferenceKey, ManualClock, Options,
    Result,
};

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let database = tempfile::tempdir()?;
    run(database.path())?;
    Ok(())
}

fn run(path: &std::path::Path) -> Result<()> {
    let clock = ManualClock::new(10_000);
    let engine = Engine::open_with_clock(Options::new(path), Arc::new(clock.clone()))?;

    let cache = InferenceCache::new(engine.clone(), "demo")?;
    let request = InferenceKey::new("model", "v1", b"hello");
    cache.put(&request, InferenceEntry::new("world"), Some(1_000))?;
    assert!(matches!(cache.get(&request)?, CacheLookup::Hit(_)));
    clock.set(11_000)?;
    assert_eq!(cache.get(&request)?, CacheLookup::Miss);

    let features = FeatureStore::new(engine.clone(), "online")?;
    let feature_key = FeatureKey::new("user", "42", "ranking", 100);
    let mut row = FeatureRecord::new();
    row.insert("active", FeatureValue::Bool(true))?;
    features.put(&feature_key, &row, None)?;
    assert_eq!(
        features
            .get(&feature_key)?
            .and_then(|row| row.get("active").cloned()),
        Some(FeatureValue::Bool(true))
    );

    let embeddings = EmbeddingStore::new(engine.clone(), "catalog")?;
    let first_key = EmbeddingKey::new("item-1").with_model("encoder", "v1");
    let second_key = EmbeddingKey::new("item-2").with_model("encoder", "v1");
    let mut first = Embedding::from_f32(&[1.0, 0.0])?;
    first.set_model("encoder", "v1");
    let mut second = Embedding::from_f32(&[0.0, 1.0])?;
    second.set_model("encoder", "v1");
    embeddings.put_many([(&first_key, &first), (&second_key, &second)])?;
    let found = embeddings.get_many([&second_key, &first_key])?;
    assert_eq!(found[0].as_ref().map(Embedding::dimension), Some(2));
    assert_eq!(found[1].as_ref().map(Embedding::dimension), Some(2));

    println!("AI adapters: ok");
    engine.close()
}
