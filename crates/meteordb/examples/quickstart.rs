use std::sync::Arc;

use meteordb::{Engine, ManualClock, Options, Result, WriteBatch};

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let database = tempfile::tempdir()?;
    run(database.path())?;
    Ok(())
}

fn run(path: &std::path::Path) -> Result<()> {
    let clock = ManualClock::new(1_000);
    let engine = Engine::open_with_clock(Options::new(path), Arc::new(clock.clone()))?;

    let mut batch = WriteBatch::default();
    batch
        .put("profile:42", "database engineer")
        .put("profile:7", "storage engineer");
    engine.write(batch)?;
    assert_eq!(
        engine.get("profile:42")?.as_deref(),
        Some(&b"database engineer"[..])
    );

    let snapshot = engine.snapshot()?;
    engine.put("profile:42", "systems researcher")?;
    assert_eq!(
        snapshot.get("profile:42")?.as_deref(),
        Some(&b"database engineer"[..])
    );

    let profiles = engine
        .scan_prefix("profile:", 10)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(profiles.len(), 2);

    engine.put_with_ttl("session:42", "active", 500)?;
    assert_eq!(engine.get("session:42")?.as_deref(), Some(&b"active"[..]));
    clock.set(1_500)?;
    assert_eq!(engine.get("session:42")?, None);

    println!("profiles: {}", profiles.len());
    drop(snapshot);
    engine.close()
}
