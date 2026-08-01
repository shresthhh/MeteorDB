use std::io::ErrorKind;

use meteordb::{Engine, Options, Result, WriteBatch};

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("meteordb-quickstart");
    remove_database(&path)?;

    let result = run(&path);
    let cleanup = remove_database(&path);
    result?;
    cleanup?;
    Ok(())
}

fn run(path: &std::path::Path) -> Result<()> {
    let engine = Engine::open(Options::new(path))?;

    let mut batch = WriteBatch::default();
    batch
        .put("user:42", "Ada")
        .put("profile:42", "systems researcher");
    engine.write(batch)?;

    let snapshot = engine.snapshot()?;
    engine.put("profile:42", "database engineer")?;

    assert_eq!(
        snapshot.get("profile:42")?.as_deref(),
        Some(b"systems researcher".as_slice())
    );

    let current = engine.get("profile:42")?.expect("profile should exist");
    println!("current profile: {}", String::from_utf8_lossy(&current));

    drop(snapshot);
    engine.close()
}

fn remove_database(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
