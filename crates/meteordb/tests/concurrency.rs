use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};

use meteordb::{Engine, Options};

fn options(path: &Path) -> Options {
    let mut options = Options::new(path);
    options.memtable_bytes = 128;
    options.target_sstable_bytes = 128;
    options.block_bytes = 64;
    options
}

#[test]
fn writer_snapshot_reader_flush_and_compaction_follow_barrier_schedule() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(options(dir.path())).unwrap();
    db.put(b"key", b"value-0").unwrap();
    db.flush().unwrap();

    let captured = Arc::new(Barrier::new(3));
    let written = Arc::new(Barrier::new(3));
    let maintained = Arc::new(Barrier::new(3));

    let writer = {
        let db = db.clone();
        let captured = captured.clone();
        let written = written.clone();
        let maintained = maintained.clone();
        std::thread::spawn(move || {
            for round in 1..=12 {
                captured.wait();
                db.put(b"key", format!("value-{round}"))?;
                written.wait();
                maintained.wait();
            }
            meteordb::Result::Ok(())
        })
    };
    let reader = {
        let db = db.clone();
        let captured = captured.clone();
        let written = written.clone();
        let maintained = maintained.clone();
        std::thread::spawn(move || {
            for round in 1..=12 {
                let snapshot = db.snapshot()?;
                captured.wait();
                written.wait();
                assert_eq!(
                    snapshot.get(b"key")?.as_deref(),
                    Some(format!("value-{}", round - 1).as_bytes()),
                    "seed=0xc011_ab1e round={round}"
                );
                maintained.wait();
            }
            meteordb::Result::Ok(())
        })
    };
    let maintainer = {
        let db = db.clone();
        std::thread::spawn(move || {
            for _ in 1..=12 {
                captured.wait();
                written.wait();
                db.flush()?;
                let _ = db.compact()?;
                maintained.wait();
            }
            meteordb::Result::Ok(())
        })
    };

    writer.join().unwrap().unwrap();
    reader.join().unwrap().unwrap();
    maintainer.join().unwrap().unwrap();
    assert_eq!(db.get(b"key").unwrap().as_deref(), Some(&b"value-12"[..]));
}

#[test]
fn snapshot_keeps_every_referenced_sstable_alive_during_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(options(dir.path())).unwrap();
    for generation in 0..6 {
        db.put(b"history", format!("value-{generation}")).unwrap();
        db.put(
            format!("filler-{generation}"),
            vec![b'a' + u8::try_from(generation).unwrap(); 96],
        )
        .unwrap();
        db.flush().unwrap();
    }
    let snapshot = db.snapshot().unwrap();
    let mut old_scan = snapshot
        .scan(meteordb::ScanBounds::all(), usize::MAX)
        .unwrap();
    let referenced = sstables(dir.path());

    db.put(b"history", b"latest").unwrap();
    db.flush().unwrap();
    assert!(db.compact().unwrap());

    for path in referenced {
        assert!(
            path.exists(),
            "compaction removed a file still referenced by an old version: {}",
            path.display()
        );
    }
    assert_eq!(
        snapshot.get(b"history").unwrap().as_deref(),
        Some(&b"value-5"[..])
    );
    assert!(old_scan.next().transpose().unwrap().is_some());
    assert_eq!(db.get(b"history").unwrap().as_deref(), Some(&b"latest"[..]));
}

fn sstables(path: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .collect()
}
