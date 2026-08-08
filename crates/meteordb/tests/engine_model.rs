use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use meteordb::{Engine, ManualClock, Options, ScanBounds, Snapshot, WriteBatch};

const SEEDS: [u64; 4] = [
    0x4d45_5445_4f52_0010,
    0x4d45_5445_4f52_1010,
    0x4d45_5445_4f52_2010,
    0x4d45_5445_4f52_3010,
];

#[derive(Clone)]
struct Version {
    sequence: u64,
    value: Option<Vec<u8>>,
    expires_at: Option<u64>,
}

#[derive(Default)]
struct Model {
    versions: BTreeMap<Vec<u8>, Vec<Version>>,
    sequence: u64,
}

impl Model {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<u64>) {
        self.sequence += 1;
        self.versions.entry(key).or_default().push(Version {
            sequence: self.sequence,
            value: Some(value),
            expires_at,
        });
    }

    fn delete(&mut self, key: Vec<u8>) {
        self.sequence += 1;
        self.versions.entry(key).or_default().push(Version {
            sequence: self.sequence,
            value: None,
            expires_at: None,
        });
    }

    fn get(&self, key: &[u8], sequence: u64, now: u64) -> Option<Vec<u8>> {
        self.versions.get(key)?.iter().rev().find_map(|version| {
            (version.sequence <= sequence).then(|| {
                if version.expires_at.is_some_and(|deadline| deadline <= now) {
                    None
                } else {
                    version.value.clone()
                }
            })
        })?
    }

    fn scan(
        &self,
        sequence: u64,
        now: u64,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        prefix: Option<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.versions
            .keys()
            .filter(|key| in_bounds(key, start, end))
            .filter(|key| prefix.is_none_or(|prefix| key.starts_with(prefix)))
            .filter_map(|key| {
                self.get(key, sequence, now)
                    .map(|value| (key.clone(), value))
            })
            .take(limit)
            .collect()
    }
}

#[test]
fn deterministic_state_machine_matches_reference_mvcc_ttl_model() {
    for seed in SEEDS {
        run_seed(seed);
    }
}

fn run_seed(seed: u64) {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(10_000));
    let options = options(dir.path());
    let mut db = Engine::open_with_clock(options.clone(), clock.clone()).unwrap();
    let mut model = Model::default();
    let mut snapshots: Vec<(Snapshot, u64)> = Vec::new();
    let mut rng = Rng(seed);
    let mut now = 10_000;

    for step in 0..240 {
        let key = generated_key(rng.pick(12));
        match rng.pick(12) {
            0 | 1 => {
                let value = generated_value(step, rng.next());
                db.put(&key, &value).unwrap();
                model.put(key, value, None);
            }
            2 => {
                db.delete(&key).unwrap();
                model.delete(key);
            }
            3 => {
                let first = key;
                let second = generated_key(rng.pick(12));
                let first_value = generated_value(step, rng.next());
                let second_value = generated_value(step + 1, rng.next());
                let mut batch = WriteBatch::default();
                batch.put(&first, &first_value).put(&second, &second_value);
                db.write(batch).unwrap();
                model.put(first, first_value, None);
                model.put(second, second_value, None);
            }
            4 => {
                let ttl = 1 + rng.pick(20);
                let value = generated_value(step, rng.next());
                db.put_with_ttl(&key, &value, i64::try_from(ttl).unwrap())
                    .unwrap();
                model.put(key, value, Some(now + ttl));
            }
            5 if snapshots.len() < 6 => {
                snapshots.push((db.snapshot().unwrap(), model.sequence));
            }
            6 => {
                assert_eq!(
                    db.get(&key).unwrap(),
                    model.get(&key, model.sequence, now),
                    "seed={seed:#x} step={step} point get"
                );
            }
            7 => compare_scan(&db, &model, model.sequence, now, seed, step),
            8 => compare_prefix(&db, &model, model.sequence, now, seed, step),
            9 => db.flush().unwrap(),
            10 => {
                db.flush().unwrap();
                let _ = db.compact().unwrap();
            }
            _ => {
                now += 1 + rng.pick(5);
                clock.set(now).unwrap();
                if snapshots.is_empty() && step % 17 == 0 {
                    db.close().unwrap();
                    drop(db);
                    db = Engine::open_with_clock(options.clone(), clock.clone()).unwrap();
                }
            }
        }

        assert_current(&db, &model, now, seed, step);
        for (snapshot, sequence) in &snapshots {
            assert_snapshot(snapshot, &model, *sequence, now, seed, step);
        }
        if step % 41 == 40 {
            snapshots.clear();
            db.flush().unwrap();
            drop(db);
            db = Engine::open_with_clock(options.clone(), clock.clone()).unwrap();
            assert_current(&db, &model, now, seed, step);
        }
    }
}

fn options(path: &Path) -> Options {
    let mut options = Options::new(path);
    options.memtable_bytes = 192;
    options.target_sstable_bytes = 256;
    options.block_bytes = 64;
    options.restart_interval = 2;
    options.max_key_bytes = 64;
    options.max_value_bytes = 128;
    options.max_batch_bytes = 1024;
    options
}

fn assert_current(db: &Engine, model: &Model, now: u64, seed: u64, step: usize) {
    for index in 0..12 {
        let key = generated_key(index);
        assert_eq!(
            db.get(&key).unwrap(),
            model.get(&key, model.sequence, now),
            "seed={seed:#x} step={step} key={:?}",
            String::from_utf8_lossy(&key)
        );
    }
    compare_scan(db, model, model.sequence, now, seed, step);
    compare_prefix(db, model, model.sequence, now, seed, step);
}

fn assert_snapshot(
    snapshot: &Snapshot,
    model: &Model,
    sequence: u64,
    now: u64,
    seed: u64,
    step: usize,
) {
    for index in 0..12 {
        let key = generated_key(index);
        assert_eq!(
            snapshot.get(&key).unwrap(),
            model.get(&key, sequence, now),
            "seed={seed:#x} step={step} snapshot={sequence} key={:?}",
            String::from_utf8_lossy(&key)
        );
    }
    let actual = collect(snapshot.scan(ScanBounds::all(), 7).unwrap());
    let expected = model.scan(sequence, now, Bound::Unbounded, Bound::Unbounded, None, 7);
    assert_eq!(actual, expected, "seed={seed:#x} step={step} snapshot scan");
}

fn compare_scan(db: &Engine, model: &Model, sequence: u64, now: u64, seed: u64, step: usize) {
    let start = Bound::Included(b"a1".to_vec());
    let end = Bound::Excluded(b"c2".to_vec());
    let actual = collect(
        db.scan(ScanBounds::new(start.clone(), end.clone()), 7)
            .unwrap(),
    );
    let expected = model.scan(
        sequence,
        now,
        start.as_ref().map(Vec::as_slice),
        end.as_ref().map(Vec::as_slice),
        None,
        7,
    );
    assert_eq!(actual, expected, "seed={seed:#x} step={step} bounded scan");
}

fn compare_prefix(db: &Engine, model: &Model, sequence: u64, now: u64, seed: u64, step: usize) {
    let actual = collect(db.scan_prefix(b"b", 5).unwrap());
    let expected = model.scan(
        sequence,
        now,
        Bound::Unbounded,
        Bound::Unbounded,
        Some(b"b"),
        5,
    );
    assert_eq!(actual, expected, "seed={seed:#x} step={step} prefix scan");
}

fn collect(
    iterator: impl Iterator<Item = meteordb::Result<(Vec<u8>, Vec<u8>)>>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    iterator.collect::<meteordb::Result<Vec<_>>>().unwrap()
}

fn in_bounds(key: &[u8], start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let after_start = match start {
        Bound::Included(start) => key >= start,
        Bound::Excluded(start) => key > start,
        Bound::Unbounded => true,
    };
    let before_end = match end {
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
        Bound::Unbounded => true,
    };
    after_start && before_end
}

fn generated_key(index: u64) -> Vec<u8> {
    let prefix = [b'a', b'b', b'c'][usize::try_from(index % 3).unwrap()];
    vec![prefix, b'0' + u8::try_from(index / 3).unwrap()]
}

fn generated_value(step: usize, random: u64) -> Vec<u8> {
    format!("v-{step:03}-{random:016x}").into_bytes()
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn pick(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}
