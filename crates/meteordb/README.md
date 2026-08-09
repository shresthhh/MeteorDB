# meteordb

`meteordb` is a pre-alpha embedded ordered key/value engine with atomic batches,
WAL recovery, MVCC snapshots and scans, SSTables, Bloom filters, block cache,
leveled compaction, TTLs, and typed AI-workload storage adapters.

```rust
use meteordb::{Engine, Options};

# fn run(path: &std::path::Path) -> meteordb::Result<()> {
let db = Engine::open(Options::new(path))?;
db.put("key", "value")?;
assert_eq!(db.get("key")?.as_deref(), Some(&b"value"[..]));
db.close()
# }
```

API and persistent formats may change without migration support. MeteorDB does
not provide ANN search or RocksDB format/API compatibility. See the
[repository README](../../README.md), [durability contract](../../docs/durability.md),
and [file-format policy](../../docs/file-formats.md).
