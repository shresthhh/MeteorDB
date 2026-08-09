# meteordb

`meteordb` is an embedded ordered key/value engine with atomic batches, WAL
recovery, MVCC snapshots and scans, SSTables, Bloom filters, block cache,
leveled compaction, TTLs, and typed AI-workload storage adapters. Version 1.x
implements public API version 1 and database format generation 1.

```rust
use meteordb::{Engine, Options};

# fn run(path: &std::path::Path) -> meteordb::Result<()> {
let db = Engine::open(Options::new(path))?;
db.put("key", "value")?;
assert_eq!(db.get("key")?.as_deref(), Some(&b"value"[..]));
db.close()
# }
```

Component format versions and the exact generation-1 read/write contract are
documented in the file-format policy. MeteorDB does not provide ANN search or
RocksDB format/API compatibility. See the
[repository README](../../README.md), [durability contract](../../docs/durability.md),
and [file-format policy](../../docs/file-formats.md).
