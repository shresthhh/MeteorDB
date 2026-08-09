# Workspace crates

| Crate | Purpose | Publication |
| --- | --- | --- |
| [`meteordb`](meteordb) | Embedded engine and workload adapters | public library |
| [`meteordb-cli`](meteordb-cli) | Read-only inspector and diagnostic benchmark | repository tool |
| [`meteordb-bench-support`](meteordb-bench-support) | Shared deterministic workload/result schema | internal |
| [`meteordb-rocks-bench`](meteordb-rocks-bench) | MeteorDB/RocksDB comparison frontend | internal |

The optional RocksDB build is a benchmark comparator, not a compatibility
layer. Workspace commands intentionally compile the comparator without its
native `rocksdb-engine` feature.
