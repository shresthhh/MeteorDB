# Integration tests

Each file is a Cargo integration-test target that exercises exported contracts
or behavior spanning multiple source modules.

| Target | Responsibility |
| --- | --- |
| `block_format` | Bloom behavior, prefix-compressed block ordering and seeking, canonical varints, handles, checksums, and malformed block rejection |
| `manifest` | Version edits, level invariants, durable publication order, locking, file-number monotonicity, manifest recovery, and metadata corruption |
| `memtable` | MVCC visibility through the engine, atomic batches, WAL failure behavior, buffered synchronization, close, and concurrent open behavior |
| `mvcc` | Internal-key ordering/encoding and snapshot registry semantics, including generated ordering properties |
| `public_contract` | Exact public validation and owned write-batch behavior |
| `read_path` | Cache partitioning and statistics, memory/table lookup order, Bloom avoidance, level probes, snapshots, tombstones, and surfaced table corruption |
| `recovery` | Restart durability, WAL ownership, flush publication, stalls and background failures, cleanup, replay scheduling, missing files, and sequence gaps |
| `sstable` | Complete table build/read/iteration, compression, resource limits, canonical physical layout, properties, and corruption handling |
| `wal` | Fragmentation, torn tails, checksums, durability modes, injected I/O failures, format validation, and batch limits |

## Choosing test placement

- Add a **unit test** beside private code for a focused decoder, helper, or
  data-structure invariant that does not require the public crate boundary.
- Add an **integration test** here for public behavior or an invariant crossing
  the WAL, memtable, manifest, SSTable, cache, or engine boundary.
- Add a **property test** when generated operation sequences or byte layouts
  express an ordering or round-trip invariant better than hand-picked cases.
- Add a **corruption test** whenever persistent decoding, checksums, lengths,
  ordering, or format metadata changes; malformed complete bytes must return a
  typed error.
- Add a **crash/fault-injection test** when changing append, sync, rename,
  installation, publication, cleanup, or WAL-retirement ordering. Assert the
  relevant filesystem event order, not only the final result.

Run one target with `cargo test -p meteordb --test <target>`. See the
[development guide](../../../docs/development.md#storage-engine-tests) for the
full storage-test and validation requirements.
