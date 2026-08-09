# Roadmap and status

MeteorDB 1.x has a stable public API version and database-format compatibility
generation. Checked items are implemented; unchecked items are directional and
are not delivery commitments.

## Implemented

- [x] Checksummed WAL, atomic batches, recovery, and durable manifest
- [x] MVCC point reads, snapshots, ordered range/prefix scans
- [x] Memtable rotation, background flush, immutable SSTables, Bloom filters
- [x] Partitioned block cache and structured statistics
- [x] Leveled compaction with snapshot-safe obsolete-file reclamation
- [x] Wall-clock TTL visibility and expiry-aware compaction
- [x] Inference-cache, feature-store, and embedding-storage adapters
- [x] Bounded read-only inspection CLI and benchmark smoke
- [x] Deterministic component and MeteorDB/RocksDB comparison harnesses
- [x] Corruption, crash-recovery, concurrency, model, fuzz, and property tests
- [x] Public API version 1 and database format generation 1 compatibility policy

## Planned hardening

- [ ] Backup/checkpoint and restore tooling
- [ ] Explicit tooling for any future cross-generation migration
- [ ] Offline repair/salvage workflow
- [ ] Continuous compaction scheduling and richer write-stall telemetry
- [ ] Encryption-at-rest integration guidance
- [ ] Published repeatable results from disclosed hardware (not universal
      performance claims)

## Explicit non-goals

- Approximate-nearest-neighbor indexing or similarity search
- RocksDB/LevelDB file, WAL, manifest, API, or options compatibility
- SQL, relational schemas, or a query planner
- Distributed consensus, replication, sharding, or a network server
- General multi-key transactions beyond atomic write batches and read snapshots
- Model serving, feature computation, or orchestration

Embedding values are storage records. Applications needing ANN should use a
dedicated vector index and treat MeteorDB as metadata/value storage.
