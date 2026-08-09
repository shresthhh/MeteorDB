# Changelog

All notable changes are documented here. MeteorDB is pre-alpha and does not yet
promise API or file-format compatibility.

## [Unreleased]

### Added

- Durable LSM engine with WAL/manifest recovery, SSTables, scans, compaction,
  MVCC snapshots, TTL, cache/statistics, and explicit durability modes.
- Inference-cache, feature-store, and embedding-storage adapters.
- Bounded read-only inspector, component benchmarks, deterministic comparison
  harness, reliability suites, and fuzz smoke workflow.
- Public architecture, format, durability, operations, benchmark, development,
  and release documentation.

### Compatibility

- Current adapter and external JSON schemas are version 1.
- Current SSTable and manifest writer versions are 2; compatible readers accept
  the documented version-1 predecessors.
- No migration guarantee or RocksDB compatibility is provided.
