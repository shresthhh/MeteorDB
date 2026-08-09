# Changelog

All notable changes are documented here.

## [Unreleased]

## [1.0.0] - 2026-08-09

### Added

- Durable LSM engine with WAL/manifest recovery, SSTables, scans, compaction,
  MVCC snapshots, TTL, cache/statistics, and explicit durability modes.
- Inference-cache, feature-store, and embedding-storage adapters.
- Bounded read-only inspector, component benchmarks, deterministic comparison
  harness, reliability suites, and fuzz smoke workflow.
- Public architecture, format, durability, operations, benchmark, development,
  and release documentation.

### Compatibility

- Stable public API version 1 and database format generation 1.
- Generation 1 writes SSTable and manifest component version 2 and reads their
  component versions 1 and 2. WAL batches, engine values, and adapter schemas
  remain component/schema version 1.
- Unknown component versions are rejected. Forward compatibility, automatic
  cross-generation migration, and RocksDB compatibility are not provided.
