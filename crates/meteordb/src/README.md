# Source modules

`lib.rs` keeps modules private and re-exports the supported crate-root API.
Dependencies below name the main internal or external collaborators, not
standard-library building blocks.

## Public coordination and contracts

| Module | Responsibility | Primary dependencies |
| --- | --- | --- |
| `lib` | Declares modules and assembles the crate-root API. | All library modules |
| `engine` | Coordinates open, recovery, writes, reads, snapshots, rotation, flush, synchronization, and close. | `background`, `wal`, `memtable`, `sstable`, `manifest`, `version`, `snapshot`, `cache`, `stats`, `fs`, `options` |
| `options` | Defines and validates durability, compression, size, and resource settings. | `error` |
| `batch` | Owns ordered atomic mutations and their WAL encoding. | No other crate module |
| `error` | Defines structured public failures. | `thiserror` |

## Write path and recovery

| Module | Responsibility | Primary dependencies |
| --- | --- | --- |
| `wal` | Frames, checksums, synchronizes, and replays atomic batches. | `batch`, `fs`, `options`, `internal_key`, `crc32c` |
| `manifest` | Owns `LOCK`, `CURRENT`, append-only version edits, recovery counters, and version publication. | `fs`, `version`, `internal_key`, `fs2`, `crc32c` |
| `background` | Signals the flush worker and callers waiting for flush progress. | Used by `engine` |

## MVCC and read path

| Module | Responsibility | Primary dependencies |
| --- | --- | --- |
| `internal_key` | Encodes user keys with sequence and value kind in MVCC sort order. | `error` |
| `memtable` | Stores ordered in-memory versions, tombstones, and batch results. | `internal_key`, `batch` |
| `snapshot` | Registers active read sequences with RAII guards. | `internal_key::SequenceNumber` |
| `version` | Validates immutable live-file metadata and level lookup rules. | `internal_key`, `error` |
| `bloom` | Builds and checks deterministic probabilistic key filters. | `error` |
| `cache` | Maintains independently budgeted LRU metadata and data-block partitions. | `error`; consumed by `sstable::reader` and `stats` |

## Table format and metadata

| Module | Responsibility | Primary dependencies |
| --- | --- | --- |
| [`sstable`](sstable) | Builds and reads immutable tables, blocks, filters, properties, and fixed footers. | `internal_key`, `bloom`, `cache`, `stats`, `fs`, `options`, `crc32c`, `snap` |

## Observability and platform I/O

| Module | Responsibility | Primary dependencies |
| --- | --- | --- |
| `stats` | Aggregates point-read, Bloom, level-probe, and cache counters. | `cache`, `version::NUM_LEVELS` |
| `clock` | Supplies injectable wall-clock time for expiration decisions. | No other crate module |
| `fs` | Defines durable file operations and the production OS implementation. | `libc` on Unix |

For system-level data flow and invariants, see the
[architecture reference](../../../docs/architecture.md).
