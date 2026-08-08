# Source modules

`lib.rs` keeps modules private and re-exports the supported crate-root API.
The collaboration column names modules or external crates that each row
directly uses; it does not list standard-library building blocks or modules
that merely consume the row.

## Public coordination and contracts

| Module | Responsibility | Direct collaborators used |
| --- | --- | --- |
| `lib` | Declares modules and assembles the crate-root API. | All library modules |
| `engine` | Coordinates open, recovery, writes, reads, snapshots, rotation, flush, synchronization, and close. | `background`, `batch`, `cache`, `error`, `fs`, `internal_key`, `manifest`, `memtable`, `options`, `snapshot`, `sstable`, `stats`, `version`, `wal` |
| `options` | Defines and validates durability, compression, size, and resource settings. | `error` |
| `batch` | Owns ordered atomic mutations and their WAL encoding. | No other crate module |
| `error` | Defines structured public failures. | `thiserror` |

## Write path and recovery

| Module | Responsibility | Direct collaborators used |
| --- | --- | --- |
| `wal` | Frames, checksums, synchronizes, and replays atomic batches. | `batch`, `error`, `fs`, `internal_key`, `options`, `crc32c` |
| `manifest` | Owns `LOCK`, `CURRENT`, append-only version edits, recovery counters, and version publication. | `error`, `fs`, `internal_key`, `version`, `fs2`, `crc32c` |
| `background` | Provides synchronization signals for flush progress and worker wakeups. | No other crate module |

## MVCC and read path

| Module | Responsibility | Direct collaborators used |
| --- | --- | --- |
| `internal_key` | Encodes user keys with sequence and value kind in MVCC sort order. | `error` |
| `memtable` | Stores ordered in-memory versions, tombstones, and batch results. | `batch`, `error`, `internal_key` |
| `snapshot` | Registers active read sequences with RAII guards. | `internal_key::SequenceNumber` |
| `version` | Validates immutable live-file metadata and level lookup rules. | `internal_key`, `error` |
| `bloom` | Builds and checks deterministic probabilistic key filters. | `error` |
| `cache` | Maintains independently budgeted LRU metadata and data-block partitions. | `error` |

## Table format and metadata

| Module | Responsibility | Direct collaborators used |
| --- | --- | --- |
| [`sstable`](sstable) | Builds and reads immutable tables, blocks, filters, properties, and fixed footers. | `bloom`, `cache`, `error`, `fs`, `internal_key`, `options`, `stats`, `crc32c`, `snap` |

## Observability and platform I/O

| Module | Responsibility | Direct collaborators used |
| --- | --- | --- |
| `stats` | Aggregates point-read, Bloom, level-probe, and cache counters. | `cache`, `version::NUM_LEVELS` |
| `clock` | Exposes the public clock abstraction reserved for deterministic expiration integration; no current engine path consumes it or enforces TTL. | No other crate module |
| `fs` | Defines durable file operations and the production OS implementation. | `libc` on Unix |

For system-level data flow and invariants, see the
[architecture reference](../../../docs/architecture.md).
