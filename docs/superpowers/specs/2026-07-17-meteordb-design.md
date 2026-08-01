# MeteorDB Design

**Date:** 2026-07-17  
**Status:** Approved  
**Language:** Rust  
**Deployment model:** Embedded, single-node library

## 1. Product Definition

MeteorDB is a high-quality embedded storage engine for AI-serving workloads. It
uses a leveled log-structured merge tree (LSM tree) and exposes a general
ordered byte key/value API. First-class adapters optimize common inference
cache, feature store, and embedding storage access patterns.

MeteorDB is a competing design, not an API-compatible or file-format-compatible
replacement for RocksDB. The project prioritizes correctness, observability,
clear subsystem boundaries, and reproducible evidence over feature count.

### 1.1 Why an LSM Tree

An LSM tree turns random writes into sequential writes:

1. A write is appended to a write-ahead log (WAL) for durability.
2. The same write enters an ordered in-memory table.
3. Full memory tables are flushed into immutable sorted-string table (SSTable)
   files.
4. Background compaction merges SSTables to control lookup and storage costs.

This design is a strong fit for inference caches and feature stores because
they commonly combine high write rates with latency-sensitive point reads.

### 1.2 Goals

- Durable atomic write batches.
- Concurrent reads with snapshot isolation.
- Ordered scans and efficient prefix search.
- Checksummed, versioned WAL, manifest, and SSTable formats.
- Predictable point-read behavior through leveled compaction, Bloom filters,
  indexes, and block caching.
- Typed adapters for inference cache, feature store, and embedding workloads.
- Linux-first performance with portable fallbacks where practical.
- Strong crash, corruption, property, concurrency, fuzz, and benchmark suites.
- Educational documentation that explains what each feature does, why it
  exists, how it works, and its trade-offs.

### 1.3 Non-Goals

- Distributed replication, sharding, or consensus.
- RocksDB API or file-format compatibility.
- General multi-key ACID transactions.
- Multiple independent processes opening one database for concurrent writes.
- Approximate nearest-neighbor indexes such as HNSW.
- Semantic vector search.
- A network database server.

## 2. Public Model

The core engine stores lexicographically ordered byte keys and byte values.
Typed workload adapters own their key encodings and value codecs rather than
adding AI concepts to the storage core.

The initial stable API includes these operations:

```rust
Engine::open(options)
Engine::get(key)
Engine::put(key, value)
Engine::delete(key)
Engine::write(batch)
Engine::snapshot()
Engine::scan(bounds, limit)
Engine::scan_prefix(prefix, limit)
Engine::flush()
Engine::compact(range)
Engine::stats()
Engine::close()
```

Snapshots expose the same read and scan operations at a fixed sequence number.
Write batches contain puts and deletes and become visible atomically.

Configuration covers:

- database path;
- synchronous or buffered WAL durability;
- memtable and target SSTable sizes;
- block size and restart interval;
- Bloom filter bits per key;
- compression codec;
- block-cache budget;
- background flush and compaction limits;
- maximum batch, key, and value sizes.

## 3. Architecture

MeteorDB is divided into focused subsystems with explicit interfaces:

| Subsystem | Responsibility |
| --- | --- |
| Engine | Public lifecycle, API coordination, and failure propagation |
| Write coordinator | Serializes batch sequence assignment and WAL/memtable publication |
| WAL | Durable, checksummed batch records and recovery replay |
| Memtable | Ordered in-memory versions and tombstones |
| SSTable | Immutable sorted blocks, indexes, filters, metadata, and checksums |
| Manifest/version set | Durable description of the live SSTable set |
| MVCC | Sequence assignment, snapshot visibility, and obsolete-version rules |
| Read path | Point lookups and merged range/prefix iteration |
| Compaction | Level scoring, input selection, merging, and output installation |
| Cache | Separate index/filter and data-block caching |
| Workload adapters | Typed AI-oriented keys, values, and operations |
| Inspection CLI | Format inspection, checking, and benchmarks |

The first concurrency model has one serialized write path and concurrent
snapshot readers. This avoids ambiguous commit ordering and makes recovery
reasoning tractable. Flush and compaction run in background workers, but they
publish new versions atomically through the manifest/version-set layer.

## 4. Internal Key and MVCC Model

Each committed write batch receives a monotonically increasing sequence
number. An internal record is identified by:

```text
(user_key, sequence_number, value_kind)
```

Internal ordering sorts user keys ascending and sequence numbers descending.
The newest version of a key is therefore encountered first during a read.
`value_kind` distinguishes values from deletion tombstones.

A snapshot captures a sequence number. It observes the newest version whose
sequence is less than or equal to the snapshot sequence. Versions newer than
the snapshot are skipped.

Compaction may remove an overwritten value or tombstone only when:

- no active snapshot can observe the older version;
- lower levels cannot contain a version that would be incorrectly exposed; and
- retention or expiration rules do not require it.

MeteorDB offers snapshot-isolated reads and atomic write batches, not arbitrary
read/write transactions or conflict detection.

## 5. Write Path and Durability

The write path is:

```text
caller
  -> validate batch
  -> assign sequence number
  -> encode and append one WAL record
  -> sync when required
  -> insert records into the mutable memtable
  -> publish the committed sequence
  -> acknowledge the caller
```

Encoding a batch as one logical WAL record prevents recovery from exposing a
partial batch.

### 5.1 WAL

The WAL is segmented and uses length-delimited, checksummed physical fragments.
Large logical records may span fragments, but replay emits a batch only after
all fragments have been validated.

Durability modes:

- `Sync`: acknowledge only after the WAL reaches stable storage through
  `fsync`.
- `Buffered`: acknowledge after the append reaches the operating system; the
  caller may explicitly request synchronization.

A structurally incomplete final WAL header, payload, or fragment chain is
ignored during recovery. A checksum mismatch is always reported as corruption,
including in the final physical fragment, because complete bytes with an
invalid checksum are not structural truncation. Replay receives the same
`max_batch_bytes` limit as the writer so recovery cannot accumulate an
unbounded logical record.

### 5.2 Memtable Rotation

When the mutable memtable reaches its configured size:

1. it becomes immutable;
2. a new mutable memtable is installed;
3. a background flush converts the immutable memtable to an SSTable;
4. the manifest atomically installs the SSTable;
5. WAL segments no longer needed by any unflushed memtable are deleted.

Write backpressure limits the number of immutable memtables so foreground
writes cannot consume memory without bound when storage is slow.

## 6. SSTable Format

An SSTable is immutable and contains:

- sorted data blocks;
- restart-point prefix compression within each data block;
- per-block checksums;
- an index block mapping separator keys to data-block handles;
- a Bloom-filter block;
- properties and format metadata;
- a fixed footer with block handles, magic value, and format version.

Prefix compression stores the shared prefix length plus the unshared key
suffix. Periodic restart points bound the work required to reconstruct a key
and permit binary search within a block.

Compression is selected per data block. Index, filter, and footer structures
remain independently readable. The initial implementation supports a
pluggable codec interface so benchmark evidence can decide the default.

All decoders validate lengths, offsets, integer overflow, checksums, magic
values, and supported format versions before exposing data.

## 7. Read Path

### 7.1 Point Reads

A point read checks sources from newest to oldest:

1. mutable memtable;
2. immutable memtables;
3. overlapping level-zero SSTables;
4. at most one candidate SSTable per non-overlapping lower level.

Bloom filters answer either "definitely absent" or "possibly present." A false
positive causes extra work, but a false negative is not allowed. Filters avoid
many index and data-block reads for absent keys.

The cache separates frequently reused index/filter blocks from data blocks so
a large value workload cannot evict all navigation metadata. Cache entries are
identified by database/file identity and block offset, not only by user key.

### 7.2 Range and Prefix Reads

Range scans merge ordered iterators from all relevant memory and disk sources.
The MVCC merge layer:

- returns user keys in order;
- emits at most one visible value per user key;
- hides tombstones;
- hides expired records;
- obeys snapshot visibility;
- stops at the requested bound or result limit.

Prefix search converts a prefix into a half-open byte range:

```text
[prefix, smallest_exclusive_successor(prefix))
```

If no finite successor exists, the upper bound is unbounded. Prefix search
therefore reuses the ordered range-scan machinery and needs no separate prefix
index.

## 8. Compaction

Level zero may contain overlapping key ranges because memtables are flushed
independently. Levels one and above contain non-overlapping SSTables.

A compaction score compares each level's current size or file count with its
configured target. The highest eligible score selects work. A compaction:

1. selects input files from one level;
2. includes all overlapping files from the next level;
3. merges records in internal-key order;
4. applies MVCC, tombstone, and expiration drop rules;
5. splits output near the target SSTable size;
6. syncs output files;
7. atomically installs the new version through the manifest;
8. deletes obsolete files only after no reader references the old version.

Leveled compaction is the initial policy because it limits overlapping files
and makes point-read costs more predictable than size-tiered compaction. Its
trade-off is greater write amplification.

Future measured improvements may include subcompactions, adaptive file sizing,
or alternative policies. These are not required for the initial stable format.

## 9. Manifest and Recovery

The manifest is an append-only log of version edits such as:

- add SSTable;
- remove SSTable;
- advance the durable sequence number;
- record compaction pointers;
- update the active WAL identity.

A small current-pointer file identifies the active manifest. New files are
written and synced before a manifest edit references them. Directory entries
are synced where required to make rename and file creation durable on the
target platform.

Recovery:

1. validates options and acquires the database lock;
2. loads the current manifest;
3. reconstructs and validates the live version set;
4. identifies required WAL segments;
5. replays complete valid batches into memtables;
6. restores the last committed sequence number;
7. resumes pending flush or compaction work.

Files not referenced by the recovered version are classified carefully before
cleanup. Recovery never guesses that a malformed referenced file is obsolete.

## 10. TTL and Expiration

TTL is stored as value metadata containing an absolute expiration timestamp.
An expired value becomes immediately invisible to normal reads. Compaction
later reclaims its storage when snapshot and lower-level safety rules permit.

Expiration does not create a new user-visible MVCC version. Reads use an
engine-provided clock abstraction so expiration behavior is deterministic in
tests. Persisted timestamps use a documented wall-clock representation;
monotonic clocks are unsuitable because they do not survive restarts.

## 11. AI Workload Adapters

### 11.1 Inference Cache

The inference-cache adapter is the first performance target.

A canonical cache key includes:

- namespace or tenant;
- model identity and version;
- normalized inference parameters;
- a stable digest of the input.

Canonical encoding prevents semantically identical requests from producing
different keys because of map ordering or formatting differences. Values
contain the inference result, creation and expiration metadata, and optional
application metadata.

The adapter supports point lookup, put, delete, batch lookup, and optional
in-process singleflight. Singleflight coalesces concurrent misses for one key
so only one caller computes the result; it is a process-local coordination
feature, not a distributed lock.

Primary benchmark scenarios cover:

- cache-hit latency;
- mixed reads and writes;
- batched lookup;
- TTL-heavy churn;
- repeated hot keys;
- concurrent identical misses;
- restart and WAL recovery.

### 11.2 Feature Store

Feature keys use an ordered composite encoding:

```text
(entity_type, entity_id, feature_group, event_or_version_time)
```

Length-delimited or escaped components prevent ambiguous boundaries.
Time encoding preserves the ordering needed by latest-version and historical
range queries.

The adapter supports online point lookup, feature-group prefix scans, batched
entity lookup, and historical version scans. A snapshot provides a consistent
view when several features must be read together.

### 11.3 Embedding Store

The embedding adapter stores:

- vector dimension and scalar type;
- raw vector bytes;
- user metadata;
- optional model/version identity.

It supports batch put/get and validates dimensions and byte lengths. Decoding
avoids copies when the stored representation and buffer alignment make that
safe; otherwise it performs an explicit checked conversion.

The initial design optimizes storage and retrieval, not vector similarity
search. Large-value separation, vector-specific compression, exact search, and
ANN indexes require separate evidence and designs.

## 12. Errors and Failure Semantics

Public errors distinguish:

- invalid options, keys, values, and batches;
- I/O failures with operation and path context;
- checksum or structural corruption;
- unsupported format versions;
- database locking conflicts;
- use after close;
- background flush or compaction failure;
- resource limits and write stalls.

The engine enters a visible failed state when a background durability-critical
operation fails. It does not silently continue acknowledging unsafe writes.
Corruption is never converted into a cache miss.

Panics are reserved for internal invariant violations that cannot be caused by
malformed persisted or user-provided input. Decoders and public APIs return
typed errors for untrusted data.

## 13. Observability and Inspection

Structured statistics include:

- operation counts and latency histograms;
- memtable and immutable-table sizes;
- WAL bytes and sync counts;
- Bloom-filter checks and useful negatives;
- block-cache hits, misses, admissions, and evictions;
- bytes read and written by level;
- flush and compaction duration;
- read, write, and space amplification;
- expired and obsolete records removed;
- write stalls and background failures.

The `meteordb` inspection CLI provides:

- `check`: validate manifest and referenced files;
- `dump-manifest`: render version edits and the resulting level layout;
- `dump-sstable`: inspect metadata, blocks, key ranges, and checksums;
- `bench`: run reproducible engine and workload benchmarks.

The CLI is diagnostic tooling over shared format readers, not a second storage
implementation.

## 14. Testing Strategy

### 14.1 Component Tests

Each subsystem has focused tests for encoding, ordering, boundaries,
checksums, rotation, and state transitions.

### 14.2 Reference-Model Property Tests

Random sequences of puts, deletes, batches, snapshots, scans, prefix scans,
flushes, and compactions are compared with a simple in-memory reference model.
This checks interactions that example-based tests commonly miss.

### 14.3 Crash and Recovery Tests

Deterministic fault injection simulates failure around:

- WAL append and sync;
- WAL rotation;
- SSTable creation and sync;
- manifest append and sync;
- current-file replacement;
- obsolete-file deletion.

After each injected crash, recovery must produce either the last acknowledged
durable state or a documented weaker state under buffered durability, never a
partially committed batch.

### 14.4 Corruption and Fuzz Tests

Fuzz targets cover WAL, manifest, block, footer, filter, and composite-key
decoders. Corruption tests flip, truncate, duplicate, and reorder bytes to
confirm that malformed data is rejected without memory unsafety or panics.

### 14.5 Concurrency Tests

Tests interleave writers, snapshot readers, flushes, and compactions. They
verify atomic batch visibility, stable snapshots, ordered scans, safe file
reclamation, close behavior, and propagation of background failures.

### 14.6 Benchmarking

Criterion benchmarks measure:

- point get/put/delete;
- atomic batches;
- present and absent reads;
- prefix and bounded scans;
- flush and recovery;
- compaction;
- inference-cache, feature-store, and embedding operations.

A separate reproducible harness compares MeteorDB with RocksDB under equivalent
durability, compression, cache, dataset, and concurrency settings. Reports
include throughput, p50/p95/p99 latency, memory, recovery time, database size,
and amplification. Results must disclose hardware, operating system, build
profile, configuration, warm-up, and dataset distribution.

## 15. Platform and Safety Policy

MeteorDB targets Linux first but isolates platform-specific file operations
behind a storage abstraction. Portable synchronous file I/O is the correctness
baseline. Linux-specific optimizations such as `io_uring`, direct I/O, or
specialized advice calls require benchmarks and remain optional.

Unsafe Rust is not prohibited, but each unsafe block must:

- be localized behind a safe interface;
- document its invariants;
- have tests that exercise boundary and failure cases;
- provide a measurable reason that safe Rust is insufficient.

Memory mapping, direct I/O, and zero-copy vector access are optimizations, not
initial correctness requirements.

## 16. Acceptance Criteria

The initial production-quality release is complete when:

- acknowledged synchronous batches survive deterministic crash tests;
- snapshots and merged scans match the reference model;
- prefix scans return ordered, deduplicated, snapshot-correct results;
- compaction preserves visible data and safely removes obsolete versions;
- all persisted structures detect tested corruption;
- TTL behavior is deterministic and storage is reclaimed through compaction;
- all three AI adapters have end-to-end tests and benchmarks;
- inference-cache benchmarks can be reproduced against RocksDB;
- inspection tools explain the current on-disk state;
- public APIs, formats, limitations, and benchmark methodology are documented;
- standard tests, property tests, fuzz smoke tests, and static checks pass.

## 17. Explicitly Deferred Work

- parallel write queues;
- general transactions and conflict detection;
- column families;
- merge operators;
- snapshots persisted across process restarts;
- remote filesystems and object storage;
- encryption at rest;
- replication and distributed operation;
- HNSW or other ANN indexes;
- automatic workload-driven compaction policy changes.

Each deferred capability requires its own design because it changes correctness,
format, or operational guarantees rather than merely adding an API method.
