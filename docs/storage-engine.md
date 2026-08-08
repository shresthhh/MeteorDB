# Storage engine

MeteorDB combines a log-structured merge-tree layout with MVCC point reads.
This page explains the storage concepts behind the current pre-alpha engine and
the trade-offs applications should expect.

## LSM trees

An LSM tree accepts writes in memory and periodically turns sorted memory into
immutable files. MeteorDB writes new files to level 0, where key ranges may
overlap. Its version metadata and point-read path also understand
non-overlapping higher levels, but automatic compaction does not yet move or
merge files between levels.

This layout makes foreground writes sequential and avoids editing SSTables in
place. Without compaction, however, level 0 grows over time and point reads can
probe more files.

## WAL segments

The write-ahead log is the first durable destination for a batch. A batch is
encoded as one logical record with a sequence number and operation count, then
fragmented across fixed-size physical blocks when necessary. CRC32C checksums
detect damaged fragments.

MeteorDB creates a new WAL when it rotates a memtable. The manifest records the
oldest required WAL and the active WAL. Recovery replays the required range in
strict sequence order; a missing segment, gap, or corrupt record is an error.

Synchronous durability is the default and synchronizes each successful append.
Buffered durability can reduce foreground synchronization but risks losing
recent acknowledged writes on power loss until `Engine::sync` or
`Engine::close` succeeds.

## Memtables

The mutable memtable is an ordered in-memory map of internal keys. An internal
key combines the application key, a sequence number, and a value or deletion
kind. This ordering keeps versions of one user key adjacent and makes the
newest version visible at a selected sequence easy to find.

When the configured memory threshold is reached, the mutable table and its WAL
become immutable and a fresh pair accepts writes. A bounded immutable queue
prevents unlimited memory growth; writes return a stall error when flush cannot
keep up.

## SSTables

A flush writes an immutable sorted-string table. SSTables contain
prefix-compressed data blocks, restart points for bounded seeks, an index,
per-table Bloom-filter data, checksummed block trailers, and a footer describing
the format.

The builder writes a temporary file and atomically installs the completed table.
The manifest publishes it only after the file and directory have been
synchronized. Readers never modify a published SSTable.

## Bloom filters

A Bloom filter compactly answers either “definitely absent” or “possibly
present.” MeteorDB builds a filter from the user keys in each SSTable. A
definite negative skips the data-block lookup; a positive result still checks
the table because false positives are possible.

More bits per key consume more metadata space but generally reduce unnecessary
data reads. Bloom filters do not replace index lookup and do not support range
scans.

## MVCC

Every committed batch receives one monotonically increasing sequence number.
Current point reads use the latest published sequence. A snapshot stores the
sequence visible when it was created, so later updates and deletions do not
change point reads through that snapshot.

MeteorDB currently retains historical versions because compaction and version
reclamation are roadmap work. Snapshots provide stable point reads, not
multi-key transactions or scan isolation.

## Manifests

The manifest is the durable source of truth for live SSTables and recovery
counters. Each edit is framed and checksummed. Applying an edit validates a new
immutable version, synchronizes newly referenced SSTables, appends and
synchronizes the edit, and only then publishes the version to readers.

`CURRENT` identifies the active manifest. On recovery, MeteorDB replays its
complete edits, rejects invalid metadata, validates referenced files, and
truncates a torn trailing manifest record to the last valid boundary.

## Block caching

Point reads use an in-process LRU block cache keyed by SSTable number, block
offset, and block kind. The cache reserves 20% of its byte budget for Bloom and
index metadata and 80% for data blocks. Independent budgets prevent a stream of
large data blocks from evicting all navigation metadata.

`Engine::stats` reports cache capacity, usage, hits, misses, admissions, and
evictions together with point reads, Bloom checks, useful negatives, and table
probes by level.

## Read and write amplification

Write amplification is currently dominated by the WAL plus one level 0
SSTable write. That is a simple path, but the absence of compaction means old
files and obsolete key versions are not reclaimed.

Read amplification depends on where a key is found. Memory hits avoid SSTable
I/O. On disk, level 0 may require multiple overlapping probes; each higher
level requires at most one candidate probe. Bloom filters and the block cache
reduce physical reads but do not remove the lookup work. The statistics API
exposes average SSTable probes per point read for workload evaluation.

## Current trade-offs

- **Embedded and synchronous:** The API is easy to integrate but foreground
  calls can perform filesystem work.
- **Strong recovery ordering:** Extra synchronization favors explicit
  durability over maximum write throughput.
- **Point reads only:** Ordered storage exists internally, but range and prefix
  scans are not yet public capabilities.
- **Flush without compaction:** Data reaches immutable SSTables, but long-lived
  databases accumulate level 0 files and historical versions.
- **No TTL enforcement:** Batches can carry expiration metadata internally, but
  the engine does not enforce expiry.
- **No workload adapters:** Inference-cache, feature-store, embedding, and ANN
  surfaces remain roadmap layers over the byte API.
- **No published performance claims:** Evaluate the current code with your own
  durability mode, dataset, and hardware.
