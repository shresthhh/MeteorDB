# Durability, time, and failures

## Acknowledgement modes

`Options::new` selects `Durability::Sync`. A successful write has appended its
complete logical WAL record and synchronized it before becoming visible.
Filesystem and hardware guarantees still depend on the host honoring file and
directory sync operations.

`Durability::Buffered` publishes after append without a sync. Process crashes
normally leave kernel-buffered data available, but power loss or kernel failure
can lose acknowledged writes. `Engine::sync` synchronizes every WAL required
for recovery and persists WAL ownership. `flush` waits until current memory is
published as SSTables. `close` syncs required WALs; it does not imply flush or
compaction.

## Publication order

New SSTables are written as temporary files, file-synced, atomically installed,
and followed by a directory sync before a synchronized manifest edit names
them. `CURRENT` is installed and directory-synced when metadata is bootstrapped.
Obsolete WALs and SSTables are deleted only after durable metadata and reader
lifetimes make them unnecessary.

## TTL and clocks

`put_with_ttl` converts a non-negative duration to an absolute Unix-millisecond
deadline once, while holding serialized write state. Zero expires immediately.
Deadlines persist in WALs and SSTables.

The clock supplied to an open engine must never decrease. A process reopening
the same database must not start behind wall time observed by the previous
process. MeteorDB does not persist a read-time watermark, so host clock rollback
across processes is unsupported. NTP adjustments, VM snapshot restore, dual
boot, and manual clock changes can violate this assumption.

Snapshots capture an MVCC sequence only. A value visible at snapshot creation
can expire before a later read through that snapshot. Scans capture one wall
time when the iterator is created. Expired entries behave as absent and become
physically reclaimable during compaction.

## Failure contract

MeteorDB distinguishes incomplete final append state from complete malformed
state:

- short final WAL fragments/headers and unfinished final fragment chains are
  ignored during replay;
- a structurally incomplete trailing manifest record is truncated to the last
  complete record;
- checksum failures, invalid ordering/encoding, sequence gaps, missing required
  files, and inconsistent metadata return `Error::Corruption`,
  `Error::UnsupportedFormat`, or contextual `Error::Io`;
- lazy iterators return a decoding/I/O error once, then are exhausted;
- a terminal mutation-path failure prevents continued unsafe writes.

There is no automatic repair. Preserve the files, capture the typed error, and
restore from an independently tested copy. The inspector is read-only and can
validate files under explicit allocation/output limits.

## Operational boundaries

MeteorDB assumes a trusted local database directory for `Engine::open`.
Do not place it on filesystems that do not provide reliable atomic rename,
locking, file sync, and directory sync semantics. The inspector is the bounded
surface for untrusted database files. MeteorDB has no replication, backup,
checkpoint, online repair, encryption-at-rest, or stable migration facility.
