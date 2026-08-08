# Task 9 Report: Cached Point Reads Across All Levels

## Status

`DONE`

MeteorDB now performs MVCC point reads without whole-SSTable scans. Reads check
mutable memory, immutable memory newest-first, every overlapping L0 file
newest-first, and at most one range candidate in each non-overlapping lower
level. Bloom filters, partitioned block caching, corruption propagation, and
structured read statistics are integrated. Scans and compaction remain
unimplemented.

## Commits

- Plan: `bce33ee3c04c2b35879914fc36de551210360546`
  - `plan: implement cached multi-level point reads`
- Feature: `9695a4a5dce8e78faa9d3915da4be2ef544c3307`
  - `feat: add cached multi-level point reads`
- Neither commit contains a Copilot co-author trailer.
- Nothing was pushed.

## Changed Behavior

- `Engine::get` and snapshot reads retain MVCC sequence visibility and
  tombstones while selecting only relevant disk candidates.
- L0 uses the manifest's descending file-number order because L0 ranges may
  overlap and newer flushed files must win.
- Levels 1-6 use their sorted, non-overlapping user-key ranges to binary-select
  at most one file per level.
- SSTable point lookup seeks directly to the first internal key visible at the
  requested snapshot sequence and reads at most one candidate data block.
- SSTable format version 2 stores Bloom entries by user key, making one Bloom
  decision safe for every MVCC version. Version 1 remains readable, but its
  exact-internal-key Bloom filter is conservatively skipped for MVCC point
  lookup so an old filter can never create a false miss.
- Checked index, filter, and data payloads are cached by
  `(file_number, block_offset, block_kind)`.
- `Engine::stats()` exposes point reads, table probes, per-level probes, Bloom
  checks/useful negatives, cache capacity/usage/entries/activity, and average
  SSTable read amplification.

## Test-Driven Development Record

The initial read-path target failed before production implementation:

```text
unresolved imports `meteordb::BlockCache`, `meteordb::BlockKind`,
`meteordb::CachePartition`
```

Subsequent RED states proved the missing behavior:

```text
no method named `snapshot` found for struct `BlockCache`
no method named `stats` found for struct `Engine`
```

The first disk-cache tests then failed because cache hits and Bloom counters
remained zero. A maximum-sized configured value reproduced a real reader-bound
bug:

```text
declared maximum data-block bytes 1048 exceeds reader limit 1024
```

The SSTable version test also failed with `left: 1, right: 2`, proving that the
new user-key Bloom semantics needed a distinct persistent format version.
Read-amplification tests initially failed to compile because `point_reads`,
`sstable_probes`, and `read_amplification` did not exist.

The final focused suite has 14 passing tests covering cache partitions and LRU,
mutable/immutable ordering, overlapping L0, lower-level pruning, snapshots,
tombstones, useful Bloom negatives, metadata/data cache hits, legacy v1
correctness, maximum values, structured statistics, and checksum propagation.

## Beginner Walkthrough

### LRU

LRU means **least recently used**. Each cache access receives a deterministic
logical timestamp. When a partition exceeds its budget, MeteorDB removes the
entry with the oldest timestamp. Reading an entry refreshes it, so a frequently
used block stays while an older idle block leaves.

### Cache keys

A user key is not enough to identify cached bytes. The same user key may exist
in several files and versions. MeteorDB instead uses:

```text
(file number, byte offset, block kind)
```

The file number identifies one immutable SSTable, the offset identifies the
physical block inside it, and the kind distinguishes filter, index, and data
roles. File numbers are never reused while live, and each engine owns its own
cache, so the key cannot accidentally alias another database block.

### Metadata/data partitioning

The configured cache budget is split deterministically:

- 20% for Bloom-filter and index metadata;
- 80% for data blocks.

The budgets are enforced independently. A workload with large values can fill
and churn the data partition without evicting every navigation block. Metadata
therefore remains useful for finding future values.

### Bloom useful negatives

A Bloom filter can say either **definitely absent** or **possibly present**.
“Possibly present” may be a false positive and must still be checked. A
**useful negative** is a definite absence for a file whose user-key range
otherwise made it a candidate. That negative avoids the data-block read.

Version 2 filters store user keys rather than full MVCC internal keys. All
versions of `k` therefore share one safe Bloom answer. Legacy version 1 filters
are skipped during MVCC point lookup because asking an exact-version filter
about a different snapshot sequence could otherwise create a false negative.

### Level invariants

L0 files come from independent memtable flushes, so their ranges may overlap.
MeteorDB must inspect every overlapping L0 file in newest-file-first order.

Levels 1 and above promise that file user-key ranges do not overlap and are
sorted. If a key belongs to one range, it cannot belong to another file in that
level. MeteorDB can therefore binary-select at most one candidate per lower
level.

### Read amplification

One logical `get` may consult several physical tables or blocks. That extra
work is **read amplification**. MeteorDB reports average SSTable probes per
point read:

```text
sstable probes / point reads
```

Range pruning, Bloom negatives, cached metadata, and cached data all reduce the
physical work. Without compaction, many overlapping L0 files can still increase
read amplification.

### MVCC and tombstones

The seek key uses the requested snapshot sequence and deletion kind, the first
kind at that sequence in internal-key order. The first returned record for the
same user key is therefore the newest version not newer than the snapshot. A
tombstone is returned internally as a real result so the engine stops searching
older files; only the public API converts it to `None`.

### Corruption is not a miss

Range exclusion and a checked Bloom negative are the only shortcuts that avoid
opening data. Footer, metadata, checksum, compression, block, and internal-key
errors propagate with `?`. Cache admission occurs only after the payload has
passed its checksum and structural validation, so damaged bytes are never
silently recorded as absence.

## Exact Validation

Validation used the repository-local GCC configuration:

```bash
cd /home/shresth/dev/meteordb/.worktrees/implementation
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"
```

Final command:

```bash
cargo fmt --check &&
cargo clippy -p meteordb --all-targets -- -D warnings &&
cargo test -p meteordb --test read_path &&
cargo test -p meteordb &&
RUSTDOCFLAGS='-D warnings' cargo doc -p meteordb --no-deps &&
git diff --check &&
git diff --cached --check
```

Results:

- formatting: passed;
- strict Clippy across all targets: passed;
- focused read-path suite: 14 passed, 0 failed;
- full crate suite: 134 passed, 0 failed;
- doc tests: 0 failed;
- rustdoc with denied warnings: passed;
- unstaged and staged diff checks: passed.

## Concerns

- Compaction is intentionally absent, so a large number of overlapping L0 files
  can still raise read amplification even though each file lookup is indexed.
- Version 1 SSTables remain correct but cannot provide safe useful Bloom
  negatives for arbitrary snapshots; they skip that optimization until rewritten
  naturally by future compaction or migration work.
- The cache uses one mutex for deterministic ordering and statistics. This is
  simple and correct for the current synchronous engine, but a highly concurrent
  workload may eventually benefit from sharding.
- Cached payloads avoid storage I/O and checksum work, but data blocks are
  structurally decoded again on each hit. Caching decoded blocks is a possible
  later CPU optimization.

## Review Remediation

Task 9 review findings were fixed in a follow-up commit.

### Statistics

- Replaced independently loaded atomics with one mutex-protected read-statistics
  state, so a snapshot cannot combine counters from different logical moments.
- Bloom checks and useful negatives now update together, preserving
  `bloom_useful_negatives <= bloom_checks`.
- Point reads, Bloom counters, and per-level probes use saturating increments.
- `sstable_probes` is documented and computed as the saturating sum of the
  per-level counters.
- Added concurrent snapshot-invariant and near-`u64::MAX` saturation tests.

### Cache

- Oversized entries are rejected before replacement or eviction.
- Replacement subtraction and final admission addition are checked; eviction
  creates room before addition.
- Snapshot and regression assertions verify that charged usage exactly equals
  the sum of retained entry sizes.
- The near-overflow budget helper proves capacity checks without overflowing
  `usize`.
- LRU timestamps renormalize deterministically before `u64` exhaustion, so a
  newly admitted entry cannot wrap around and become the oldest.
- Added small-capacity, oversized-entry, replacement/eviction accounting, and
  near-overflow recency tests.

Exact 20/80 budgets, deterministic `(stamp, key)` eviction, point-read
correctness, and the original Task 9 scope remain unchanged.

### Review TDD and Validation

The new unit target first failed before production changes because the
near-overflow accounting predicate did not exist. The old atomic increments and
plain per-level sum were also exercised at `u64::MAX`, where they would wrap or
overflow. After the fixes:

- `cargo test -p meteordb --test read_path`: 15 passed;
- `cargo test -p meteordb`: 140 passed across unit, integration, and doc-test
  targets, 0 failed;
- `cargo fmt --check`: passed;
- `cargo clippy -p meteordb --all-targets -- -D warnings`: passed;
- `RUSTDOCFLAGS='-D warnings' cargo doc -p meteordb --no-deps`: passed;
- `git diff --check` and `git diff --cached --check`: passed.

### Remaining Concern

The coherent read-statistics state uses one mutex. This prioritizes exact
snapshot invariants and simple saturation semantics; a future profiling-driven
change could use a seqlock if statistics contention becomes measurable.
