# MeteorDB Task 9 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development and execute every red-green cycle in order.

**Goal:** Replace correctness-only whole-SSTable scans with level-aware, cached MVCC point reads and structured read-path statistics.

**Architecture:** `Engine::get_at` retains the newest visible memory result, probes only overlapping on-disk candidates, and merges exact visible records without scanning tables. A shared byte-block cache keys validated blocks by file number, offset, and kind, with independent deterministic LRU partitions reserving 20% for metadata and 80% for data. Atomic counters expose a consistent `StatsSnapshot` without making read failures disappear.

**Tech Stack:** Rust 2024, standard-library synchronization and atomics, existing MVCC internal keys, Bloom filters, SSTable blocks, manifests, and version metadata.

## Global Constraints

- Search mutable memory, immutable memory newest-first, overlapping L0 newest-first, then at most one candidate in each non-overlapping lower level.
- Preserve snapshot visibility and tombstones across every source.
- Never convert I/O, checksum, structural, or internal-key corruption into a miss.
- Key cached blocks by `(file_number, block_offset, block_kind)`.
- Reserve 20% of `Options::block_cache_bytes` for index/filter metadata and 80% for data; enforce each budget independently.
- Use deterministic least-recently-used eviction.
- Count Bloom checks, useful Bloom negatives, cache hits, misses, admissions, evictions, and per-level table probes.
- Do not implement scans or compaction.
- Keep the task feature commit separate from this plan commit, omit the Copilot trailer, and do not push.

---

### Task 1: Define executable read-path requirements

**Files:**
- Create: `crates/meteordb/tests/read_path.rs`

**Interfaces:**
- Consumes: `Engine::{open,put,delete,get,snapshot,flush,stats}`, `Options`, `VersionSet`, `VersionEdit`, `FileMeta`, `TableBuilder`
- Produces: focused tests for source ordering, MVCC, Bloom statistics, cache partitioning/LRU, level pruning, and corruption propagation

- [ ] **Step 1: Add fixture helpers**

Create helpers that use tiny `memtable_bytes` and `block_bytes`, build sorted SSTables with explicit internal-key sequences, publish files into chosen levels through `VersionSet`, and reopen the engine after fixture construction. Keep file ranges non-overlapping in levels 1 and above.

- [ ] **Step 2: Write ordering and visibility tests**

Add tests proving:

```rust
// Newer memory wins over disk.
assert_eq!(db.get(b"k")?.as_deref(), Some(&b"mutable"[..]));

// A snapshot sees the newest sequence not newer than its captured sequence.
assert_eq!(snapshot.get(b"k")?.as_deref(), Some(&b"snapshot"[..]));

// A visible tombstone stops the search and hides every older value.
assert_eq!(db.get(b"k")?, None);
```

Arrange overlapping L0 files so file number order and sequence order both matter. Arrange L1 files so the target falls in exactly one user-key range.

- [ ] **Step 3: Write cache and statistics tests**

Assert that:

```rust
let stats = db.stats();
assert_eq!(stats.cache.metadata.capacity_bytes, configured.block_cache_bytes / 5);
assert_eq!(
    stats.cache.data.capacity_bytes,
    configured.block_cache_bytes - configured.block_cache_bytes / 5
);
assert!(stats.bloom_checks >= 1);
assert!(stats.bloom_useful_negatives >= 1);
```

Use repeated reads to prove validated metadata/data blocks hit the cache. Use a deliberately tiny cache and three distinct blocks to prove deterministic LRU recency and independent metadata/data eviction.

- [ ] **Step 4: Write level-pruning and corruption tests**

Assert one absent read probes every overlapping L0 candidate newest-first but no more than one candidate in each lower level. Corrupt the checksum of the selected data block and assert `Engine::get` returns `Error::Corruption`, even after earlier candidates miss.

- [ ] **Step 5: Verify RED**

Run:

```bash
cargo test -p meteordb --test read_path
```

Expected: compilation fails because `Engine::stats`, `StatsSnapshot`, `BlockCache`, and cache-aware point lookup do not exist.

---

### Task 2: Implement deterministic partitioned block caching

**Files:**
- Create: `crates/meteordb/src/cache.rs`
- Modify: `crates/meteordb/src/lib.rs`
- Test: `crates/meteordb/tests/read_path.rs`

**Interfaces:**
- Produces:
  - `pub enum CachePartition { Metadata, Data }`
  - `pub enum BlockKind { Filter, Index, Data }`
  - `pub struct BlockCache`
  - `BlockCache::new(total_bytes: usize) -> Result<Self>`
  - `BlockCache::{get,insert}(file_number: u64, block_offset: u64, kind: BlockKind, ...)`
  - `BlockCache::snapshot() -> CacheSnapshot`

- [ ] **Step 1: Add direct cache assertions**

Test exact 20/80 budgets, replacement without double-counting bytes, values larger than a partition being rejected, recency refresh on hit, oldest-entry eviction, and metadata insertions never evicting data entries.

- [ ] **Step 2: Run the cache tests to verify RED**

Run:

```bash
cargo test -p meteordb --test read_path cache_
```

Expected: compilation fails because `cache` exports do not exist.

- [ ] **Step 3: Implement the cache**

Use one mutex-protected state containing two independent partitions. Each partition stores `Arc<[u8]>` values in a `HashMap<CacheKey, Entry>` and assigns a monotonically increasing access stamp on every hit/admission. When over budget, evict the entry with the smallest `(stamp, file_number, block_offset, block_kind)` tuple so ties are deterministic. Charge the retained byte slice length, reject oversize values without evicting useful entries, and classify `Filter`/`Index` as metadata and `Data` as data.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run:

```bash
cargo test -p meteordb --test read_path cache_
```

Expected: all cache partition and LRU tests pass.

---

### Task 3: Add structured read statistics

**Files:**
- Create: `crates/meteordb/src/stats.rs`
- Modify: `crates/meteordb/src/lib.rs`
- Modify: `crates/meteordb/src/engine.rs`
- Test: `crates/meteordb/tests/read_path.rs`

**Interfaces:**
- Produces:
  - `pub struct CachePartitionSnapshot`
  - `pub struct CacheSnapshot`
  - `pub struct StatsSnapshot`
  - `Engine::stats(&self) -> StatsSnapshot`
- Internal counters: Bloom checks/useful negatives, cache hits/misses/admissions/evictions by partition, and `[u64; NUM_LEVELS]` table probes

- [ ] **Step 1: Add snapshot-shape and monotonicity assertions**

Assert a new engine reports configured cache capacities and zero activity. After reads, assert counters only increase and snapshot fields are plain owned values that remain unchanged after later operations.

- [ ] **Step 2: Run statistics tests to verify RED**

Run:

```bash
cargo test -p meteordb --test read_path statistics_
```

Expected: compilation fails because `Engine::stats` and snapshot types do not exist.

- [ ] **Step 3: Implement atomic counters and snapshots**

Store counters in `EngineInner` using relaxed atomics because they are observational, not synchronization primitives. Build `StatsSnapshot` by loading every counter and combining it with the cache's locked usage/capacity snapshot. Record cache events at the cache boundary so every reader observes the same definitions.

- [ ] **Step 4: Run statistics tests to verify GREEN**

Run:

```bash
cargo test -p meteordb --test read_path statistics_
```

Expected: all statistics tests pass.

---

### Task 4: Make `TableReader` cache-aware and MVCC-aware

**Files:**
- Modify: `crates/meteordb/src/sstable/reader.rs`
- Modify: `crates/meteordb/src/sstable/mod.rs`
- Test: `crates/meteordb/tests/read_path.rs`

**Interfaces:**
- Consumes: shared `BlockCache`, file number, and read statistics
- Produces:
  - cached metadata reads during open
  - cached data-block reads
  - `TableReader::get_visible(user_key: &[u8], sequence: SequenceNumber) -> Result<Option<(InternalKey, Vec<u8>)>>`

- [ ] **Step 1: Add focused reader tests**

Build a table with multiple versions and a tombstone for one user key. Assert `get_visible` seeks from `InternalKey::value(user_key, snapshot_sequence)`, returns the first equal-user-key record, and reports `None` when the lower bound belongs to another user key. Assert a Bloom definite negative skips the data block and increments useful-negative statistics.

- [ ] **Step 2: Run reader tests to verify RED**

Run:

```bash
cargo test -p meteordb --test read_path reader_
```

Expected: compilation fails because `get_visible` and cache-aware open options do not exist.

- [ ] **Step 3: Cache checked metadata and data payloads**

On cache miss, read the exact block range, verify checksum/compression, decode enough to validate it, then admit the checked uncompressed payload. On cache hit, decode the retained checked payload. Do not insert failed reads or decodes. Keep footer validation uncached. Use the real footer handles as cache offsets and distinct `BlockKind` values.

- [ ] **Step 4: Implement visible point seek**

Construct the snapshot seek key with value kind, ask the Bloom filter about the exact seek key, choose the first index separator not less than it, seek inside one data block, decode the returned internal key, and require the same user key and `sequence <= snapshot_sequence`. Return tombstones as records rather than misses so the engine can stop correctly.

- [ ] **Step 5: Run reader tests to verify GREEN**

Run:

```bash
cargo test -p meteordb --test read_path reader_
```

Expected: all reader cache, Bloom, MVCC, and corruption tests pass.

---

### Task 5: Implement level-aware engine point reads

**Files:**
- Modify: `crates/meteordb/src/engine.rs`
- Test: `crates/meteordb/tests/read_path.rs`

**Interfaces:**
- Consumes: `Version` level invariants and `TableReader::get_visible`
- Produces: complete `Engine::get_at`

- [ ] **Step 1: Run source-order tests to verify RED**

Run:

```bash
cargo test -p meteordb --test read_path ordering_ visibility_ level_
```

Expected: failures show whole-table scans and excessive lower-level probes.

- [ ] **Step 2: Select candidates by level**

Validate user-key overlap against `FileMeta::{smallest,largest}.user_key()`. Probe every overlapping L0 file in the version's existing newest-file-number-first order. For each level `1..NUM_LEVELS`, binary-search the sorted non-overlapping ranges and probe at most one containing file.

- [ ] **Step 3: Merge visible records correctly**

Search mutable then immutable tables newest-first. If memory returns a record, it is newer than all disk state and may be returned immediately, including a tombstone. Otherwise probe disk candidates in level order; the first visible record in L0 order or the first visible record in the first lower level containing one is authoritative because version invariants encode source age. Convert only a found tombstone to the public `None`.

- [ ] **Step 4: Preserve every error**

Propagate reader open errors, metadata corruption, data checksum errors, internal-key decode errors, and cache lock errors with `?`. Continue only on proven range exclusion, Bloom definite-negative, or checked absence.

- [ ] **Step 5: Run the complete focused suite**

Run:

```bash
cargo test -p meteordb --test read_path
```

Expected: all ordering, visibility, tombstone, Bloom, cache, level, and corruption tests pass.

---

### Task 6: Validate, document, and commit the feature

**Files:**
- Modify: `README.md`
- Create: `.superpowers/sdd/task-9-report.md`

**Interfaces:**
- Produces: accurate feature status, beginner walkthrough, exact validation evidence, final feature commit

- [ ] **Step 1: Update public status**

State that point reads now use Bloom-filtered, cached, level-aware SSTable lookup. Keep scans and compaction explicitly planned.

- [ ] **Step 2: Run exact validation**

If the system linker fails, configure:

```bash
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"
```

Then run:

```bash
cargo fmt --check &&
cargo clippy -p meteordb --all-targets -- -D warnings &&
cargo test -p meteordb --test read_path &&
cargo test -p meteordb &&
RUSTDOCFLAGS='-D warnings' cargo doc -p meteordb --no-deps &&
git diff --check &&
git diff --cached --check
```

Expected: every command succeeds.

- [ ] **Step 3: Write the report**

Record RED/GREEN evidence, exact commands and counts, changed files, and concerns. Explain to a beginner:

- LRU as discarding the block unused for the longest logical time;
- cache keys as file identity plus byte offset plus block kind;
- 20/80 partitioning as protecting navigation metadata from large values;
- Bloom useful negatives as definite absences that avoid data reads;
- L0 overlap versus lower-level non-overlap;
- read amplification as the number of tables/blocks consulted for one logical read.

- [ ] **Step 4: Commit separately from the plan**

```bash
git add README.md crates/meteordb/src/{cache,engine,lib,stats}.rs \
  crates/meteordb/src/sstable/{mod,reader}.rs \
  crates/meteordb/tests/read_path.rs .superpowers/sdd/task-9-report.md
git commit -m "feat: add cached multi-level point reads"
```

The commit message must have no Copilot co-author trailer. Do not push.
