# Task 11 Report: Leveled Compaction

## Status

Complete.

- Feature commit: `613cdd459228a6e6f987c35aa8b5aec3d3594995`
- Atomic snapshot fix: `52ebd2509328bedb588d621489411e4224e03a8a`
- Failed-compaction cleanup fix:
  `051c8a929e8f0c2f17ed1f78fcdaafcffa628ea8`
- Branch: `feature/meteordb-engine`
- Nothing was pushed.
- The commits have no co-author trailers.
- No AI adapters or TTL API were added.

## Review findings resolved

### Atomic snapshot registration

`Engine::snapshot` now holds `write_state` while it validates readability,
captures the committed sequence, and acquires the `SnapshotGuard`. A writer or
compaction cannot advance/prune between sequence capture and registry
insertion.

The deterministic regression pauses snapshot creation immediately after
sequence capture. Before the fix, a writer completed an overwrite, flush, and
compaction while the snapshot was paused, and the test failed. After the fix,
the writer remains blocked until the guard is registered, and the snapshot
still reads the historical value.

### Failed compaction cleanup

Compaction now carries an armed scoped cleanup object that records only
temporary files successfully created by the current attempt and final files
successfully installed by that attempt. Any failure before manifest append
removes those paths through `DurableFs` and synchronizes the directory, so a
retry can reuse the unchanged next file number.

Manifest application now distinguishes failures before append from failures
during append/sync. Cleanup is disarmed for the latter because recovery may
observe the edit; files that may be manifest-visible are never deleted.

## What changed

- Added public `CompactionPicker`, `CompactionPlan`, `CompactionJob`, and
  `Engine::compact`.
- Added L0 file-count scoring and byte scoring for levels 1 through 5.
- Added fixed-point overlap expansion between the selected level and its next
  level.
- Reused one internal merging iterator for scans and compaction.
- Added snapshot-safe MVCC version filtering and conservative tombstone
  removal.
- Added target-sized output splitting without dividing one user key's history
  between files.
- Added synchronized output installation followed by one atomic `VersionEdit`.
- Added deferred SSTable reclamation protected by every retained old
  `Arc<Version>`, including versions older than the immediately previous one.

## Beginner explanation

### Scores

Compaction asks, “Which level needs cleanup most?”

- Level 0 is scored as `file count / 4`.
- Later levels are scored as `total bytes / target bytes`.

A score of `1.0` means the level is exactly at its allowance. MeteorDB chooses
the highest score strictly above `1.0`. The final level is not selected because
there is nowhere lower to place its output.

### Overlaps

Level 0 files may cover the same keys. Levels 1 and below must not overlap
inside one level. Once a source file is selected, every next-level file whose
user-key range intersects it is included too. The ranges are expanded until
no newly included file pulls another source or destination file into the job.
This prevents leaving two overlapping files behind in a nonzero level.

### Merge

Each input SSTable is already sorted by internal key: user key ascending, then
newest sequence first. Compaction feeds all table iterators into the same
heap-based internal merge used by scans. It therefore sees one globally sorted
stream and handles all versions of one user key together.

### Version retention

The newest record is always retained. If snapshots exist, compaction also
retains:

- every version newer than the oldest active snapshot; and
- the first version at or below that snapshot sequence.

Those records are enough for every active snapshot between the oldest snapshot
and the current sequence to find the same visible history it saw before
compaction.

### Tombstone safety

A tombstone says, “Older values for this key are deleted.” Removing it too
early can expose an old value from a lower level.

MeteorDB drops a newest tombstone only when:

1. no level below the compaction output can contain that user key; and
2. no active snapshot needs history from before that tombstone.

Otherwise the tombstone remains in the output.

### File splitting

Outputs are split near `target_sstable_bytes`. A user key and all versions
retained for it are written as one group, so a split never places one key's
history in two adjacent files. Oversized groups may exceed the target because
record boundaries are never broken.

### Atomic install

Every output is built as a temporary SSTable, fully finished, synchronized,
installed under its final name, and directory-synchronized. `VersionSet`
re-synchronizes each referenced output. Only then does one manifest edit add
all outputs and remove all inputs together. The edit is synchronized before
the new immutable `Version` is published.

A crash therefore observes either the old input set or the complete new output
set through the manifest, never a half-installed logical version.

### Deferred deletion

Publishing a new version does not mean old readers have stopped using the old
one. Scans retain an `Arc<Version>`. Removed files enter a reclamation queue
instead of being deleted immediately.

Before deleting a file, MeteorDB checks every retained old version. This
includes a version that survived multiple later compactions: if any live old
version still lists the file, its path remains. Once all such readers release
their `Arc<Version>`, a later compaction pass deletes the obsolete file and
synchronizes the directory.

## TDD evidence

The compaction integration test was created first. It failed because
`CompactionPicker` and `Engine::compact` did not exist.

Coverage includes:

- L0 trigger scoring;
- highest-score level selection;
- next-level overlap expansion;
- MVCC merge and active-snapshot history;
- target output splitting;
- bottom-level tombstone removal;
- output-sync, manifest-sync, then input-removal ordering;
- old input retention while a scan owns its version;
- transitive retention when a much older version references a file that
  survives one compaction and is removed by a later one.
- atomic sequence capture and snapshot registration against a concurrent
  overwrite/flush/compaction;
- temporary SSTable cleanup after an injected output synchronization failure;
- installed-output cleanup and file-number reuse after an injected
  pre-manifest SSTable synchronization failure; and
- preservation of installed outputs after an uncertain manifest sync failure.

The transitive-retention test first reproduced an unsafe early deletion, then
passed after reclamation began checking all retained old versions.

The snapshot regression first failed because the concurrent writer and
compaction completed before registration. The cleanup regressions first failed
with a remaining `.sst.tmp` and five unpublished installed SSTables,
respectively. Both then passed after the fixes.

## Validation

Using `.superpowers/sdd/local-toolchain/root/usr/bin/gcc-13`:

- `cargo fmt --all -- --check` — passed.
- `cargo test -p meteordb --test compaction` — 10 passed.
- `cargo test -p meteordb` — 167 unit/integration tests and all doc tests
  passed.
- `cargo clippy -p meteordb --all-targets -- -D warnings` — passed.
- `RUSTDOCFLAGS="-D warnings" cargo doc -p meteordb --no-deps` — passed.
- `git diff --check` — passed.

## Concerns

1. `Engine::compact` is synchronous and holds the engine write-state mutex
   during table I/O. This favors correctness for the first implementation but
   can pause writes during a large compaction.
2. Obsolete files are retried/reclaimed when compaction runs. A future
   dedicated background compaction/reclamation loop could reclaim sooner
   without changing the safety rule.
3. Level byte targets currently use the configured SSTable target for each
   scored level. Per-level growth factors can be introduced later as an
   explicit tuning policy.
4. If manifest append or synchronization fails, the edit may be recoverable,
   so its installed outputs intentionally remain and the manifest writer is
   unusable until the database is reopened. This is the conservative
   no-data-loss behavior rather than an in-process retry path.
5. Scoped cleanup is best-effort during unwinding. A separate failure of
   `DurableFs::remove_file` or directory synchronization can still require
   recovery-time orphan cleanup.
