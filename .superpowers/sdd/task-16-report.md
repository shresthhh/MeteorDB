# Task 16 Report: Reliability Validation

## Review follow-up

All six Task 16 review findings are addressed:

1. `FaultyFs` now keeps separate volatile, file-synchronized, and durable
   images. A simulated crash restores only file contents covered by successful
   file syncs and names/removals covered by later successful directory syncs.
   The first observation of an existing regular file seeds its bytes and name
   into all three images; new files remain volatile until directory sync and
   their bytes until file sync. The crash matrix uses that oracle, and focused
   tests prove loss of buffered writes, new names, and unsynchronized
   truncation/rename state while preserving prior database files and a
   synchronously acknowledged write.
2. Reader-version registration performs bounded cleanup whenever the weak
   registry reaches 64 entries. A 10,000-read regression inspects the private
   registry and verifies dead leases remain bounded without requiring
   compaction.
3. Missing-`CURRENT` bootstrap recovery now fully replays the installed initial
   manifest, validates counters, file-number history, version edits, referenced
   files, and the absence of a torn tail before publishing `CURRENT`.
   Checksum corruption returns typed corruption and leaves `CURRENT` absent;
   the crash matrix continues to cover valid interrupted publication.
4. The reference model assigns one sequence to every atomic batch. It compares
   `Snapshot::sequence()` with the model and includes a same-key
   put/delete/put batch with before/after snapshot visibility checks.
5. The WAL torn-tail regression writes distinguishable multi-operation batches
   and truncates at every byte offset, requiring replay to equal the exact
   complete atomic prefix.
6. `.github/workflows/fuzz-smoke.yml` uses nightly Rust and cargo-fuzz 0.12.0
   to build and smoke each of the four targets in a matrix. Every run has
   `-max_len=65536`, `-runs=10000`, `-max_total_time=15`, and a ten-minute job
   timeout. Path filters keep the normal workspace test workflow unaffected.

## Test strategy

### Reference MVCC/TTL model

`crates/meteordb/tests/engine_model.rs` runs 240 operations for each recorded
seed:

```text
0x4d4554454f520010
0x4d4554454f521010
0x4d4554454f522010
0x4d4554454f523010
```

The model is a `BTreeMap<Vec<u8>, Vec<Version>>` with sequence numbers,
tombstones, absolute TTL deadlines, and snapshot sequence views. Generated
operations include puts, deletes, atomic batches, TTL writes, snapshots, point
reads, bounded scans, prefix scans, clock advances, flushes, compactions, and
reopens. Every mutation is followed by all-key, scan, prefix, and retained
snapshot comparisons. Flush/compaction/reopen boundaries occur throughout each
run.

Reproduce one failure by retaining its printed seed and reducing `SEEDS` to
that value:

```bash
cargo test -p meteordb --test engine_model -- --nocapture
```

### Crash/restart matrix

`FaultyFs` records a one-based sequence of write, file-sync, path-sync,
atomic-install, atomic-replace, directory-sync, truncate, and remove
operations. It separately tracks volatile bytes, file-synchronized bytes, and
directory-synchronized durable names. Existing regular files are seeded as
durable when first read, opened, synchronized, truncated, renamed, installed,
or removed; exclusive creates and absent append targets start only in the
volatile image. Rename/install move volatile and synchronized state while
leaving the prior durable directory image intact until directory sync.

The crash suite first records a complete workload, then reruns it from an empty
directory with an injected crash immediately before every recorded operation.
Each crash restores the durable image before the failed engine is discarded and
reopened normally. A separate deterministic regression creates and closes a
database, reopens it with a crash injected before the first directory sync, and
compares every regular file byte-for-byte with the pre-reopen image. It proves
the previously durable database survives, the newly created unsynchronized WAL
name disappears, and a normal reopen still reads the prior acknowledged key.

Assertions require:

- every acknowledged synchronous batch survives;
- every unacknowledged two-key batch is either wholly visible or wholly absent;
- WAL append/sync, SSTable sync/install, manifest append/sync, CURRENT install,
  directory sync, and obsolete WAL/SSTable deletion are all reached.

MeteorDB currently creates `CURRENT` once with `atomic_install`; it does not
rotate manifests or call `atomic_replace`, so there is no CURRENT-replace
runtime boundary to exercise yet. `FaultyFs` records `AtomicReplace` for future
manifest rotation.

The crash seed is `0xc2a50010`:

```bash
cargo test -p meteordb --test crash_recovery -- --nocapture
```

### Corruption

`crates/meteordb/tests/corruption.rs` creates a database containing manifest,
CURRENT, WAL, and SSTable data, copies it per case, and both flips and truncates
each persisted format class. Manifest, CURRENT, and SSTable mutations must
return `Error::Corruption` or `Error::UnsupportedFormat`, never panic. WAL
checksum damage must return typed corruption; structurally truncated WAL tails
must recover only a complete atomic prefix, matching the documented recovery
format.

The recorded corruption seed is `0xc0220010`:

```bash
cargo test -p meteordb --test corruption -- --nocapture
```

### Concurrency and file lifetime

`crates/meteordb/tests/concurrency.rs` uses reusable barriers, not sleeps. A
writer, snapshot reader, and maintenance thread execute twelve deterministic
rounds. Each reader captures before the write, verifies its old value after the
write, and releases only after flush/compaction completes.

A second regression holds an iterator's immutable version across compaction,
asserts every referenced SSTable remains linked, and reads successfully from
the iterator afterward.

```bash
cargo test -p meteordb --test concurrency -- --nocapture
```

### Fuzzing

The standalone `fuzz/` workspace contains targets and recorded corpus seeds
for WAL inspection, stored/raw block decoding, manifest inspection with
bounded retained metadata, and composite/internal-key decoding. It is not a
member of the normal workspace, so normal tests do not require nightly Rust or
libFuzzer.

CI now builds and bounded-smoke-runs every target on `ubuntu-latest` with
nightly Rust, cargo-fuzz 0.12.0, the default sanitizer, a 65,536-byte maximum
input, 10,000-run maximum, 15-second maximum, and ten-minute job timeout.

This host still has no C++ compiler, so a fresh local cargo-fuzz build stops in
`libfuzzer-sys` before target compilation. Earlier Task 16 bounded runs used
nightly Rust and `--sanitizer none`; all four completed 10-second budgets:

```bash
cargo +nightly fuzz run wal_decode --sanitizer none -- -max_total_time=10
cargo +nightly fuzz run block_decode --sanitizer none -- -max_total_time=10
cargo +nightly fuzz run manifest_decode --sanitizer none -- -max_total_time=10
cargo +nightly fuzz run composite_key_decode --sanitizer none -- -max_total_time=10
```

Observed executions were approximately 3.2M, 3.0M, 2.2M, and 4.4M
respectively, with no crashes.

## Bugs found and fixed

1. `471ae4d` / `9d2cb94`: an iterator could retain an older version that shared
   files with a later version, while compaction tracked only the immediately
   replaced version. Obsolete files could consequently be unlinked too early.
   Reader-version leases now protect all live reader versions and expire
   independently of obsolete-version queue ownership.
2. `abb6a1c`: a crash during initial manifest or CURRENT publication left
   `.tmp` metadata, or a valid installed manifest without CURRENT, making every
   reopen fail. Creation now removes stale temporary metadata and resumes
   publication of a validated installed initial manifest without weakening
   no-follow/symlink checks.

## Validation

All final Cargo gates use repository-local GCC 13:

```text
gcc-13 (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
```

```bash
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export PATH="$ROOT/usr/bin:$PATH"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"

cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --workspace
git diff --check
```

All five gates passed after the crash-oracle baseline fix, including the
reopen-existing-database regression and the complete crash, model, corruption,
and reader-lease suites.

## Concerns

- CURRENT replacement remains untestable until manifest rotation exists;
  initial CURRENT installation is fully crash-injected.
- Fresh local fuzz build/smoke validation is blocked because this host has no
  `c++`, `g++`, or `clang++`. The new GitHub Actions matrix provides the
  sanitizer-enabled build and bounded smoke gate on a complete hosted
  toolchain.
