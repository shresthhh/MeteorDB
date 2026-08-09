# Task 17 Report: Reproducible Benchmarks and RocksDB Comparison

## Commits

- `a28be3a` — `bench: add reproducible RocksDB comparison`
- `78ac59e` — `docs: document benchmark methodology`
- Report: the commit containing this file.

No commit has a trailer, and nothing was pushed.

## Delivered

- `meteordb-bench-support` owns the versioned workload/result schemas,
  validation, fixed-seed ChaCha8 fixtures, uniform and Zipfian schedules,
  absent-key namespace, grouped scan prefixes, nearest-rank latency summaries,
  and host/tool capture.
- Criterion harnesses cover present/absent get, put, delete, atomic batches,
  prefix/range scans, flush, compaction, recovery, inference cache with TTL,
  feature point/history access, and 6 KiB embedding batch I/O.
- `meteordb-rocks-bench` accepts either the canonical versioned JSON file or
  `--smoke`, excludes declared warm-up operations, samples at the declared
  interval, and emits one `ComparisonResult` schema for either engine.
- The comparison schema includes Git revision, hardware, OS, compiler/tools,
  complete options/workload, throughput, p50/p95/p99, peak RSS, recovery time,
  database bytes, amplification fields/availability notes, and explicit
  semantic equivalence and non-equivalence.
- The canonical smoke fixture uses seed `0x4d4554454f524442`, 100 32-byte keys,
  25 absent keys, 1 KiB cache values, 6 KiB embedding values, one foreground
  thread, synchronous durability, no compression, 8 MiB cache, 4 MiB write
  buffers/files, 50 warm-up operations, 200 measured operations, and one
  latency sample per operation.
- Native RocksDB remains optional. The dependency checker verifies `cc`, `c++`,
  CMake, pkg-config, and libclang and prints installation-oriented diagnostics
  before Cargo. The runner writes files only after explicit `--output`, and
  only to a Git-ignored path.

## Fairness and interpretation

Both engines consume the same generated data, operation schedule, warm-up,
sample policy, foreground operation count, durability choice, no-compression
setting, cache budget, write-buffer target, and target file size.

The output and documentation label unavoidable differences:

- RocksDB TTL writes are ordinary persistent puts; MeteorDB records per-entry
  expiry, and expiry itself is not measured.
- MeteorDB compacts one highest-priority level while RocksDB `compact_range`
  covers the full key range.
- cache organization, Bloom filters, file format, recovery, and background
  scheduling are engine-specific.
- MeteorDB exposes SSTable probes/read as read amplification. Equivalent
  portable physical-byte/live-data counters are unavailable, so unsupported
  read/write/space fields are `null` with notes rather than invented values.
- MeteorDB has no runtime compression option. Equivalent comparisons therefore
  use `none`; the MeteorDB runner rejects a Snappy workload instead of silently
  reporting a mismatched configuration.

`docs/benchmarks.md` prohibits universal claims and explains that smoke runs
only prove the harness. No generated result file or throughput claim is
committed.

## TDD evidence

- The interrupted support crate initially passed its five existing fixture
  tests. New tests were then added first. The first focused run failed to
  compile because `capture_environment` did not exist; environment capture,
  fixed comparison value sizes, and complete workload coverage were
  implemented, after which the suite passed.
- The runner's three interrupted CLI tests were run against the empty `main`
  and all failed for the expected reasons: empty output, unexpected RocksDB
  success, and missing argument conflict. The tests were extended for invalid
  files and stable result content before implementation; all four now pass.
- A prefix-fixture regression failed with only one matching key. Key generation
  was changed to deterministic 16-key prefix groups, and the focused test then
  passed.
- The dependency-check command first failed because the script did not exist.
  After implementation it failed before Cargo with one actionable diagnostic
  for every unavailable native dependency.

The interrupted Criterion harnesses already compiled in test mode when
resumed. They were retained after audit, corrected so feature history scans
actually return the declared 20 records, and revalidated.

## Validation

All Cargo gates used the repository-local GCC 13 workaround:

```bash
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export PATH="$ROOT/usr/bin:$PATH"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"
export TMPDIR="$PWD/target/benchmark-tmp"
```

Successful final gates:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo bench -p meteordb --bench engine -- --test
cargo bench -p meteordb --bench workloads -- --test
cargo bench --no-run
bash -n scripts/check-rocksdb-bench-deps.sh scripts/run-benchmark-comparison.sh
git diff --check
```

A final MeteorDB smoke run emitted schema version 1 with all seven workload
results and ordered p50/p95/p99 values. The ignored output file was deleted.

Captured host/tool metadata:

```text
Linux x86_64
AMD Ryzen 9 8945HS w/ Radeon 780M Graphics
16 logical CPUs, 16,393,633,792 bytes memory
rustc 1.88.0 (6b00bc388 2025-06-23)
cargo 1.88.0 (873a06493 2025-05-10)
git 2.43.0
```

## Concern

This host has no system `cc`, `c++`, CMake, pkg-config, or libclang. The
repository-local GCC is sufficient for all normal Rust gates, but there is no
C++ compiler or libclang for `librocksdb-sys`; therefore the feature-enabled
RocksDB build and smoke measurement could not run locally. The dependency
checker verified that this condition fails clearly before Cargo and gives
installation instructions.
