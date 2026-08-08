# Task 15 Report: Inspection CLI and Structured Statistics

## Commits

- `82fa15a` — `fix: harden task 15 inspection and benchmarks`
- Documentation/report commit: the commit containing this updated report.
- Remaining review fixes: the commit containing this updated report.

## Review findings fixed

### Remaining Task 15 review fixes

- `check` now reports `ManifestInspection::edits_total`; its one retained edit
  remains an internal validation sample rather than the reported manifest edit
  count. A multi-edit CLI regression compares `check` with `dump-manifest`.
- `ManifestInspectionOptions::max_historical_files` bounds the distinct file
  numbers retained for reuse detection. `check` and `dump-manifest` expose
  `--max-historical-files`. Inspection checks the trusted limit before each new
  insertion, retains every accepted number for reuse checks, and rejects
  immediately rather than forgetting history or accepting an unvalidated
  suffix.
- `TableReaderOptions::max_metadata_bytes` bounds the combined stored index,
  filter, and properties handles before any metadata block allocation or read.
  Engine readers derive this trusted ceiling from configured table/key limits;
  inspection commands expose `--max-metadata-bytes`.
- SSTable output byte accounting now starts with the raw smallest/largest
  property keys. Mandatory property keys over `--max-bytes` reject inspection;
  otherwise sampled-entry truncation remains valid JSON and is described by
  `bytes_truncated`.
- Regressions cover sparse oversized metadata handles, an oversized encoded
  property-key length, property-key output accounting, bounded historical
  tracking, reuse detection within the bound, and the multi-edit `check` count.

### 1. No-follow, same-handle inspection

The root cause was split filesystem handling. Manifest and WAL helpers used
`DurableFs::read_file`, but SSTables used `File::open` directly, manifest
inspection separately called path-based metadata, and engine cached readers did
not receive the engine's injected filesystem.

`DurableFs` now exposes `open_read`, returning a `DurableReadFile` that combines
`Read`, `Seek`, and handle-derived `len`. Unix opens still use `O_NOFOLLOW`;
Windows uses `FILE_FLAG_OPEN_REPARSE_POINT`; the opened object must be a regular
file. Manifest, WAL, SSTable metadata, and SSTable payload reads derive from the
same handle. Engine reads, scans, and compaction pass their existing
`Arc<dyn DurableFs>` into `TableReader`, preserving lazy data-block reads and
the existing cache hit/miss behavior.

Engine, compaction, and `check` also compare the manifest-recorded SSTable
length against the length of the exact handle whose footer, metadata, and data
blocks are validated. Canonical `CURRENT` and `--file` parsing continue to
reject path components and directory escape.

Regressions cover:

- manifest and SSTable symlinks through the CLI;
- direct SSTable symlink rejection;
- an SSTable path replacement after `open_read`, followed by a lazy block read;
- manifest and WAL path replacements after `open_read`;
- injected readable handles, proving these paths no longer bypass `DurableFs`.

### 2. Bounded streaming inspection

The previous manifest inspection retained every decoded edit before applying
`--max-edits`, and WAL inspection called recovery, retaining every decoded
batch and sequence. Output truncation therefore did not bound peak memory.

Manifest and WAL physical framing now read one 32 KiB block at a time from the
same open handle. Complete logical records still go through the existing
`decode_edit` and `decode_batch` parsers. Recovery may collect records as
required by the engine, while inspection callbacks update validation state and
summary counters immediately:

- manifest inspection retains at most `max_edits`, `max_files`, and
  `max_bytes`;
- `max_files` and `max_bytes` also hard-bound the live manifest version needed
  for replay validation;
- edit file lists and final level layout share the file/byte output budgets;
- WAL inspection retains no batches or sequence vector, only counts and
  first/last sequence;
- cross-WAL continuity uses each fully validated segment's contiguous
  first/last range;
- SSTable inspection retains at most `max_entries`, `max_blocks`, and raw
  `max_bytes` of sampled key material while iterating and validating every
  record.

The CLI adds:

- `check --max-files --max-bytes`;
- `dump-manifest --max-files --max-bytes`;
- `check`/`dump-manifest --max-historical-files`;
- `check`/`dump-sstable --max-metadata-bytes`;
- `dump-sstable --max-bytes`.

All count/byte limits reject zero. Tests confirm corruption after the manifest
edit sample cap is still reported.

### 3. Explicit benchmark durability

`bench` now accepts `--durability sync|buffered`, defaults to `sync`, and maps
directly to `Options::durability`. Human and JSON results include
`durability`. Unit coverage locks both enum-to-engine mappings; integration
coverage runs and checks both JSON modes and the human default.

The README documents that `sync` matches `Options::new` and that `buffered`
may acknowledge writes still resident in operating-system buffers.

## CLI compatibility notes

- Existing commands, top-level `--path`, output formats, workload name, and
  benchmark default behavior remain available.
- Benchmark output gains an additive `durability` field/line.
- Manifest JSON gains additive total/truncation fields for bounded edit file
  lists and live levels.
- SSTable JSON/human output gains `shown_bytes` and `bytes_truncated`.
- `shown_bytes` now includes mandatory smallest/largest property-key bytes.
- Zero values formerly accepted for dump sample counts are now usage errors.
- `dump-manifest` and `dump-sstable` defaults remain bounded; callers needing
  larger output must raise the explicit limits.
- Library `TableReader::inspect(max_entries, max_blocks)` retains its previous
  zero-sample behavior. New callers can use `inspect_with_limits`.

## TDD evidence

CLI regressions were written first. The GCC run failed because durability,
`--max-files`, `--max-bytes`, and positive-limit validation did not exist.
After implementation, the focused CLI suite passed 13 tests.

The remaining-review regressions failed first because `check` reported its
single retained edit, and the historical/metadata limit fields did not exist.
After implementation, the manifest, SSTable, and CLI focused suites pass 26,
18, and 15 tests respectively. The SSTable suite includes sparse oversized
metadata handles and a `u64::MAX` property-key length declaration without
constructing corresponding payload buffers.

## Full GCC validation

All final gates used:

```text
gcc-13 (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
```

Environment:

```bash
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export PATH="$ROOT/usr/bin:$PATH"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"
```

Successful gates:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --workspace
git diff --check
```

## Residual risks

- `O_NOFOLLOW`/reparse-point protection applies to the final database-file
  component. MeteorDB still trusts the database directory path supplied by the
  caller; securing every ancestor against concurrent privileged rename would
  require a directory-handle/open-at API.
- One manifest edit and one WAL batch may allocate up to their existing trusted
  parser limits while being decoded. Inspection no longer accumulates multiple
  decoded records.
- WAL torn-tail handling intentionally remains identical to recovery:
  structurally incomplete final fragments are ignored, while checksum damage
  remains corruption.
