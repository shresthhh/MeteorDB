# Task 15 Report: Inspection CLI and Structured Statistics

## What was added

MeteorDB now has a `meteordb` binary with four commands:

- `check` validates `CURRENT`, every manifest edit, referenced SSTable lengths,
  SSTable metadata and data-block checksums, and required WAL checksums.
- `dump-manifest` prints validated edits and the final seven-level file layout.
- `dump-sstable` prints checked properties, block locations, key ranges, and a
  caller-bounded entry sample.
- `bench` runs a seeded `inference-cache` workload and reports configuration,
  dataset size, operations, throughput, p50/p95/p99 latency, and engine stats.

Inspection is deliberately separate from writer recovery. It only opens files
for reading: it does not acquire `LOCK`, append to a manifest, recover an
engine, truncate a torn tail, or create a replacement WAL. A torn manifest is
reported as corruption and left byte-for-byte unchanged.

## Beginner walkthrough

1. Clap parses the required top-level `--path` and then one subcommand.
2. `check` calls `inspect_manifest`, which reuses the engine's checked
   `CURRENT`, manifest framing, checksum, edit decoder, counter validation, and
   version-building logic.
3. Each live table is opened with `TableReader`. `TableReader::inspect` walks
   the normal iterator, so every data block passes the existing checksum,
   compression, block, and internal-key decoders.
4. Required `.wal` files are passed to `inspect_wal`, which delegates to the
   existing WAL replay parser. The CLI never implements a second disk-format
   decoder.
5. Human output uses a fixed field order. JSON uses versioned structs and
   Serde's struct field order, so names and serialization remain stable.
6. Output samples are bounded by `--max-edits`, `--max-blocks`, and
   `--max-entries`; validation still covers the complete file.

## Clap and parser design

`clap` derive supplies usage errors and exit code 2 before command execution.
Operational failures return 1. Corruption and unsupported persistent formats
return 3. Successful commands return 0.

The CLI accepts only canonical relative SSTable names such as `000042.sst`, so
`dump-sstable` cannot escape the database directory. Canonical WAL filenames
are recognized with the same six-digit-or-wider number rule used by the
engine. Storage bytes are decoded only by library inspection APIs.

## Structured statistics

`StatsSnapshot`, `CacheSnapshot`, and `CachePartitionSnapshot` now implement
`Serialize`. The existing mutex-protected read counters preserve related
invariants in one snapshot, and cache partitions are copied while holding the
cache mutex. A byte-exact JSON test locks field names and order.

## Examples

```bash
meteordb --path ./database check
meteordb --path ./database check --format json
meteordb --path ./database dump-manifest --format json --max-edits 100
meteordb --path ./database dump-sstable --file 000004.sst \
  --max-blocks 20 --max-entries 20
meteordb --path ./bench-db bench --seconds 1 --workload inference-cache \
  --seed 7 --dataset-size 1000 --format json
```

## TDD and verification

The integration suite was written before the command implementation. The first
red invocation exposed the environment's missing system linker; a local GCC
toolchain was then used for every executable gate. Focused red/green cycles
also covered WAL sequence gaps and bounded block output. The suite covers
live-writer read-only inspection, unchanged torn manifests, SSTable and WAL
corruption, human and JSON manifest output, bounded SSTable output, benchmark
metrics, and usage exits.

All gates used GCC 12.4.0:

```text
cargo test -p meteordb-cli --test cli
  8 passed

cargo test -p meteordb stats::tests::snapshot_json_has_stable_field_order_and_names
  1 passed

cargo fmt --all -- --check
  passed

cargo clippy --workspace --all-targets -- -D warnings
  passed

cargo test --workspace
  all unit, integration, CLI, and doc tests passed
```

## Concerns

- WAL replay intentionally ignores structurally torn final fragments, matching
  MeteorDB recovery semantics; checksum damage is still surfaced as corruption.
- `check --max-batch-bytes` must match a database created with a non-default WAL
  batch limit.
