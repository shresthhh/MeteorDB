# Reproducible benchmarks

These benchmarks are development tools, not published performance results.
Do not treat one run, one host, or the smoke profile as a universal claim about
either engine. Keep raw result files local and compare only runs produced on
the same otherwise-idle machine with the same workload JSON and toolchain.

## Workload definition

`benchmarks/workloads/v1-smoke.json` is the canonical short profile. The
versioned schema rejects unknown fields and unsupported versions. It records:

- the ChaCha8 seed, key count/size, absent-key count, 1 KiB cache values, and
  6 KiB embedding values;
- uniform or explicit Zipfian access (`theta` is serialized);
- operation mixes, batch and scan sizes, TTL churn frequency and duration;
- synchronous or buffered durability, compression, cache budget, foreground
  thread count, write-buffer target, and file-size target; and
- warm-up operations, measured operations, and latency sampling interval.

Generation is byte-for-byte deterministic. Present and absent keys occupy
different namespaces. Values are seeded pseudorandom bytes. The comparison
runner creates a fresh database for every workload, seeds and flushes it before
warm-up, excludes warm-up from measurements, and uses the same generated
access schedule for both engines.

| Workload | Semantics |
| --- | --- |
| `point-read-write` | Present and deterministic absent gets, 1 KiB puts, deletes, and bounded scans |
| `inference-cache` | Zipfian hot reads, 1 KiB values, and periodic TTL writes |
| `feature-store` | Zipfian point access, TTL churn, and ordered range history scans |
| `embedding-storage` | Uniform batched gets and atomic batched puts of 6 KiB values |
| `prefix-scan` | Bounded scans using a declared key-prefix length |
| `range-scan` | Bounded ordered half-open range scans |
| `compaction` | Overwrite batches followed by flush and explicit compaction |

The smoke profile uses one foreground thread, no compression, 8 MiB cache, 4
MiB write buffers/files, 50 warm-up operations, 200 measured operations, and
one latency sample per operation. It is intended only to prove the harness.
Create a separate versioned JSON file with larger `key_count` and measurement
counts for analysis; do not edit captured output and call it reproducible.

## Criterion component benchmarks

```bash
cargo bench -p meteordb --bench engine
cargo bench -p meteordb --bench workloads
```

`engine` covers present/absent point gets, put, delete, atomic batches, prefix
and range scans, flush, compaction, and recovery. `workloads` covers inference
cache, feature point/history access, and embedding batch I/O. Every benchmark
uses deterministic fixtures and a fresh database under `target/benchmark-tmp`.
Criterion is explicitly configured for 250 ms warm-up, 500 ms measurement,
ten samples, no plots, and operation/byte throughput units.

Criterion's measured closures contain only the operation under test. Each
harness separately runs a fixed 32-operation reporting probe after Criterion
timing, emits its versioned JSON on stdout, and writes the same sidecar under
`target/criterion/meteordb-sidecars/{engine,workloads}`. Sidecars contain
nearest-rank p50/p95/p99, recursive database bytes, and available
read/write/space amplification counters with explicit `null` values and notes
when the engine does not expose a relevant physical counter.
Flush and compaction prepare a fresh dirty or compactable database before
every reporting sample, outside its timed interval, and reject any sample that
does no work. Counter-based amplification uses coherent before/after snapshots
for that reporting probe rather than process-lifetime totals.

Criterion's own estimate format is distinct from both these component
sidecars and the comparison JSON. The comparison runner records nearest-rank
p50/p95/p99 latency from the declared sampling interval, total measured
operations, elapsed operation time, and operations/second.

## Comparison runner

MeteorDB requires only the repository's normal Rust/native linker setup:

```bash
scripts/run-benchmark-comparison.sh \
  --engine meteordb --workload benchmarks/workloads/v1-smoke.json
```

RocksDB additionally requires `cc`, `c++`, CMake, pkg-config, and libclang:

```bash
scripts/check-rocksdb-bench-deps.sh
cargo run -p meteordb-rocks-bench -- --engine rocksdb --smoke
```

The dependency checker reports every missing tool with an installation hint
and exits before invoking the native Cargo build. The exact command above
automatically enables the feature-gated native implementation after the check,
while normal workspace builds do not require those native dependencies.

Results go to stdout unless `--output` is explicitly supplied. Output paths
must already be ignored by Git:

```bash
scripts/run-benchmark-comparison.sh --engine meteordb --smoke \
  --output .bench-results/meteordb-smoke.json
```

The script uses the repository-local GCC 13 workaround when present. To invoke
Cargo directly on this host:

```bash
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export PATH="$ROOT/usr/bin:$PATH"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"
```

## Result schema and interpretation

Both engines emit `ComparisonResult` schema version 1. It contains the engine
and version, Git revision (with a dirty marker), OS/architecture, CPU model,
logical CPU and memory totals, Rust/Cargo/native tool versions, complete
workload and engine options, per-workload throughput and p50/p95/p99, process
peak RSS, reopen time, recursive database bytes, amplification fields, and
explicit equivalence/non-equivalence notes.

Database bytes include every per-workload database and the separate recovery
fixture. Peak RSS is process high-water RSS, not isolated cache memory.
MeteorDB read amplification is the measured-interval delta in SSTable probes
divided by the measured-interval delta in point reads.
Equivalent portable physical-byte and live-data counters are unavailable, so
write/space amplification are `null`; RocksDB's read amplification is also
`null`. The accompanying notes are part of the result and must not be dropped.

## Fairness boundaries

The runner maps the same logical schedule and these options:

- `sync` to MeteorDB synchronous durability and RocksDB `WriteOptions::sync`;
- `buffered` to their corresponding unsynchronized acknowledgment modes;
- cache bytes, write-buffer bytes, target file bytes, and no compression.

MeteorDB currently writes uncompressed SSTables and exposes no runtime
compression option. Its runner therefore rejects `snappy`; the canonical fair
profile uses `none`. RocksDB can parse `snappy` for RocksDB-only experiments,
but such a run is not an equivalent comparison.

The following are deliberately labeled non-equivalent in JSON:

- RocksDB has no per-entry TTL in this runner. TTL-marked writes use normal
  puts and do not measure expiry, while MeteorDB records expirations.
- MeteorDB performs one highest-priority leveled compaction; RocksDB
  `compact_range` covers the full key range.
- cache layout, Bloom filters, file format, recovery algorithm, and background
  scheduling are engine-specific. `threads` is foreground workload concurrency
  and currently must be `1` for both frontends. RocksDB's two background jobs
  are configured independently; MeteorDB owns its engine-internal background
  worker.

Run on local storage, record power/CPU policy and free-space conditions beside
the JSON, avoid concurrent workloads, and compare repeated runs rather than a
single number. Never commit generated result files as authoritative results.
