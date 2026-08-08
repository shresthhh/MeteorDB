# MeteorDB

MeteorDB is an experimental embedded key/value storage engine written in Rust.
It is being built to explore durable, ordered storage for AI-oriented workloads
while keeping the core API useful as a general byte-key/byte-value engine.

> **Implementation status:** MeteorDB is under active development and is not
> ready for production data. The current engine stores committed versions in
> memory and writes them to a WAL, but it does not recover them when reopened.
> `Engine::open` currently starts with an empty memtable and creates or
> truncates `000001.wal`.

## What works today

The current crate provides:

- `Engine` point reads, puts, deletes, atomic `WriteBatch` commits, snapshots,
  explicit WAL synchronization, and idempotent close;
- configurable `Options`, typed errors, and synchronous or buffered durability;
- MVCC internal keys that order user keys and historical versions;
- a checksummed, fragmented write-ahead log plus public WAL writer and replay
  APIs; and
- a serialized write path that publishes a complete batch to the in-memory
  table only after its WAL append succeeds.

Atomic batches and snapshot-isolated point reads are supported. General
transactions are not.

Only the database path, durability mode, and key/value/batch input limits
currently affect the in-memory engine. SSTable sizing and block layout,
Bloom-filter, block-cache, compression, and memtable-rotation configuration are
public forward-compatible surfaces, but they are not operational until those
storage subsystems land.

## Prerequisites

- Rust 1.88.0 with Cargo
- A working native C toolchain whose linker is available as `cc`

The repository's `rust-toolchain.toml` selects Rust 1.88.0 with `rustfmt` and
`clippy`. On Ubuntu or Debian, install the native toolchain with:

```bash
sudo apt install build-essential
```

On macOS, install the Xcode command-line tools with `xcode-select --install`.

## Build and test

```bash
git clone https://github.com/shresthhh/MeteorDB.git
cd MeteorDB
cargo build --workspace
cargo test --workspace
```

Run the executable quickstart:

```bash
cargo run -p meteordb --example quickstart
```

It prints:

```text
current profile: database engineer
```

## Quickstart

The complete runnable source is
[`crates/meteordb/examples/quickstart.rs`](crates/meteordb/examples/quickstart.rs).
Its central API flow is:

```rust
use meteordb::{Engine, Options, Result, WriteBatch};

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let database = tempfile::tempdir()?;
    run(database.path())?;
    Ok(())
}

fn run(path: &std::path::Path) -> Result<()> {
    let engine = Engine::open(Options::new(path))?;

    let mut batch = WriteBatch::default();
    batch
        .put("user:42", "Ada")
        .put("profile:42", "systems researcher");
    engine.write(batch)?;

    let snapshot = engine.snapshot()?;
    engine.put("profile:42", "database engineer")?;

    assert_eq!(
        snapshot.get("profile:42")?.as_deref(),
        Some(b"systems researcher".as_slice())
    );

    let current = engine.get("profile:42")?.expect("profile should exist");
    println!("current profile: {}", String::from_utf8_lossy(&current));

    drop(snapshot);
    engine.close()
}
```

`tempfile::tempdir()` creates a uniquely owned directory and removes it
automatically. The snapshot is dropped and the engine is closed inside `run`,
before the temporary-directory handle is dropped; errors also unwind `run` and
drop its handles before automatic cleanup. The example never recursively
deletes a shared predictable path.

## How a write becomes visible

MeteorDB first validates the whole batch. One mutex then gives concurrent
writers a definite order and assigns the batch one sequence number. The engine
encodes the batch as one logical WAL record; the default synchronous durability
mode asks the operating system to sync that record before continuing.

Only after the WAL append succeeds does the engine add every operation to the
MVCC memtable and publish the sequence to readers. A failed append therefore
does not expose part of a batch in memory. Ordinary point reads use the latest
published sequence. A snapshot keeps the sequence captured when it was created,
so later updates do not change what that snapshot sees.

This is only the current in-memory write/read path. Flushing to sorted tables,
reopening from durable state, and reclaiming old versions are not implemented
yet.

## Feature status

| Capability | Status |
| --- | --- |
| Typed options, errors, clocks, and owned write batches | Implemented |
| MVCC internal-key encoding and snapshot lifetime tracking | Implemented |
| Atomic serialized batches in a WAL-backed in-memory engine | Implemented |
| Checksummed and fragmented WAL writer/replay APIs | Implemented |
| Point `get`, `put`, `delete`, snapshots, `sync`, and `close` | Implemented |
| Engine recovery from existing WAL files | Planned |
| Memtable rotation and flush | Planned |
| SSTables and block cache | Planned |
| Bloom filters | Planned |
| Range and prefix scans | Planned |
| Leveled compaction and safe version reclamation | Planned |
| TTL enforcement | Planned |
| AI workload adapters | Planned |

## Inspection resource limits

`Engine::open` assumes its database directory is trusted, local engine state.
Engine readers therefore accept all structurally valid SSTable metadata that
MeteorDB writers can produce; sizing targets are not security limits and cannot
soundly cap retained MVCC history. Use the inspection CLI, not `Engine::open`,
when examining untrusted database files.

Inspection commands apply caller-trusted limits before retaining replay state
or allocating SSTable metadata buffers:

```bash
meteordb --path ./db check \
  --max-historical-files 100000 --max-metadata-bytes 67108864
meteordb --path ./db dump-sstable --file 000042.sst \
  --max-bytes 1048576 --max-metadata-bytes 67108864
```

`--max-historical-files` bounds the distinct manifest and SSTable numbers
remembered to detect reuse. If the next new number would exceed the limit,
inspection rejects the manifest immediately; it never forgets accepted numbers
or silently skips the remaining stream. `--max-metadata-bytes` bounds the
combined stored index, filter, and properties handles before those blocks are
read.

SSTable `--max-bytes` includes raw smallest/largest property-key bytes and
sampled entry-key bytes. Inspection rejects a table when its mandatory property
keys alone exceed that output budget. Otherwise JSON output remains complete
and valid, with `bytes_truncated` describing omitted entry samples.

## AI workload direction

MeteorDB's roadmap includes typed adapters for inference caching, feature
storage, and embedding metadata over the same ordered byte API. These are design
goals, not completed features. Approximate-nearest-neighbor search is outside
the current project scope.

Future comparisons with RocksDB should run equivalent workloads with documented
hardware, configuration, datasets, and reproducible commands. The repository
does not currently publish benchmark results or performance claims.

The benchmark CLI defaults to synchronous durability, matching
`Options::new`:

```bash
meteordb --path ./bench-db bench --seconds 10 \
  --workload inference-cache --durability sync
```

Use `--durability buffered` only when intentionally measuring writes that may
remain in operating-system buffers after acknowledgment. Human and JSON
benchmark results include the selected durability mode.

## Design documents

- [MeteorDB design](docs/superpowers/specs/2026-07-17-meteordb-design.md)
- [MeteorDB implementation plan](docs/superpowers/plans/2026-07-17-meteordb.md)

These documents describe the intended full engine. Their roadmap sections
should not be read as statements about the current implementation.

## Contributing

Keep public documentation aligned with exported code and clearly distinguish
implemented behavior from roadmap work. Before submitting a change, run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
```
