# `meteordb` crate

This crate implements MeteorDB's embedded LSM storage engine. It owns write
sequencing, WAL recovery, MVCC memtables and snapshots, immutable SSTables,
durable version metadata, background flush, point reads, block caching, and
read statistics.

## Public surface

Applications normally use `Engine`, `Options`, `WriteBatch`, `Snapshot`, and
the crate-wide `Result` and `Error` types. The crate root also exports lower
level WAL, memtable, SSTable, manifest, cache, filesystem, and version types
used to test and evolve their contracts.

The engine supports atomic batches, point `get`, snapshots, explicit `sync`
and `flush`, and deterministic `close`. Synchronous durability is the default;
buffered writes require `sync` or `close` for a stable-storage guarantee.
Recovery validates checksums and persistent structure, accepts only documented
torn-tail cases, and returns errors for complete corruption or missing required
files.

The API and file formats are pre-alpha and may change without migration
support. Range scans, compaction, version reclamation, and TTL enforcement are
not implemented.

## Code and validation

- [`src`](src) contains the library modules.
- [`examples`](examples) contains runnable public-API examples.
- [`tests`](tests) contains public-contract and cross-component integration
  tests.

See the repository [architecture reference](../../docs/architecture.md) for
write, read, flush, recovery, concurrency, and durability invariants. Local
build and validation commands are in the
[development guide](../../docs/development.md).
