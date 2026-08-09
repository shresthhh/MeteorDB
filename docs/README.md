# MeteorDB documentation

## Use MeteorDB

- [Project quickstarts and status](../README.md)
- [Storage-engine concepts](storage-engine.md)
- [Durability, TTL clocks, and failures](durability.md)
- [Inspection and operations](operations.md)
- [API documentation](https://docs.rs/meteordb)

## Evaluate the implementation

- [Architecture and invariants](architecture.md)
- [Persistent file formats and compatibility](file-formats.md)
- [Reproducible benchmark methodology](benchmarks.md)
- [Roadmap and non-goals](../ROADMAP.md)

## Contribute and release

- [Contribution guide](../CONTRIBUTING.md)
- [Development and validation](development.md)
- [Release checklist](release-checklist.md)
- [Changelog](../CHANGELOG.md)
- [Security policy](../SECURITY.md)

Implementation is in [`crates/meteordb`](../crates/meteordb); public examples
are in [`crates/meteordb/examples`](../crates/meteordb/examples), integration
tests in [`crates/meteordb/tests`](../crates/meteordb/tests), and CLI tooling in
[`crates/meteordb-cli`](../crates/meteordb-cli).
