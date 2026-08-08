# Development guide

This guide covers the local workflow for changing MeteorDB. Read the
[contribution guide](../CONTRIBUTING.md) for review and correctness
expectations and the [architecture reference](architecture.md) before changing
storage behavior.

## Toolchain

MeteorDB uses Rust 1.88.0 with the 2024 edition. The checked-in
[`rust-toolchain.toml`](../rust-toolchain.toml) installs `rustfmt` and Clippy.
A native C toolchain with a linker available as `cc` is also required.

Verify the environment:

```bash
rustc --version
cargo --version
cc --version
```

## Build and run

From the repository root:

```bash
cargo build --workspace
cargo test --workspace
cargo run -p meteordb --example quickstart
```

The workspace currently contains the `meteordb` crate. Use `-p meteordb` for
crate-focused commands.

## Focused tests

Run one integration-test target while iterating:

```bash
cargo test -p meteordb --test wal
cargo test -p meteordb --test manifest
cargo test -p meteordb --test recovery
cargo test -p meteordb --test sstable
cargo test -p meteordb --test read_path
```

Run a single named test by passing its name:

```bash
cargo test -p meteordb --test recovery <test_name>
```

Run library unit tests or all tests for the crate:

```bash
cargo test -p meteordb --lib
cargo test -p meteordb
```

## Source placement

- Put storage-engine implementation in
  [`crates/meteordb/src`](../crates/meteordb/src), following the module
  responsibilities in the
  [architecture module map](architecture.md#module-map).
- Keep private, single-module tests in a `#[cfg(test)]` module beside the code.
- Put public-contract and cross-component tests in
  [`crates/meteordb/tests`](../crates/meteordb/tests).
- Put runnable API examples in
  [`crates/meteordb/examples`](../crates/meteordb/examples).
- Put product and technical documentation in [`docs`](.).

Avoid exposing internals only to make an integration test convenient. Test
through public behavior when possible; keep narrowly scoped unit tests near
private decoding and data-structure code.

## Storage-engine tests

Persistent-state changes need more than a success-path test.

### Recovery and corruption

Cover clean reopen, incomplete final writes, and malformed complete data when
changing the WAL, manifest, or SSTable formats. Torn-tail handling must remain
limited to the documented structural cases. Checksum failures, invalid
ordering, missing required files, and inconsistent metadata must remain
errors—not empty reads or cache misses.

### Fault injection

The filesystem boundary in `src/fs.rs` allows tests to record operations,
block synchronization, and fail selected calls. Existing WAL, manifest, and
recovery tests define focused filesystem implementations for these cases.
Follow that pattern to verify:

- file data is synchronized before metadata publishes it;
- directories are synchronized after atomic installation when required;
- obsolete WALs are not removed before replacement recovery state is durable;
- terminal filesystem failures prevent unsafe continued operation; and
- cleanup never deletes a file still referenced by durable state.

Make assertions about the relevant operation order, not only the final return
value.

### Property tests

MeteorDB uses `proptest` as a development dependency. Use it when a behavior is
best expressed as an invariant over many generated operation sequences or byte
layouts. The MVCC tests in `tests/mvcc.rs` provide the current pattern. Keep
generated cases bounded and ensure failures produce a reproducible minimized
input.

## Required validation

Before requesting review, run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
```

Run focused tests first so failures are easier to diagnose. The full workspace
test is still required after focused tests pass.

## Documentation checks

New or changed public APIs require Rustdoc. Keep examples compilable and public
claims aligned with code on `main`.

Build API documentation without dependencies and reject Rustdoc warnings:

```bash
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

When editing Markdown, verify every relative link resolves from the file that
contains it. When editing GitHub forms, parse the YAML and confirm each form
has `name`, `description`, `title`, `body`, and valid unique field IDs.
