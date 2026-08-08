# Task 15 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate inspection filesystem races, bound inspection memory/output, and expose benchmark durability accurately.

**Architecture:** Add a no-follow readable-file handle to `DurableFs` and make SSTable, manifest, and WAL validation derive metadata and bytes from that same handle. Refactor manifest/WAL replay around streaming callbacks so inspection retains only caller-limited samples while still validating the complete input. Add explicit CLI limits and a `sync|buffered` benchmark option mapped directly to `Options::durability`.

**Tech Stack:** Rust 1.88, std I/O traits, clap, serde, existing MeteorDB parsers, Cargo test/clippy/rustdoc, GCC 13.

## Global Constraints

- Preserve SSTable lazy data-block reads and existing block-cache behavior.
- Reuse existing manifest, WAL, and SSTable format decoders.
- Reject zero or nonsensical resource limits.
- Continue validating corruption after every sample/output cap is reached.
- Commit without a co-author trailer and do not push.

---

### Task 1: No-follow readable handles

**Files:**
- Modify: `crates/meteordb/src/fs.rs`
- Modify: `crates/meteordb/src/sstable/reader.rs`
- Modify: `crates/meteordb/src/engine.rs`
- Test: `crates/meteordb/tests/sstable.rs`

**Interfaces:**
- Produces: `DurableReadFile`, `DurableFs::open_read`, and handle-backed `TableReader` opens.

- [ ] Add failing tests proving SSTable symlinks are rejected and an injected readable handle, rather than direct `std::fs::File::open`, serves reads and metadata.
- [ ] Run the focused SSTable tests and observe the old direct-open behavior fail.
- [ ] Implement `DurableReadFile: Read + Seek + Send` with `len()`, implement it for OS files, and route all `TableReader` opens through one `DurableFs::open_read` handle.
- [ ] Pass the engine's existing filesystem abstraction into cached table opens without changing lazy reads or cache admission.
- [ ] Run focused SSTable/read-path tests.

### Task 2: Streaming bounded manifest and WAL inspection

**Files:**
- Modify: `crates/meteordb/src/manifest.rs`
- Modify: `crates/meteordb/src/wal.rs`
- Modify: `crates/meteordb/src/lib.rs`
- Test: `crates/meteordb/tests/manifest.rs`
- Test: `crates/meteordb/tests/wal.rs`

**Interfaces:**
- Produces: validated streaming replay callbacks, `ManifestInspectionOptions`, bounded edit/live-file samples, and summary-only `WalInspection`.

- [ ] Add failing tests for manifest/WAL symlinks, same-handle metadata, bounded retained samples, and corruption after the sample cap.
- [ ] Run focused manifest/WAL tests and confirm failure.
- [ ] Refactor physical-block replay to read incrementally from `DurableReadFile`; invoke existing edit/batch decoders per complete logical record.
- [ ] Keep recovery collection behavior where required, but make inspection update counters/version/summary incrementally and retain at most configured edits/files.
- [ ] Remove retained WAL batch/sequence vectors from inspection; validate internal continuity while streaming and use first/last sequence for cross-segment checks.
- [ ] Run focused manifest/WAL tests.

### Task 3: CLI output limits and durability

**Files:**
- Modify: `crates/meteordb-cli/src/main.rs`
- Modify: `crates/meteordb-cli/tests/cli.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: bounded inspection options and streaming WAL summaries.
- Produces: `dump-manifest --max-edits --max-files`, `dump-sstable --max-entries --max-blocks --max-value-bytes`, and `bench --durability sync|buffered`.

- [ ] Add failing CLI tests for zero limits, bounded JSON/human layout and payload output, post-cap corruption detection, both benchmark durability modes, default help text, and JSON/human durability fields.
- [ ] Run `cargo test -p meteordb-cli --test cli` and confirm the new assertions fail.
- [ ] Validate all limits with clap ranges or explicit errors; pass them into library inspection before decoding/retention.
- [ ] Add a clap durability enum defaulting to `sync`, set `Options::durability`, and serialize/render the selected mode.
- [ ] Document the benchmark default and crash-safety trade-off in README examples/help.
- [ ] Run the CLI integration suite.

### Task 4: Full validation and report

**Files:**
- Modify: `.superpowers/sdd/task-15-report.md`

**Interfaces:**
- Produces: final evidence, compatibility notes, residual risks, and commits.

- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Run `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- [ ] Run `cargo test --workspace`.
- [ ] Review `git diff --check` and the complete diff for unrelated changes.
- [ ] Update the report with root causes, APIs/CLI changes, TDD evidence, exact GCC commands, compatibility, and residual risks.
- [ ] Commit the fixes and report without a trailer; verify the final worktree is clean.
