# Contributing to MeteorDB

MeteorDB welcomes focused fixes, tests, documentation, and storage-engine
improvements. Correctness and recoverability take priority over throughput or
API convenience.

## Before you start

- Read the [development guide](docs/development.md) and
  [architecture reference](docs/architecture.md).
- Check the [roadmap](ROADMAP.md) before implementing a planned capability.
- Search existing issues and pull requests to avoid duplicate work.
- For a substantial behavior, API, or on-disk-format change, open an issue
  before investing in an implementation.
- Report suspected vulnerabilities through the process in
  [SECURITY.md](SECURITY.md), not in a public issue.

## Choosing an issue

Choose an unassigned issue with a clear outcome, or comment before starting so
work is not duplicated. Keep pull requests limited to one problem. If an issue
requires unrelated refactoring or multiple independently reviewable changes,
split the work into separate pull requests.

Bug reports should include a minimal reproduction and identify the MeteorDB
version or commit, platform, and durability mode. Feature proposals should
describe the workload, access pattern, correctness requirements, and a
measurable success criterion.

## Development workflow

1. Build the workspace and run the relevant existing tests.
2. Add a failing test or a targeted regression case for the behavior being
   changed.
3. Make the smallest complete change that satisfies the test and preserves the
   storage invariants.
4. Run focused tests while iterating.
5. Run the full validation gates documented in the
   [development guide](docs/development.md#required-validation).
6. Update public documentation when behavior, configuration, APIs, file
   formats, or product claims change.

## Correctness expectations

Storage changes must preserve the
[documented invariants](docs/architecture.md#correctness-invariants).
In particular:

- Decode untrusted or persistent bytes with explicit bounds and structural
  validation. Malformed complete data must return a typed error; it must not be
  treated as absence, an empty value, or a recoverable torn tail.
- Never convert an I/O, checksum, decoding, or invariant error into a cache
  miss or key miss.
- Keep filesystem durability ordering explicit. Install and synchronize new
  files before publishing references to them, and retain WAL ownership until
  replacement recovery state is durable.
- Treat atomic batches as indivisible and preserve sequence and snapshot
  visibility rules.
- Add recovery and corruption tests for changes to WAL, manifest, SSTable, or
  filesystem behavior.
- Document any persistent-format change and its compatibility consequences.
  The project is pre-alpha, but format changes must still be deliberate and
  testable.

## Tests and documentation

Behavior changes require tests. Put focused module tests beside private
implementation code and cross-component behavior in
[`crates/meteordb/tests`](crates/meteordb/tests). Use fault-injecting filesystem
implementations for ordering and failure-path tests, and property tests for
state spaces that are difficult to cover with examples.

Add Rustdoc to every new or changed public API. Examples and documentation must
describe behavior implemented on `main`; do not present roadmap work as
available. See [Documentation checks](docs/development.md#documentation-checks)
for the commands to run.

## Commit and pull request guidance

Write concise, imperative commits that explain one logical change. Before
requesting review:

- complete the pull request template;
- link the relevant issue when one exists;
- describe user-visible and persistent-format effects;
- include benchmark methodology and results for performance claims; and
- call out durability, recovery, corruption, concurrency, or compatibility
  risks explicitly.

Do not include credentials, private database contents, or other secrets in
issues, logs, tests, commits, or pull requests.

## Review criteria

Reviewers evaluate whether a change:

- solves the stated problem without unrelated scope;
- preserves storage-engine invariants and failure behavior;
- has targeted regression, corruption, recovery, or concurrency coverage where
  applicable;
- keeps public APIs documented and product claims accurate;
- makes persistent-format and durability ordering changes explicit;
- supports performance claims with reproducible evidence; and
- passes formatting, Clippy, tests, documentation checks, and
  `git diff --check`.
