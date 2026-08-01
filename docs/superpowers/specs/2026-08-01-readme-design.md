# MeteorDB README Design

**Date:** 2026-08-01
**Status:** Approved

## Purpose

The root README helps a developer clone, build, test, and understand MeteorDB
without implying that roadmap features are already complete.

## Audience

The primary audience is a Rust developer or contributor encountering the
project for the first time. The README assumes basic command-line familiarity
but does not assume storage-engine knowledge.

## Structure

1. Project definition and an explicit implementation-status notice.
2. Prerequisites and clone/build/test commands.
3. A compilable example using only the current public API.
4. An explanation of the currently implemented write and read behavior.
5. A feature-status table separating implemented capabilities from roadmap work.
6. The AI workload goals and RocksDB comparison methodology, without performance claims.
7. Links to the full design and implementation plan.
8. Contribution and validation commands.

## Accuracy Rules

- Describe only code present on the feature branch as implemented.
- Mark SSTables, Bloom filters, compaction, scans, and AI adapters as planned.
- Do not publish performance claims before reproducible benchmarks exist.
- Do not add CI, coverage, release, or benchmark badges before those systems exist.
- Keep commands executable from the repository root.
- Keep the API example synchronized with the public crate exports.

## Current Feature Claims

The README may describe these capabilities as implemented:

- Rust workspace and typed public configuration, errors, clocks, and write batches.
- Order-preserving MVCC internal keys and automatically released snapshots.
- Checksummed, fragmented WAL records with synchronous and buffered durability.
- A serialized WAL-backed in-memory engine with atomic batch publication.

Everything else in the main storage-engine design remains roadmap work until
its feature commit passes review.

## Validation

- Run the README's build and test commands.
- Compile or test the documented Rust example.
- Check every feature claim against the current public API and committed code.
- Run `git diff --check`.
