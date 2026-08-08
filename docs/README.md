# MeteorDB documentation

Use this index to choose the shortest path to the information you need.

## Users

- Start with the [project README](../README.md) for capabilities and a runnable
  quickstart.
- Read [storage-engine concepts](storage-engine.md) for the behavior and
  trade-offs behind the API.
- Check the [roadmap](../ROADMAP.md) before depending on a planned capability.

## Evaluators

- Review the [architecture](architecture.md) for current data paths,
  concurrency, recovery, and correctness invariants.
- Run the [complete quickstart](../crates/meteordb/examples/quickstart.rs).
- Inspect the
  [cross-component tests](../crates/meteordb/tests) for executable durability
  and recovery scenarios.

## Contributors

- Use the validation commands in the [contributing section](../README.md#contributing).
- Keep storage changes within the module boundaries in the
  [architecture module map](architecture.md#module-map).
- Add integration coverage under
  [`crates/meteordb/tests`](../crates/meteordb/tests) when behavior crosses
  subsystem boundaries.

## Maintainers

- Treat the [correctness invariants](architecture.md#correctness-invariants) as
  release constraints.
- Keep the capability table and [roadmap](../ROADMAP.md) synchronized with
  code on `main`.
- Review persistent-format changes for recovery compatibility and explicit
  corruption handling.
