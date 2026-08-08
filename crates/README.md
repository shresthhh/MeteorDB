# Workspace crates

The Cargo workspace currently contains one crate:

- [`meteordb`](meteordb) implements the embedded storage engine and exposes
  its Rust API.

Add a crate here only when it has an independently useful responsibility,
dependency boundary, and test surface. Keep storage-engine internals in
`meteordb`; workload adapters, command-line tools, or reusable test utilities
may become separate crates when they no longer belong in the core library.

Register new members in the workspace [`Cargo.toml`](../Cargo.toml) and keep
shared dependency versions in `[workspace.dependencies]`.
