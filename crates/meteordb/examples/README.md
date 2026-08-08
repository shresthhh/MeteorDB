# Examples

Run examples from the repository root.

| Example | Command | Demonstrates |
| --- | --- | --- |
| [`quickstart.rs`](quickstart.rs) | `cargo run -p meteordb --example quickstart` | Opening an engine, committing an atomic batch, snapshot isolation, a current read, and clean close |

Examples must use only public APIs, produce deterministic results, and return
errors instead of hiding failures. Use an isolated temporary database
directory or remove owned files on every exit path; never write into a
developer's existing database. Keep output short and avoid timing,
randomness, network access, or platform-specific assumptions unless those are
the point of the example.

See the [crate guide](../README.md) for current guarantees and limitations.
