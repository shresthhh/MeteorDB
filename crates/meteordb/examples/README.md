# Examples

Run from the repository root:

| Example | Command | Demonstrates |
| --- | --- | --- |
| [`quickstart.rs`](quickstart.rs) | `cargo run -p meteordb --example quickstart` | atomic batch, point read, snapshot, prefix scan, TTL |
| [`ai_adapters.rs`](ai_adapters.rs) | `cargo run -p meteordb --example ai_adapters` | inference TTL cache, feature lookup, embedding batch retrieval |

Both examples use uniquely owned temporary directories and deterministic data.
