# Release checklist

## Scope and metadata

- [ ] Capability table, roadmap, Rustdoc, examples, changelog, and crate version
      agree with the exact code being released.
- [ ] `license`, `repository`, `readme`, categories, keywords, and MSRV are
      correct; `cargo package -p meteordb --allow-dirty` contains no internal
      artifacts or generated benchmark results.
- [ ] Persistent-format changes and migration limitations are called out.

## Required gates

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo test --workspace --doc
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test -p meteordb --examples
cargo run -p meteordb --example quickstart
cargo run -p meteordb --example ai_adapters
cargo bench -p meteordb --bench engine -- --test
scripts/check-doc-links.sh
cargo package -p meteordb --allow-dirty
git diff --check
```

- [ ] Smoke the inspector against a freshly flushed database.
- [ ] Run the deterministic MeteorDB comparison smoke.
- [ ] Run RocksDB comparison when its documented native dependencies exist;
      otherwise record the dependency check's actionable failure.
- [ ] Run scheduled fuzz jobs and any feasible clean-container/MSRV job.

## Publication

- [ ] Review package contents, generated docs, licenses, and security policy.
- [ ] Tag only the tested commit, publish from a clean checkout, verify docs.rs,
      then add release notes with known limitations and reproducibility details.
