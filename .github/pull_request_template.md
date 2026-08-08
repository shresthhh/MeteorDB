## Summary

<!-- Explain the problem, the chosen approach, and the observable result. -->

## Correctness and compatibility

<!-- Describe effects on recovery, corruption handling, concurrency, snapshots, persistent formats, and compatibility. Write "Not applicable" where appropriate. -->

## Validation

- [ ] I added or updated focused tests for behavior changes.
- [ ] I ran `cargo fmt --check`.
- [ ] I ran `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] I ran `cargo test --workspace`.
- [ ] I ran `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- [ ] I ran `git diff --check`.
- [ ] I added or updated Rustdoc and product documentation for public behavior.
- [ ] I documented persistent-format compatibility, or this change does not affect persistent formats.
- [ ] I verified filesystem durability ordering, or this change does not perform filesystem mutations.
- [ ] I included reproducible benchmark evidence for performance claims, or this pull request makes no performance claim.

## Test evidence

<!-- List focused commands and relevant results. Include fault-injection, corruption, recovery, property, or concurrency coverage when applicable. -->

## Related issue

<!-- Link an issue with "Closes #..." when applicable. -->
