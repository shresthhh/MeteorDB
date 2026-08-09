# Inspection and operations

## Read-only inspector

Build the CLI with `cargo build -p meteordb-cli`; the binary is `meteordb`.
Every command requires `--path`.

```bash
meteordb --path ./db check --format json
meteordb --path ./db dump-manifest --format human --max-edits 1000
meteordb --path ./db dump-sstable --file 000042.sst \
  --format json --max-entries 100 --max-blocks 100 \
  --max-bytes 1048576 --max-metadata-bytes 67108864
```

`check` validates `CURRENT`, every manifest edit, referenced SSTables, and
required WALs without recovery, truncation, or writer locking. Dump commands
still validate all input while bounding retained/rendered items. Canonical
relative SSTable names are required; traversal, symlinks, and non-regular files
are rejected by the no-follow inspection path.

For untrusted files, set `--max-batch-bytes`, `--max-files`, `--max-bytes`,
`--max-metadata-bytes`, and `--max-historical-files` to locally acceptable
limits. An exceeded limit is an error, never silent omission. Human and JSON
output indicate bounded sample truncation where supported.

## CLI benchmark smoke

```bash
meteordb --path ./bench-db bench --seconds 1 --seed 1 \
  --dataset-size 100 --workload inference-cache --durability sync --format json
```

This benchmark is a local diagnostic, not a performance claim. It modifies the
selected path. Use a fresh path and remove it only when you own it.

## Routine care

- Keep an independently restorable copy; no online backup API exists.
- Monitor `Engine::stats` for cache behavior, per-level table probes, Bloom
  usefulness, and read amplification.
- Call `flush` before controlled maintenance when SSTable publication is
  desired, then call `compact` repeatedly only while it returns `true`.
- Treat write stalls as backpressure and background/terminal errors as
  operator-visible failures.
- Validate a copied database with `check` before relying on it.
- Never edit `CURRENT`, manifests, WALs, or SSTables by hand.
