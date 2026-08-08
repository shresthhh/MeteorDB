# SSTable implementation

This directory owns MeteorDB's immutable sorted-table format.

| File | Responsibility |
| --- | --- |
| `block.rs` | Builds, validates, seeks, and iterates prefix-compressed key/value blocks with restart points. |
| `builder.rs` | Groups sorted internal keys into data blocks, builds Bloom/index/properties metadata, writes the footer, and synchronizes the completed temporary file. |
| `reader.rs` | Validates footer and metadata eagerly, reads data blocks lazily, applies resource limits, and integrates cache and read statistics. |
| `format.rs` | Defines block handles, checksummed stored-block framing, compression markers, canonical varints, format version, magic, and footer size. |
| `mod.rs` | Connects the implementation files and re-exports the SSTable surface. |

## Canonical physical layout

```text
[data block 0 + trailer]
[data block 1 + trailer]
...
[Bloom filter block + trailer]
[index block + trailer]
[properties block + trailer]
[72-byte footer]
```

Every stored block ends with a one-byte compression marker and masked CRC32C.
Data blocks contain sorted prefix-compressed internal keys, values, restart
offsets, and a restart count. The Bloom filter covers user keys. The index maps
separator keys to data-block handles, and properties record file identity,
counts, compression, maximum data-block size, and key bounds.

The footer contains three 20-byte padded handle slots in index, filter, and
properties order, followed by a little-endian format version and `METEOR01`
magic. Readers reject non-canonical handles, gaps or reordered metadata,
overlap, out-of-range blocks, invalid checksums, unsupported versions, and
inconsistent properties.

`TableBuilder::finish` synchronizes the temporary file. Installation,
directory synchronization, manifest publication, and temporary-file cleanup
belong to the engine and manifest paths described in the
[architecture reference](../../../../docs/architecture.md#flush-and-recovery).
