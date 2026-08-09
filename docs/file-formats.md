# Persistent file formats

MeteorDB's current application-facing adapter schemas use version `1`.
The storage formats below are pre-alpha implementation formats and may change
without migration support. They are not RocksDB-compatible.

| Surface | Identifier/version | Notes |
| --- | --- | --- |
| SSTable | magic `METEOR01`, footer version `2` | readers accept v1 and v2 |
| WAL logical batch | version `1` | 32 KiB physical blocks, fragmented records |
| Write batch | version `1` | put/delete operations, optional absolute TTL |
| Manifest edit | version `2` | readers accept v1 and v2 |
| Engine value envelope | magic `MEV`, version `1` | value/tombstone expiry metadata |
| Inspector JSON | `format_version: 1` | output contract, not storage format |
| Benchmark JSON | schema version `1` | deterministic comparison contract |
| Inference/feature/embedding keys and values | schema version `1` | private namespace-prefixed encodings |

## WAL and manifest framing

WAL and manifest streams use 32 KiB physical blocks. Each fragment has a
seven-byte header: masked CRC32C (`u32` little-endian), payload length (`u16`
little-endian), and fragment type (`FULL`, `FIRST`, `MIDDLE`, or `LAST`).
Checksums cover the type and payload.

A WAL logical record contains version, sequence, operation count, and checked
length-delimited operations. A manifest logical record contains its version
and a serialized `VersionEdit`. Canonical lengths, operation counts, sequence
continuity, and maximum payloads are checked before allocation/publication.

## SSTables

SSTables contain prefix-compressed data blocks with restart points, an index,
a user-key Bloom filter, properties, five-byte checksummed block trailers, and
a fixed 72-byte footer. The footer stores three padded block-handle slots,
format version, and eight-byte magic. Version 2's Bloom filter hashes user keys
rather than internal MVCC keys. Codec `0` is uncompressed and codec `1` is
Snappy.

The low-level SSTable builder/reader support codec `1`, but `Engine` currently
writes uncompressed tables and has no runtime compression option. Benchmark
comparison profiles therefore reject Snappy for MeteorDB.

## Compatibility policy

Version numbers make rejection explicit; they do not promise future migration.
Unknown versions return `Error::UnsupportedFormat`. Before upgrading, retain a
recoverable copy and validate it with the old binary. Cross-version migration,
RocksDB ingestion/export, and compatibility with LevelDB/RocksDB MANIFEST, WAL,
or SST files are not implemented.

The low-level details are documented in Rustdoc beside the encoder/decoder and
tested by corruption, recovery, property, and fuzz suites. Treat those tests
and constants as the source of truth for this pre-alpha release.
