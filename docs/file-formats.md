# Persistent file formats

MeteorDB 1.x writes database format generation `1`. This generation is the
compatibility boundary for the complete persisted database: WAL and manifest
records, SSTables, engine value envelopes, and adapter keys and values.
Component encodings keep their own versions within generation 1; those numbers
do not need to equal the database generation. The formats are not
RocksDB-compatible.

| Surface | Identifier/version | Generation-1 read/write behavior | Unknown version |
| --- | --- | --- | --- |
| SSTable | magic `METEOR01`, component version `2` | readers accept component versions 1 and 2; writers emit 2 | `Error::UnsupportedFormat` |
| WAL logical batch/write batch | component version `1` | readers and writers use 1; 32 KiB physical blocks | `Error::UnsupportedFormat` |
| Manifest edit | component version `2` | readers accept component versions 1 and 2; writers emit 2 | `Error::UnsupportedFormat` |
| Engine value envelope | magic `MEV`, component version `1` | readers and writers use 1 | `Error::Corruption` with `SSTable engine value` context |
| Inference/feature/embedding keys and values | schema version `1` | readers and writers use the version-1 namespace and value schema | unknown key versions are separate namespaces; an unknown value version under the v1 namespace returns `Error::UnsupportedFormat` |
| Inspector JSON | `format_version: 1` | output contract, not storage format | consumers must reject unsupported output versions |
| Benchmark JSON | schema version `1` | deterministic comparison contract | parsers reject unsupported input versions |

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

The machine-readable constants `meteordb::PUBLIC_API_VERSION` and
`meteordb::DATABASE_FORMAT_VERSION` are both `1`. The database generation is a
label for the compatible set, not an additional field stored in each database.
Within database format generation 1, the current reader accepts exactly the
component versions in the table above and the current writer emits exactly the
listed writer versions. A newer release may call itself generation 1 only if
it continues to read these generation-1 encodings; otherwise it must increment
the database format generation.

There is no forward-compatibility promise: an older binary may reject a
component version written by a newer binary. There is no automatic migration
between database generations, and the generation constant does not itself
detect or migrate an unknown generation on disk. Unknown-version behavior is
the per-component behavior in the table; no decoder guesses or rewrites an
unknown encoding. Before upgrading, retain a recoverable copy so the database
can still be opened with the binary that wrote it. RocksDB ingestion/export
and compatibility with LevelDB/RocksDB MANIFEST, WAL, or SST files are not
implemented.

The low-level details are documented in Rustdoc beside the encoder/decoder and
tested by corruption, recovery, property, and fuzz suites. The component
constants and this generation table are the source of truth for release 1.x.
