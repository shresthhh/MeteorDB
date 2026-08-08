//! Public contracts and MVCC building blocks for MeteorDB.
//!
//! The crate exposes configuration, write batches, internal-key ordering, and
//! snapshot lifetime tracking. Modules remain private so applications can use a
//! stable crate-root API without depending on the source-file layout.

#![deny(missing_docs)]

mod background;
mod batch;
mod bloom;
mod clock;
mod engine;
mod error;
mod fs;
mod internal_key;
mod manifest;
mod memtable;
mod options;
mod snapshot;
mod sstable;
mod version;
mod wal;

pub use batch::{WriteBatch, WriteOp};
pub use bloom::BloomFilter;
pub use clock::{Clock, SystemClock};
pub use engine::{Engine, Snapshot};
pub use error::{Error, Result};
pub use fs::{DurableFile, DurableFs, OsDurableFs};
pub use internal_key::{InternalKey, SequenceNumber, ValueKind};
pub use manifest::VersionSet;
pub use memtable::{MemTable, ValueRecord};
pub use options::{Compression, Durability, Options};
pub use snapshot::{SnapshotGuard, SnapshotRegistry};
pub use sstable::{
    BLOCK_TRAILER_BYTES, Block, BlockBuilder, BlockHandle, BlockIter,
    DEFAULT_MAX_UNCOMPRESSED_DATA_BLOCK_BYTES, NO_COMPRESSION, SNAPPY_COMPRESSION,
    SSTABLE_FOOTER_BYTES, SSTABLE_FORMAT_VERSION, SSTABLE_MAGIC, TableBuildResult, TableBuilder,
    TableIter, TableProperties, TableReader, TableReaderOptions, decode_stored_block,
    encode_stored_block,
};
pub use version::{FileMeta, NUM_LEVELS, Version, VersionEdit};
pub use wal::{RecoveredBatch, WalWriter, replay_wal, replay_wal_with_fs};
