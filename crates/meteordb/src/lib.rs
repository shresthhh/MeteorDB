//! Public contracts and MVCC building blocks for MeteorDB.
//!
//! The crate exposes configuration, write batches, internal-key ordering, and
//! snapshot lifetime tracking. Modules remain private so applications can use a
//! stable crate-root API without depending on the source-file layout.

#![deny(missing_docs)]

mod batch;
mod bloom;
mod clock;
mod engine;
mod error;
mod fs;
mod internal_key;
mod memtable;
mod options;
mod snapshot;
mod sstable;
mod wal;

pub use batch::{WriteBatch, WriteOp};
pub use bloom::BloomFilter;
pub use clock::{Clock, SystemClock};
pub use engine::{Engine, Snapshot};
pub use error::{Error, Result};
pub use fs::{DurableFile, DurableFs, OsDurableFs};
pub use internal_key::{InternalKey, SequenceNumber, ValueKind};
pub use memtable::{MemTable, ValueRecord};
pub use options::{Compression, Durability, Options};
pub use snapshot::{SnapshotGuard, SnapshotRegistry};
pub use sstable::{
    BLOCK_TRAILER_BYTES, Block, BlockBuilder, BlockHandle, BlockIter, NO_COMPRESSION,
    SNAPPY_COMPRESSION, SSTABLE_FORMAT_VERSION, SSTABLE_MAGIC, decode_stored_block,
    encode_stored_block,
};
pub use wal::{RecoveredBatch, WalWriter, replay_wal};
