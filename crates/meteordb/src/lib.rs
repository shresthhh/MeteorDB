//! Public contracts and MVCC building blocks for MeteorDB.
//!
//! The crate exposes configuration, write batches, internal-key ordering, and
//! snapshot lifetime tracking. Modules remain private so applications can use a
//! stable crate-root API without depending on the source-file layout.

#![deny(missing_docs)]

mod batch;
mod clock;
mod error;
mod fs;
mod internal_key;
mod options;
mod snapshot;
mod wal;

pub use batch::{WriteBatch, WriteOp};
pub use clock::{Clock, SystemClock};
pub use error::{Error, Result};
pub use fs::{DurableFile, DurableFs, OsDurableFs};
pub use internal_key::{InternalKey, SequenceNumber, ValueKind};
pub use options::{Compression, Durability, Options};
pub use snapshot::{SnapshotGuard, SnapshotRegistry};
pub use wal::{RecoveredBatch, WalWriter, replay_wal};
