//! Embedded ordered key/value storage with MVCC, TTL, and workload adapters.
//!
//! MeteorDB's current public contract and adapter schemas are version 1.
//! Storage files remain pre-alpha: the SSTable and manifest writer versions are
//! documented separately and can change without a migration path.
//!
//! # Open and put/get
//!
//! ```
//! use meteordb::{Engine, Options, Result};
//!
//! fn run(path: &std::path::Path) -> Result<()> {
//!     let db = Engine::open(Options::new(path))?;
//!     db.put("key", "value")?;
//!     assert_eq!(db.get("key")?.as_deref(), Some(&b"value"[..]));
//!     db.close()
//! }
//! let directory = tempfile::tempdir()?;
//! run(directory.path())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Atomic batch
//!
//! ```
//! use meteordb::{Engine, Options, Result, WriteBatch};
//!
//! fn run(path: &std::path::Path) -> Result<()> {
//!     let db = Engine::open(Options::new(path))?;
//!     let mut batch = WriteBatch::default();
//!     batch.put("a", "1").put("b", "2").delete("obsolete");
//!     db.write(batch)?;
//!     assert_eq!(db.get("b")?.as_deref(), Some(&b"2"[..]));
//!     db.close()
//! }
//! let directory = tempfile::tempdir()?;
//! run(directory.path())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Snapshot and prefix scan
//!
//! ```
//! use meteordb::{Engine, Options, Result};
//!
//! fn run(path: &std::path::Path) -> Result<()> {
//!     let db = Engine::open(Options::new(path))?;
//!     db.put("user:1", "old")?;
//!     db.put("user:2", "two")?;
//!     let snapshot = db.snapshot()?;
//!     db.put("user:1", "new")?;
//!     assert_eq!(snapshot.get("user:1")?.as_deref(), Some(&b"old"[..]));
//!     let rows = snapshot
//!         .scan_prefix("user:", 10)?
//!         .collect::<Result<Vec<_>>>()?;
//!     assert_eq!(rows.len(), 2);
//!     drop(snapshot);
//!     db.close()
//! }
//! let directory = tempfile::tempdir()?;
//! run(directory.path())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # TTL inference cache
//!
//! ```
//! use std::sync::Arc;
//! use meteordb::{
//!     CacheLookup, Engine, InferenceCache, InferenceEntry, InferenceKey,
//!     ManualClock, Options, Result,
//! };
//!
//! fn run(path: &std::path::Path) -> Result<()> {
//!     let clock = ManualClock::new(100);
//!     let db = Engine::open_with_clock(Options::new(path), Arc::new(clock.clone()))?;
//!     let cache = InferenceCache::new(db.clone(), "docs")?;
//!     let key = InferenceKey::new("model", "v1", b"prompt");
//!     cache.put(&key, InferenceEntry::new("answer"), Some(50))?;
//!     assert!(matches!(cache.get(&key)?, CacheLookup::Hit(_)));
//!     clock.set(150)?;
//!     assert_eq!(cache.get(&key)?, CacheLookup::Miss);
//!     db.close()
//! }
//! let directory = tempfile::tempdir()?;
//! run(directory.path())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Feature lookup
//!
//! ```
//! use meteordb::{
//!     Engine, FeatureKey, FeatureRecord, FeatureStore, FeatureValue, Options, Result,
//! };
//!
//! fn run(path: &std::path::Path) -> Result<()> {
//!     let db = Engine::open(Options::new(path))?;
//!     let store = FeatureStore::new(db.clone(), "online")?;
//!     let key = FeatureKey::new("user", "42", "ranking", 1_000);
//!     let mut row = FeatureRecord::new();
//!     row.insert("active", FeatureValue::Bool(true))?;
//!     store.put(&key, &row, None)?;
//!     assert_eq!(
//!         store.get(&key)?.and_then(|row| row.get("active").cloned()),
//!         Some(FeatureValue::Bool(true))
//!     );
//!     db.close()
//! }
//! let directory = tempfile::tempdir()?;
//! run(directory.path())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Embedding batch retrieval
//!
//! ```
//! use meteordb::{Embedding, EmbeddingKey, EmbeddingStore, Engine, Options, Result};
//!
//! fn run(path: &std::path::Path) -> Result<()> {
//!     let db = Engine::open(Options::new(path))?;
//!     let store = EmbeddingStore::new(db.clone(), "catalog")?;
//!     let a_key = EmbeddingKey::new("a");
//!     let b_key = EmbeddingKey::new("b");
//!     let a = Embedding::from_f32(&[1.0, 0.0])?;
//!     let b = Embedding::from_f32(&[0.0, 1.0])?;
//!     store.put_many([(&a_key, &a), (&b_key, &b)])?;
//!     let values = store.get_many([&b_key, &a_key])?;
//!     assert!(values.iter().all(Option::is_some));
//!     db.close()
//! }
//! let directory = tempfile::tempdir()?;
//! run(directory.path())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![deny(missing_docs)]

mod background;
mod batch;
mod bloom;
mod cache;
mod clock;
mod compaction;
mod engine;
mod error;
mod fs;
mod internal_key;
mod iter;
mod manifest;
mod memtable;
mod options;
mod snapshot;
mod sstable;
mod stats;
mod version;
mod wal;
mod workloads;

pub use batch::{WriteBatch, WriteOp};
pub use bloom::BloomFilter;
pub use cache::{BlockCache, BlockKind, CachePartition, CachePartitionSnapshot, CacheSnapshot};
pub use clock::{Clock, ManualClock, SystemClock};
pub use compaction::{
    CompactionJob, CompactionPicker, CompactionPlan, DEFAULT_L0_COMPACTION_TRIGGER,
};
pub use engine::{Engine, Snapshot};
pub use error::{Error, Result};
pub use fs::{
    DurableFile, DurableFs, DurableReadFile, FaultEvent, FaultOperation, FaultyFs, OsDurableFs,
};
pub use internal_key::{InternalKey, SequenceNumber, ValueKind};
pub use iter::{KvIterator, ScanBounds};
pub use manifest::{
    ManifestEditInspection, ManifestFileInspection, ManifestInspection, ManifestInspectionOptions,
    VersionSet, inspect_manifest, inspect_manifest_with_fs, inspect_manifest_with_options,
};
pub use memtable::{MemTable, ValueRecord};
pub use options::{Compression, Durability, Options};
pub use snapshot::{SnapshotGuard, SnapshotRegistry};
pub use sstable::{
    BLOCK_TRAILER_BYTES, Block, BlockBuilder, BlockHandle, BlockIter, DEFAULT_MAX_METADATA_BYTES,
    DEFAULT_MAX_UNCOMPRESSED_DATA_BLOCK_BYTES, NO_COMPRESSION, SNAPPY_COMPRESSION,
    SSTABLE_FOOTER_BYTES, SSTABLE_FORMAT_VERSION, SSTABLE_MAGIC, SstableBlockInspection,
    SstableEntryInspection, SstableInspection, TableBuildResult, TableBuilder, TableIter,
    TableProperties, TableReader, TableReaderOptions, decode_stored_block, encode_stored_block,
};
pub use stats::StatsSnapshot;
pub use version::{FileMeta, NUM_LEVELS, Version, VersionEdit};
pub use wal::{
    RecoveredBatch, WalInspection, WalWriter, inspect_wal, inspect_wal_with_fs, replay_wal,
    replay_wal_with_fs,
};
pub use workloads::{
    CacheLookup, Embedding, EmbeddingKey, EmbeddingStore, FeatureKey, FeatureRecord, FeatureStore,
    FeatureValue, InferenceCache, InferenceEntry, InferenceKey, ScalarType,
};
