use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
    DurableFs, Error, MemTable, Options, OsDurableFs, Result, SequenceNumber, SnapshotGuard,
    SnapshotRegistry, ValueRecord, WalWriter, WriteBatch, WriteOp,
};

const WAL_FILE_NAME: &str = "000001.wal";

/// A cloneable handle to MeteorDB's WAL-backed in-memory engine.
///
/// Clones share one inner engine. Writes are serialized by a mutex while reads
/// use an atomically published committed sequence to choose a consistent MVCC
/// view. Task 4 intentionally has no SSTables, flush, or compaction.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    options: Options,
    write_state: Mutex<WriteState>,
    committed_sequence: AtomicU64,
    snapshots: SnapshotRegistry,
}

struct WriteState {
    wal: WalWriter,
    next_sequence: SequenceNumber,
    memtable: MemTable,
    closed: bool,
    write_failure: Option<String>,
}

impl Engine {
    /// Opens a new engine using the operating system's durable filesystem.
    ///
    /// This Task 4 implementation creates `000001.wal` inside
    /// [`Options::path`] and starts with an empty memtable. WAL replay and
    /// multi-segment recovery belong to a later task.
    pub fn open(options: Options) -> Result<Self> {
        Self::open_with_fs(options, Arc::new(OsDurableFs))
    }

    /// Opens an engine with an injectable durable-filesystem implementation.
    ///
    /// Supplying a trait object lets tests fail a physical WAL write or sync
    /// deterministically. Production callers normally use [`Engine::open`].
    pub fn open_with_fs(options: Options, fs: Arc<dyn DurableFs>) -> Result<Self> {
        options.validate()?;
        std::fs::create_dir_all(&options.path).map_err(|source| Error::Io {
            operation: "create database directory",
            path: options.path.clone(),
            source,
        })?;
        let wal_path = options.path.join(WAL_FILE_NAME);
        let wal = WalWriter::create_with_fs(&wal_path, options.max_batch_bytes, fs)?;
        Ok(Self {
            inner: Arc::new(EngineInner {
                options,
                write_state: Mutex::new(WriteState {
                    wal,
                    next_sequence: 1,
                    memtable: MemTable::default(),
                    closed: false,
                    write_failure: None,
                }),
                committed_sequence: AtomicU64::new(0),
                snapshots: SnapshotRegistry::default(),
            }),
        })
    }

    /// Stores `value` under `key` as one atomic write batch.
    ///
    /// Key and value bytes are copied into an owned [`WriteBatch`] before the
    /// batch enters the serialized write coordinator.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.write(batch)
    }

    /// Writes a tombstone for `key`.
    ///
    /// A tombstone hides older values from current reads while preserving them
    /// for snapshots that were taken before the deletion.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete(key);
        self.write(batch)
    }

    /// Commits every operation in `batch` at one sequence number.
    ///
    /// One [`Mutex`] serializes validation, sequence assignment, WAL append,
    /// memtable application, and lifecycle state. This makes concurrent
    /// writers take an unambiguous order. The memtable changes only after the
    /// complete WAL batch succeeds, so a WAL error publishes no operation.
    ///
    /// Publication uses a release store after all records are installed.
    /// Readers use an acquire load, which establishes that a reader observing
    /// the new sequence also observes the preceding memtable writes.
    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        let mut state = self.lock_state();
        ensure_open(&state)?;
        if let Some(message) = &state.write_failure {
            return Err(Error::Background(message.clone()));
        }
        validate_batch(&self.inner.options, &batch)?;

        let sequence = state.next_sequence;
        if sequence == u64::MAX {
            return Err(Error::InvalidArgument(
                "sequence number space is exhausted".to_owned(),
            ));
        }
        if let Err(error) = state
            .wal
            .append(sequence, &batch, self.inner.options.durability)
        {
            state.write_failure = Some(format!(
                "the WAL append failed; further writes are disabled: {error}"
            ));
            return Err(error);
        }

        state.memtable.apply(sequence, batch)?;
        state.next_sequence = sequence + 1;
        self.inner
            .committed_sequence
            .store(sequence, Ordering::Release);
        Ok(())
    }

    /// Returns the current value for `key`, or `None` for absence or deletion.
    ///
    /// The acquire load captures the latest fully published sequence. The
    /// mutex then provides safe borrowed access to the memtable while the
    /// selected value is copied into an owned `Vec<u8>` for the caller.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let sequence = self.inner.committed_sequence.load(Ordering::Acquire);
        self.get_at(key.as_ref(), sequence)
    }

    /// Captures a stable read view at the current committed sequence.
    ///
    /// Later writes receive larger sequence numbers, so [`Snapshot::get`]
    /// skips them and continues to see the version that was current here.
    pub fn snapshot(&self) -> Result<Snapshot> {
        let sequence = self.inner.committed_sequence.load(Ordering::Acquire);
        {
            let state = self.lock_state();
            ensure_open(&state)?;
        }
        Ok(Snapshot {
            engine: self.clone(),
            sequence,
            _guard: self.inner.snapshots.acquire(sequence),
        })
    }

    /// Synchronizes all buffered WAL writes to stable storage.
    ///
    /// Synchronous writes already perform this step before returning;
    /// [`crate::Durability::Buffered`] callers can use this method as an explicit
    /// durability barrier.
    pub fn sync(&self) -> Result<()> {
        let state = self.lock_state();
        ensure_open(&state)?;
        if let Some(message) = &state.write_failure {
            return Err(Error::Background(message.clone()));
        }
        state.wal.sync()
    }

    /// Closes the shared engine handle.
    ///
    /// The first call synchronizes the WAL before marking the engine closed.
    /// Later calls return success without repeating work. Every other engine
    /// or snapshot operation returns [`Error::Closed`] after publication.
    pub fn close(&self) -> Result<()> {
        let mut state = self.lock_state();
        if state.closed {
            return Ok(());
        }
        state.wal.sync()?;
        state.closed = true;
        Ok(())
    }

    fn get_at(&self, key: &[u8], sequence: SequenceNumber) -> Result<Option<Vec<u8>>> {
        if key.len() > self.inner.options.max_key_bytes {
            return Err(Error::InvalidArgument(format!(
                "key length {} exceeds max_key_bytes {}",
                key.len(),
                self.inner.options.max_key_bytes
            )));
        }
        let state = self.lock_state();
        ensure_open(&state)?;
        Ok(match state.memtable.get(key, sequence)? {
            Some(ValueRecord::Value { value, .. }) => Some(value.clone()),
            Some(ValueRecord::Tombstone) | None => None,
        })
    }

    fn lock_state(&self) -> MutexGuard<'_, WriteState> {
        self.inner
            .write_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A fixed-sequence read view owned by an [`Engine`].
///
/// The snapshot owns an engine clone and a [`SnapshotGuard`]. Ownership keeps
/// the shared state alive, while the guard uses RAII to unregister the
/// sequence automatically when this value is dropped.
pub struct Snapshot {
    engine: Engine,
    sequence: SequenceNumber,
    _guard: SnapshotGuard,
}

impl Snapshot {
    /// Returns the sequence number captured by this snapshot.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Reads `key` at the snapshot's fixed sequence number.
    ///
    /// Versions committed after the snapshot are ignored. A visible tombstone
    /// returns `None` rather than exposing an older value.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.engine.get_at(key.as_ref(), self.sequence)
    }
}

fn ensure_open(state: &WriteState) -> Result<()> {
    if state.closed {
        Err(Error::Closed)
    } else {
        Ok(())
    }
}

fn validate_batch(options: &Options, batch: &WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Err(Error::InvalidArgument(
            "cannot write an empty batch".to_owned(),
        ));
    }
    if batch.approximate_bytes() > options.max_batch_bytes {
        return Err(Error::InvalidArgument(format!(
            "write batch payload {} exceeds max_batch_bytes {}",
            batch.approximate_bytes(),
            options.max_batch_bytes
        )));
    }
    for operation in batch.operations() {
        let (key, value) = match operation {
            WriteOp::Put { key, value, .. } => (key.as_slice(), Some(value.as_slice())),
            WriteOp::Delete { key } => (key.as_slice(), None),
        };
        validate_length("key", key.len(), "max_key_bytes", options.max_key_bytes)?;
        if let Some(value) = value {
            validate_length(
                "value",
                value.len(),
                "max_value_bytes",
                options.max_value_bytes,
            )?;
        }
    }
    Ok(())
}

fn validate_length(
    kind: &'static str,
    length: usize,
    limit_name: &'static str,
    limit: usize,
) -> Result<()> {
    if length > limit {
        return Err(Error::InvalidArgument(format!(
            "{kind} length {length} exceeds {limit_name} {limit}"
        )));
    }
    Ok(())
}
