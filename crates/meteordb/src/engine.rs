use std::collections::{BTreeSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread;

use crate::background::BackgroundSignal;
use crate::{
    DurableFs, Error, FileMeta, InternalKey, MemTable, Options, OsDurableFs, Result,
    SequenceNumber, SnapshotGuard, SnapshotRegistry, TableBuildResult, TableBuilder, TableReader,
    ValueKind, ValueRecord, VersionEdit, VersionSet, WalWriter, WriteBatch, WriteOp,
    replay_wal_with_fs,
};

/// A cloneable handle to MeteorDB's durable engine.
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    options: Options,
    fs: Arc<dyn DurableFs>,
    write_state: Mutex<WriteState>,
    background: Arc<BackgroundSignal>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    handle_count: AtomicUsize,
    shutdown: AtomicBool,
    committed_sequence: AtomicU64,
    snapshots: SnapshotRegistry,
}

struct WriteState {
    versions: VersionSet,
    wal: WalWriter,
    mutable: MutableMemTable,
    immutables: VecDeque<Arc<ImmutableMemTable>>,
    wal_numbers: BTreeSet<u64>,
    next_file_number: u64,
    next_sequence: SequenceNumber,
    flush_running: bool,
    closed: bool,
    terminal_failure: Option<TerminalFailure>,
    background_failure: bool,
}

struct MutableMemTable {
    table: MemTable,
    wal_number: u64,
}

struct ImmutableMemTable {
    table: MemTable,
    wal_number: u64,
    largest_sequence: SequenceNumber,
}

struct TerminalFailure {
    operation: Option<&'static str>,
    path: Option<PathBuf>,
    source_kind: Option<io::ErrorKind>,
    message: String,
}

impl TerminalFailure {
    fn from_error(error: Error) -> Self {
        match error {
            Error::Io {
                operation,
                path,
                source,
            } => Self {
                operation: Some(operation),
                path: Some(path),
                source_kind: Some(source.kind()),
                message: source.to_string(),
            },
            error => Self {
                operation: None,
                path: None,
                source_kind: None,
                message: error.to_string(),
            },
        }
    }

    fn to_error(&self) -> Error {
        match (self.operation, &self.path, self.source_kind) {
            (Some(operation), Some(path), Some(kind)) => Error::Io {
                operation,
                path: path.clone(),
                source: io::Error::new(kind, self.message.clone()),
            },
            _ => Error::Background(self.message.clone()),
        }
    }
}

impl Engine {
    /// Opens or recovers an engine using the operating system's durable filesystem.
    pub fn open(options: Options) -> Result<Self> {
        Self::open_with_fs(options, Arc::new(OsDurableFs))
    }

    /// Opens or recovers an engine with injectable crash-sensitive filesystem operations.
    pub fn open_with_fs(options: Options, fs: Arc<dyn DurableFs>) -> Result<Self> {
        options.validate()?;
        std::fs::create_dir_all(&options.path).map_err(|source| Error::Io {
            operation: "create database directory",
            path: options.path.clone(),
            source,
        })?;

        let current_path = options.path.join("CURRENT");
        let mut versions = if fs
            .entry_exists(&current_path)
            .map_err(|source| io_error("check CURRENT", &current_path, source))?
        {
            VersionSet::recover_with_fs(&options.path, fs.clone())?
        } else {
            VersionSet::create_with_fs(&options.path, fs.clone())?
        };
        remove_unpublished_sstables(&options.path, &versions, fs.as_ref())?;

        let wal_paths = wal_paths(&options.path)?;
        let mut wal_numbers = BTreeSet::new();
        let mut recovered = Vec::new();
        let mut largest_sequence = versions.last_sequence();
        let mut largest_file_number = versions.next_file_number().saturating_sub(1);
        for (number, path) in &wal_paths {
            largest_file_number = largest_file_number.max(*number);
            wal_numbers.insert(*number);
            let mut table = MemTable::default();
            let mut table_largest = 0;
            for record in replay_wal_with_fs(path, options.max_batch_bytes, fs.clone())? {
                if record.sequence <= versions.last_sequence() {
                    continue;
                }
                if record.sequence <= largest_sequence {
                    return Err(Error::Corruption {
                        context: "WAL recovery",
                        detail: format!(
                            "sequence {} does not follow recovered sequence {largest_sequence}",
                            record.sequence
                        ),
                    });
                }
                largest_sequence = record.sequence;
                table_largest = record.sequence;
                table.apply(record.sequence, record.batch)?;
            }
            recovered.push((*number, table, table_largest));
        }

        let mut next_file_number = largest_file_number
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("file number space is exhausted".into()))?;
        let mut immutables = VecDeque::new();
        for (number, table, table_largest) in recovered {
            if !table.is_empty() {
                immutables.push_back(Arc::new(ImmutableMemTable {
                    table,
                    wal_number: number,
                    largest_sequence: table_largest,
                }));
            }
        }
        let number = allocate_file_number(&mut next_file_number)?;
        let path = options.path.join(wal_name(number));
        let wal = WalWriter::create_with_fs(&path, options.max_batch_bytes, fs.clone())?;
        wal_numbers.insert(number);
        let mutable = MutableMemTable {
            table: MemTable::default(),
            wal_number: number,
        };

        let mut counter_edit = VersionEdit::new();
        counter_edit.set_next_file_number(next_file_number);
        counter_edit.set_last_sequence(versions.last_sequence());
        versions.apply(counter_edit)?;

        let engine = Self {
            inner: Arc::new(EngineInner {
                options,
                fs,
                write_state: Mutex::new(WriteState {
                    versions,
                    wal,
                    mutable,
                    immutables,
                    wal_numbers,
                    next_file_number,
                    next_sequence: largest_sequence.checked_add(1).ok_or_else(|| {
                        Error::InvalidArgument("sequence number space is exhausted".into())
                    })?,
                    flush_running: false,
                    closed: false,
                    terminal_failure: None,
                    background_failure: false,
                }),
                background: Arc::new(BackgroundSignal::default()),
                worker: Mutex::new(None),
                handle_count: AtomicUsize::new(1),
                shutdown: AtomicBool::new(false),
                committed_sequence: AtomicU64::new(largest_sequence),
                snapshots: SnapshotRegistry::default(),
            }),
        };
        engine.start_background_worker()?;
        if !engine.lock_state().immutables.is_empty() {
            engine.inner.background.wake_all();
        }
        Ok(engine)
    }

    /// Stores `value` under `key` as one atomic write batch.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.write(batch)
    }

    /// Writes a tombstone for `key`.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete(key);
        self.write(batch)
    }

    /// Commits every operation in `batch` at one sequence number.
    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        let mut state = self.lock_state();
        ensure_writable(&state)?;
        validate_batch(&self.inner.options, &batch)?;
        if state.immutables.len() >= self.inner.options.max_immutable_memtables {
            return Err(Error::WriteStall {
                immutable_memtables: state.immutables.len(),
            });
        }

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
            return Err(record_terminal_failure(&mut state, error));
        }
        state.mutable.table.apply(sequence, batch)?;
        state.next_sequence = sequence + 1;
        self.inner
            .committed_sequence
            .store(sequence, Ordering::Release);

        if state.mutable.table.approximate_bytes() >= self.inner.options.memtable_bytes {
            if let Err(error) = rotate_memtable(&self.inner, &mut state, sequence) {
                record_terminal_failure(&mut state, error);
                return Ok(());
            }
            self.inner.background.wake_all();
        }
        Ok(())
    }

    /// Returns the current value for `key`, or `None` for absence or deletion.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let sequence = self.inner.committed_sequence.load(Ordering::Acquire);
        self.get_at(key.as_ref(), sequence)
    }

    /// Captures a stable read view at the current committed sequence.
    pub fn snapshot(&self) -> Result<Snapshot> {
        let sequence = self.inner.committed_sequence.load(Ordering::Acquire);
        {
            let state = self.lock_state();
            ensure_readable(&state)?;
        }
        Ok(Snapshot {
            engine: self.clone(),
            sequence,
            _guard: self.inner.snapshots.acquire(sequence),
        })
    }

    /// Synchronizes all buffered writes in the active WAL.
    pub fn sync(&self) -> Result<()> {
        let mut state = self.lock_state();
        ensure_writable(&state)?;
        if let Err(error) = state.wal.sync() {
            return Err(record_terminal_failure(&mut state, error));
        }
        Ok(())
    }

    /// Rotates the current memtable and waits for every scheduled flush.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.lock_state();
        ensure_writable(&state)?;
        while !state.mutable.table.is_empty()
            && state.immutables.len() >= self.inner.options.max_immutable_memtables
        {
            state = self.inner.background.wait(state);
            ensure_writable(&state)?;
        }
        if !state.mutable.table.is_empty() {
            let largest = state.next_sequence.saturating_sub(1);
            if let Err(error) = rotate_memtable(&self.inner, &mut state, largest) {
                return Err(record_terminal_failure(&mut state, error));
            }
            self.inner.background.wake_all();
        }
        while !state.immutables.is_empty() || state.flush_running {
            state = self.inner.background.wait(state);
            ensure_writable(&state)?;
        }
        Ok(())
    }

    /// Synchronizes the active WAL and closes this shared engine.
    pub fn close(&self) -> Result<()> {
        let mut state = self.lock_state();
        if let Some(failure) = &state.terminal_failure {
            let error = failure.to_error();
            state.closed = true;
            self.inner.background.wake_all();
            return Err(error);
        }
        if state.closed {
            return Ok(());
        }
        if let Err(error) = state.wal.sync() {
            let error = record_terminal_failure(&mut state, error);
            state.closed = true;
            self.inner.background.wake_all();
            return Err(error);
        }
        state.closed = true;
        self.inner.background.wake_all();
        Ok(())
    }

    fn start_background_worker(&self) -> Result<()> {
        let weak = Arc::downgrade(&self.inner);
        let signal = self.inner.background.clone();
        let handle = thread::Builder::new()
            .name("meteordb-flush".into())
            .spawn(move || background_loop(weak, signal))
            .map_err(|source| Error::Io {
                operation: "start background flush worker",
                path: self.inner.options.path.clone(),
                source,
            })?;
        *self
            .inner
            .worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle);
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
        ensure_readable(&state)?;
        let mut best = state
            .mutable
            .table
            .get_entry(key, sequence)?
            .map(clone_candidate);
        for immutable in state.immutables.iter().rev() {
            merge_candidate(
                &mut best,
                immutable
                    .table
                    .get_entry(key, sequence)?
                    .map(clone_candidate),
            );
        }

        let version = state.versions.current();
        for level in 0..crate::NUM_LEVELS {
            for file in version.files(level) {
                let reader = TableReader::open(
                    self.inner
                        .options
                        .path
                        .join(format!("{:06}.sst", file.number())),
                )?;
                for entry in reader.iter() {
                    let (internal_key, value) = entry?;
                    if internal_key.user_key() == key && internal_key.sequence() <= sequence {
                        let record = if internal_key.kind() == ValueKind::Deletion {
                            ValueRecord::Tombstone
                        } else {
                            ValueRecord::value(value, None)
                        };
                        merge_candidate(&mut best, Some((internal_key.sequence(), record)));
                        break;
                    }
                }
            }
        }
        Ok(match best {
            Some((_, ValueRecord::Value { value, .. })) => Some(value),
            Some((_, ValueRecord::Tombstone)) | None => None,
        })
    }

    fn lock_state(&self) -> MutexGuard<'_, WriteState> {
        self.inner
            .write_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Clone for Engine {
    fn clone(&self) -> Self {
        self.inner.handle_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.inner.handle_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.background.wake_all();
        if let Some(worker) = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = worker.join();
        }
    }
}

/// A fixed-sequence read view owned by an [`Engine`].
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
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.engine.get_at(key.as_ref(), self.sequence)
    }
}

fn background_loop(weak: Weak<EngineInner>, signal: Arc<BackgroundSignal>) {
    let mut generation = signal.generation();
    loop {
        let Some(probe) = weak.upgrade() else {
            return;
        };
        if probe.shutdown.load(Ordering::Acquire) {
            return;
        }
        drop(probe);
        let Some(inner) = weak.upgrade() else {
            return;
        };
        let (immutable, file_number) = {
            let mut state = inner
                .write_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.closed || state.terminal_failure.is_some() {
                return;
            }
            let Some(immutable) = state.immutables.front().cloned() else {
                drop(state);
                drop(inner);
                generation = signal.wait_for_work(generation);
                continue;
            };
            state.flush_running = true;
            let file_number = match allocate_file_number(&mut state.next_file_number) {
                Ok(number) => number,
                Err(error) => {
                    record_background_failure(&mut state, error);
                    state.flush_running = false;
                    inner.background.wake_all();
                    return;
                }
            };
            (immutable, file_number)
        };

        let built = build_sstable(&inner, file_number, &immutable.table);
        let mut state = inner
            .write_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let result = built.and_then(|built| {
            let mut edit = VersionEdit::new();
            edit.add_file(
                0,
                FileMeta::new(
                    built.file_number,
                    built.file_size,
                    built.smallest,
                    built.largest,
                )?,
            );
            edit.set_next_file_number(state.next_file_number);
            edit.set_last_sequence(immutable.largest_sequence);
            state.versions.apply(edit)?;
            state.immutables.pop_front();
            retire_obsolete_wals(&inner, &mut state)
        });
        state.flush_running = false;
        if let Err(error) = result {
            record_background_failure(&mut state, error);
            inner.background.wake_all();
            return;
        }
        inner.background.wake_all();
    }
}

fn build_sstable(
    inner: &EngineInner,
    file_number: u64,
    table: &MemTable,
) -> Result<TableBuildResult> {
    let temporary = inner.options.path.join(format!("{file_number:06}.sst.tmp"));
    let final_path = inner.options.path.join(format!("{file_number:06}.sst"));
    let mut builder = TableBuilder::create_with_fs(
        &temporary,
        file_number,
        inner.options.block_bytes,
        inner.options.restart_interval,
        inner.options.bloom_bits_per_key,
        crate::Compression::None,
        inner.fs.clone(),
    )?;
    for (key, record) in table.iter() {
        let value = match record {
            ValueRecord::Value { value, .. } => value.as_slice(),
            ValueRecord::Tombstone => &[],
        };
        builder.add(key, value)?;
    }
    let built = builder.finish()?;
    inner
        .fs
        .atomic_install(&temporary, &final_path)
        .map_err(|source| io_error("install SSTable", &final_path, source))?;
    inner
        .fs
        .sync_directory(&inner.options.path)
        .map_err(|source| io_error("sync SSTable directory", &inner.options.path, source))?;
    Ok(built)
}

fn rotate_memtable(
    inner: &EngineInner,
    state: &mut WriteState,
    largest_sequence: SequenceNumber,
) -> Result<()> {
    let new_number = allocate_file_number(&mut state.next_file_number)?;
    let new_path = inner.options.path.join(wal_name(new_number));
    let new_wal =
        WalWriter::create_with_fs(&new_path, inner.options.max_batch_bytes, inner.fs.clone())?;
    let old = std::mem::replace(
        &mut state.mutable,
        MutableMemTable {
            table: MemTable::default(),
            wal_number: new_number,
        },
    );
    state.wal = new_wal;
    state.wal_numbers.insert(new_number);
    state.immutables.push_back(Arc::new(ImmutableMemTable {
        table: old.table,
        wal_number: old.wal_number,
        largest_sequence,
    }));
    Ok(())
}

fn retire_obsolete_wals(inner: &EngineInner, state: &mut WriteState) -> Result<()> {
    let oldest_required = state
        .immutables
        .iter()
        .map(|table| table.wal_number)
        .chain(std::iter::once(state.mutable.wal_number))
        .min()
        .expect("the mutable memtable always owns a WAL");
    let obsolete: Vec<_> = state
        .wal_numbers
        .iter()
        .copied()
        .take_while(|number| *number < oldest_required)
        .collect();
    for number in &obsolete {
        let path = inner.options.path.join(wal_name(*number));
        inner
            .fs
            .remove_file(&path)
            .map_err(|source| io_error("remove obsolete WAL", &path, source))?;
        state.wal_numbers.remove(number);
    }
    if !obsolete.is_empty() {
        inner
            .fs
            .sync_directory(&inner.options.path)
            .map_err(|source| io_error("sync WAL retirement", &inner.options.path, source))?;
    }
    Ok(())
}

fn remove_unpublished_sstables(
    directory: &Path,
    versions: &VersionSet,
    fs: &dyn DurableFs,
) -> Result<()> {
    let current = versions.current();
    let live: BTreeSet<_> = (0..crate::NUM_LEVELS)
        .flat_map(|level| current.files(level).iter().map(FileMeta::number))
        .collect();
    let mut removed = false;
    for entry in std::fs::read_dir(directory)
        .map_err(|source| io_error("list database", directory, source))?
    {
        let entry = entry.map_err(|source| io_error("read database entry", directory, source))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let temporary = parse_numbered_name(&name, ".sst.tmp").is_some();
        let unpublished =
            parse_numbered_name(&name, ".sst").is_some_and(|number| !live.contains(&number));
        if temporary || unpublished {
            fs.remove_file(&entry.path())
                .map_err(|source| io_error("remove unpublished SSTable", &entry.path(), source))?;
            removed = true;
        }
    }
    if removed {
        fs.sync_directory(directory)
            .map_err(|source| io_error("sync SSTable cleanup", directory, source))?;
    }
    Ok(())
}

fn wal_paths(directory: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(directory)
        .map_err(|source| io_error("list database", directory, source))?
    {
        let entry = entry.map_err(|source| io_error("read database entry", directory, source))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(number) = parse_numbered_name(&name, ".wal") {
            paths.push((number, entry.path()));
        }
    }
    paths.sort_by_key(|(number, _)| *number);
    Ok(paths)
}

fn parse_numbered_name(name: &str, suffix: &str) -> Option<u64> {
    let digits = name.strip_suffix(suffix)?;
    (digits.len() == 6 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

fn wal_name(number: u64) -> String {
    format!("{number:06}.wal")
}

fn allocate_file_number(next: &mut u64) -> Result<u64> {
    let number = *next;
    if number == 0 {
        return Err(Error::InvalidArgument(
            "file number must be greater than zero".into(),
        ));
    }
    *next = number
        .checked_add(1)
        .ok_or_else(|| Error::InvalidArgument("file number space is exhausted".into()))?;
    Ok(number)
}

fn clone_candidate(entry: (&InternalKey, &ValueRecord)) -> (SequenceNumber, ValueRecord) {
    (entry.0.sequence(), entry.1.clone())
}

fn merge_candidate(
    current: &mut Option<(SequenceNumber, ValueRecord)>,
    candidate: Option<(SequenceNumber, ValueRecord)>,
) {
    if let Some(candidate) = candidate
        && current
            .as_ref()
            .is_none_or(|(sequence, _)| candidate.0 > *sequence)
    {
        *current = Some(candidate);
    }
}

fn ensure_open(state: &WriteState) -> Result<()> {
    if state.closed {
        Err(Error::Closed)
    } else {
        Ok(())
    }
}

fn ensure_writable(state: &WriteState) -> Result<()> {
    if let Some(failure) = &state.terminal_failure {
        Err(failure.to_error())
    } else {
        ensure_open(state)
    }
}

fn ensure_readable(state: &WriteState) -> Result<()> {
    if state.background_failure {
        Err(state
            .terminal_failure
            .as_ref()
            .expect("a background failure stores its diagnostic")
            .to_error())
    } else {
        ensure_open(state)
    }
}

fn record_terminal_failure(state: &mut WriteState, error: Error) -> Error {
    let failure = TerminalFailure::from_error(error);
    let returned = failure.to_error();
    state.terminal_failure = Some(failure);
    returned
}

fn record_background_failure(state: &mut WriteState, error: Error) -> Error {
    let returned = record_terminal_failure(state, error);
    state.background_failure = true;
    returned
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

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> Error {
    Error::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}
