use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

/// An open durable file whose writes and synchronization can be injected.
///
/// WAL code uses this interface instead of calling [`File::write_all`] or
/// [`File::sync_all`] directly, so tests can deterministically observe and
/// fail the physical operations that define append durability.
pub trait DurableFile: Send {
    /// Writes all bytes or returns the first physical write error.
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()>;

    /// Requests that all file contents and metadata reach stable storage.
    fn sync_all(&self) -> std::io::Result<()>;
}

/// An open regular file used for checked, no-follow reads.
///
/// Metadata and contents are obtained from this same handle so callers cannot
/// validate one directory entry and then read a swapped replacement.
pub trait DurableReadFile: Read + Seek + Send {
    /// Returns the length of the file represented by this open handle.
    fn len(&self) -> std::io::Result<u64>;

    /// Reports whether this open file is empty.
    fn is_empty(&self) -> std::io::Result<bool> {
        self.len().map(|length| length == 0)
    }
}

struct OsDurableFile(File);
struct OsDurableReadFile(File);
struct OwnedDurableReadFile(Cursor<Vec<u8>>);

impl DurableFile for OsDurableFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        Write::write_all(&mut self.0, bytes)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.0.sync_all()
    }
}

impl Read for OsDurableReadFile {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buffer)
    }
}

impl Seek for OsDurableReadFile {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        self.0.seek(position)
    }
}

impl DurableReadFile for OsDurableReadFile {
    fn len(&self) -> std::io::Result<u64> {
        self.0.metadata().map(|metadata| metadata.len())
    }
}

impl Read for OwnedDurableReadFile {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buffer)
    }
}

impl Seek for OwnedDurableReadFile {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        self.0.seek(position)
    }
}

impl DurableReadFile for OwnedDurableReadFile {
    fn len(&self) -> std::io::Result<u64> {
        u64::try_from(self.0.get_ref().len())
            .map_err(|_| std::io::Error::other("read buffer length exceeds u64"))
    }
}

/// Filesystem operations whose crash behavior matters to persistent metadata.
///
/// Keeping these operations behind a trait lets recovery tests substitute a
/// filesystem that fails at a chosen append, synchronization, or rename. A
/// successful file synchronization is not sufficient to persist a new name:
/// [`DurableFs::sync_directory`] separately persists directory-entry changes
/// such as a newly created or renamed file.
pub trait DurableFs: Send + Sync {
    /// Reports whether any directory entry exists at `path` without following symlinks.
    fn entry_exists(&self, path: &Path) -> std::io::Result<bool> {
        match std::fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Exclusively creates `path` and opens it for writing.
    ///
    /// This fails with [`std::io::ErrorKind::AlreadyExists`] when `path`
    /// already exists, preventing accidental truncation and check-then-create
    /// races.
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>>;

    /// Opens `path` for appending, creating it when it does not exist.
    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>>;

    /// Opens an existing regular file for appending without following symlinks.
    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let mut options = OpenOptions::new();
        options.append(true);
        Ok(Box::new(OsDurableFile(open_regular(path, &mut options)?)))
    }

    /// Opens an existing regular file for random-access reading.
    ///
    /// The compatibility default owns the bytes returned by [`Self::read_file`],
    /// so implementations that intercept only that method continue to affect
    /// manifest, WAL, and SSTable readers.
    fn open_read(&self, path: &Path) -> std::io::Result<Box<dyn DurableReadFile>> {
        Ok(Box::new(OwnedDurableReadFile(Cursor::new(
            self.read_file(path)?,
        ))))
    }

    /// Reads an existing regular file without following symlinks.
    ///
    /// This default performs the OS read directly rather than calling
    /// [`Self::open_read`], avoiding recursion with its compatibility adapter.
    fn read_file(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        let mut options = OpenOptions::new();
        options.read(true);
        let mut file = open_regular(path, &mut options)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    /// Validates that `path` names an existing regular file without following symlinks.
    fn validate_file(&self, path: &Path) -> std::io::Result<()> {
        self.open_read(path).map(drop)
    }

    /// Opens and synchronizes an existing immutable file.
    ///
    /// Manifest publication uses this operation before recording an SSTable,
    /// establishing that the referenced file exists and its bytes are durable.
    fn sync_file(&self, path: &Path) -> std::io::Result<()> {
        let mut options = OpenOptions::new();
        options.read(true);
        open_regular(path, &mut options)?.sync_all()
    }

    /// Shortens an existing file to `length` bytes.
    ///
    /// Manifest recovery uses this to discard a structurally torn final
    /// record before opening the log for later appends.
    fn truncate_file(&self, path: &Path, length: u64) -> std::io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true);
        open_regular(path, &mut options)?.set_len(length)
    }

    /// Requests that changes to entries in `path` reach stable storage.
    ///
    /// Syncing a file persists its bytes, but a crash can still lose a recent
    /// create or rename unless the containing directory is also synchronized.
    fn sync_directory(&self, path: &Path) -> std::io::Result<()>;

    /// Atomically replaces `destination` with `source` on one filesystem.
    ///
    /// Readers see either the old destination or the complete replacement,
    /// never a partially copied file. Call [`DurableFs::sync_directory`]
    /// afterward when the rename itself must survive a crash.
    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()>;

    /// Atomically installs `source` at an absent `destination` without replacement.
    ///
    /// The destination link is created exclusively, so any existing directory
    /// entry, including a dangling symlink, makes the operation fail.
    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        std::fs::hard_link(source, destination)?;
        std::fs::remove_file(source)
    }

    /// Removes one directory entry without following it.
    ///
    /// Callers synchronize the containing directory after a removal that must
    /// survive a crash.
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        std::fs::remove_file(path)
    }
}

/// The durable-filesystem implementation backed by Rust's standard library.
#[derive(Clone, Copy, Debug, Default)]
pub struct OsDurableFs;

impl DurableFs for OsDurableFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(OsDurableFile(
            OpenOptions::new().create_new(true).write(true).open(path)?,
        )))
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(OsDurableFile(
            OpenOptions::new().create(true).append(true).open(path)?,
        )))
    }

    fn open_read(&self, path: &Path) -> std::io::Result<Box<dyn DurableReadFile>> {
        let mut options = OpenOptions::new();
        options.read(true);
        Ok(Box::new(OsDurableReadFile(open_regular(
            path,
            &mut options,
        )?)))
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        File::open(path)?.sync_all()
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        std::fs::rename(source, destination)
    }
}

/// A crash-sensitive filesystem operation observed by [`FaultyFs`].
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FaultOperation {
    /// Bytes are written to a WAL, manifest, or temporary SSTable.
    Write,
    /// An open file is synchronized.
    FileSync,
    /// An existing immutable file is synchronized by path.
    SyncFile,
    /// A source path atomically replaces a destination path.
    AtomicReplace,
    /// A temporary immutable file is atomically installed.
    AtomicInstall,
    /// A directory is synchronized.
    DirectorySync,
    /// A persistent file is shortened.
    Truncate,
    /// An obsolete file is removed.
    Remove,
}

/// One deterministic operation recorded by [`FaultyFs`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultEvent {
    /// One-based position in the complete operation stream.
    pub index: usize,
    /// Kind of durable operation.
    pub operation: FaultOperation,
    /// Primary path affected by the operation.
    pub path: PathBuf,
}

#[derive(Default)]
struct FaultState {
    next_index: AtomicUsize,
    events: Mutex<Vec<FaultEvent>>,
    fail_at: Option<usize>,
    crash_image: Mutex<CrashImage>,
}

#[derive(Default)]
struct CrashImage {
    volatile: HashMap<PathBuf, Vec<u8>>,
    synced: HashMap<PathBuf, Vec<u8>>,
    durable: HashMap<PathBuf, Vec<u8>>,
    tracked: HashSet<PathBuf>,
}

impl FaultState {
    fn observe(&self, operation: FaultOperation, path: &Path) -> std::io::Result<()> {
        let index = self.next_index.fetch_add(1, Ordering::SeqCst) + 1;
        self.events.lock().unwrap().push(FaultEvent {
            index,
            operation,
            path: path.to_path_buf(),
        });
        if self.fail_at == Some(index) {
            self.restore_durable_state()?;
            return Err(std::io::Error::other(format!(
                "injected crash at operation {index}: {operation:?} {}",
                path.display()
            )));
        }
        Ok(())
    }

    fn track_file(&self, path: &Path) -> std::io::Result<()> {
        let bytes = std::fs::read(path)?;
        let mut image = self.crash_image.lock().unwrap();
        image.tracked.insert(path.to_path_buf());
        image.volatile.insert(path.to_path_buf(), bytes);
        Ok(())
    }

    fn record_write(&self, path: &Path, bytes: &[u8]) {
        let mut image = self.crash_image.lock().unwrap();
        image.tracked.insert(path.to_path_buf());
        image
            .volatile
            .entry(path.to_path_buf())
            .or_default()
            .extend_from_slice(bytes);
    }

    fn record_file_sync(&self, path: &Path) {
        let mut image = self.crash_image.lock().unwrap();
        if let Some(bytes) = image.volatile.get(path).cloned() {
            image.synced.insert(path.to_path_buf(), bytes.clone());
            if image.durable.contains_key(path) {
                image.durable.insert(path.to_path_buf(), bytes);
            }
        }
    }

    fn record_rename(&self, source: &Path, destination: &Path) {
        let mut image = self.crash_image.lock().unwrap();
        image.tracked.insert(source.to_path_buf());
        image.tracked.insert(destination.to_path_buf());
        if let Some(bytes) = image.volatile.remove(source) {
            image.volatile.insert(destination.to_path_buf(), bytes);
        }
        if let Some(bytes) = image.synced.remove(source) {
            image.synced.insert(destination.to_path_buf(), bytes);
        }
    }

    fn record_directory_sync(&self, directory: &Path) {
        let mut image = self.crash_image.lock().unwrap();
        let paths = image
            .tracked
            .iter()
            .filter(|path| path.parent() == Some(directory))
            .cloned()
            .collect::<Vec<_>>();
        for path in paths {
            if image.volatile.contains_key(&path) {
                let bytes = image
                    .synced
                    .get(&path)
                    .or_else(|| image.durable.get(&path))
                    .cloned()
                    .unwrap_or_default();
                image.durable.insert(path, bytes);
            } else {
                image.durable.remove(&path);
            }
        }
    }

    fn record_truncate(&self, path: &Path) -> std::io::Result<()> {
        let bytes = std::fs::read(path)?;
        let mut image = self.crash_image.lock().unwrap();
        image.tracked.insert(path.to_path_buf());
        image.volatile.insert(path.to_path_buf(), bytes);
        Ok(())
    }

    fn record_remove(&self, path: &Path) {
        let mut image = self.crash_image.lock().unwrap();
        image.tracked.insert(path.to_path_buf());
        image.volatile.remove(path);
        image.synced.remove(path);
    }

    fn restore_durable_state(&self) -> std::io::Result<()> {
        let mut image = self.crash_image.lock().unwrap();
        let paths = image
            .tracked
            .iter()
            .chain(image.durable.keys())
            .cloned()
            .collect::<HashSet<_>>();
        for path in paths {
            if let Some(bytes) = image.durable.get(&path) {
                std::fs::write(&path, bytes)?;
            } else {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        image.volatile = image.durable.clone();
        image.synced = image.durable.clone();
        Ok(())
    }
}

/// An operating-system filesystem wrapper with a reproducible crash point.
///
/// Every mutation relevant to storage durability receives a one-based index.
/// [`FaultyFs::recording`] discovers the stable sequence for a workload, and
/// [`FaultyFs::fail_at`] replays that workload while returning an I/O error
/// immediately before the selected physical operation. Tests can then discard
/// the engine and reopen with a normal filesystem to model a process crash at
/// that boundary.
pub struct FaultyFs {
    inner: OsDurableFs,
    state: Arc<FaultState>,
}

impl FaultyFs {
    /// Creates a wrapper that records every crash-sensitive operation.
    pub fn recording() -> Self {
        Self {
            inner: OsDurableFs,
            state: Arc::new(FaultState::default()),
        }
    }

    /// Creates a wrapper that fails immediately before one one-based operation.
    pub fn fail_at(operation_index: usize) -> Self {
        Self {
            inner: OsDurableFs,
            state: Arc::new(FaultState {
                fail_at: Some(operation_index),
                ..FaultState::default()
            }),
        }
    }

    /// Returns a snapshot of all operations observed so far.
    pub fn events(&self) -> Vec<FaultEvent> {
        self.state.events.lock().unwrap().clone()
    }

    /// Simulates an immediate process crash by restoring only durable state.
    ///
    /// File contents survive only after a successful file synchronization.
    /// New names, renames, and removals survive only after a later successful
    /// synchronization of their containing directory.
    pub fn crash(&self) -> std::io::Result<()> {
        self.state.restore_durable_state()
    }

    fn wrap(&self, path: &Path, file: Box<dyn DurableFile>) -> Box<dyn DurableFile> {
        Box::new(FaultyFile {
            inner: file,
            path: path.to_path_buf(),
            state: self.state.clone(),
        })
    }
}

struct FaultyFile {
    inner: Box<dyn DurableFile>,
    path: PathBuf,
    state: Arc<FaultState>,
}

impl DurableFile for FaultyFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.state.observe(FaultOperation::Write, &self.path)?;
        self.inner.write_all(bytes)?;
        self.state.record_write(&self.path, bytes);
        Ok(())
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.state.observe(FaultOperation::FileSync, &self.path)?;
        self.inner.sync_all()?;
        self.state.record_file_sync(&self.path);
        Ok(())
    }
}

impl DurableFs for FaultyFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let file = self.inner.create(path)?;
        self.state.track_file(path)?;
        Ok(self.wrap(path, file))
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let file = self.inner.append(path)?;
        self.state.track_file(path)?;
        Ok(self.wrap(path, file))
    }

    fn append_existing(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        let file = self.inner.append_existing(path)?;
        self.state.track_file(path)?;
        Ok(self.wrap(path, file))
    }

    fn open_read(&self, path: &Path) -> std::io::Result<Box<dyn DurableReadFile>> {
        self.inner.open_read(path)
    }

    fn read_file(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        self.inner.read_file(path)
    }

    fn sync_file(&self, path: &Path) -> std::io::Result<()> {
        self.state.observe(FaultOperation::SyncFile, path)?;
        self.inner.sync_file(path)?;
        self.state.record_truncate(path)?;
        self.state.record_file_sync(path);
        Ok(())
    }

    fn truncate_file(&self, path: &Path, length: u64) -> std::io::Result<()> {
        self.state.observe(FaultOperation::Truncate, path)?;
        self.inner.truncate_file(path, length)?;
        self.state.record_truncate(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.state.observe(FaultOperation::DirectorySync, path)?;
        self.inner.sync_directory(path)?;
        self.state.record_directory_sync(path);
        Ok(())
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.state
            .observe(FaultOperation::AtomicReplace, destination)?;
        self.inner.atomic_replace(source, destination)?;
        self.state.record_rename(source, destination);
        Ok(())
    }

    fn atomic_install(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.state
            .observe(FaultOperation::AtomicInstall, destination)?;
        self.inner.atomic_install(source, destination)?;
        self.state.record_rename(source, destination);
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.state.observe(FaultOperation::Remove, path)?;
        self.inner.remove_file(path)?;
        self.state.record_remove(path);
        Ok(())
    }
}

pub(crate) fn open_lock_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    open_regular(path, &mut options)
}

fn open_regular(path: &Path, options: &mut OpenOptions) -> std::io::Result<File> {
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);

    #[cfg(windows)]
    options.custom_flags(0x0020_0000);

    #[cfg(not(any(unix, windows)))]
    {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "symlinks are not accepted for database files",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }

    let file = options.open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "database path is not a regular file",
        ));
    }
    Ok(file)
}
