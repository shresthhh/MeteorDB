use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

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

struct OsDurableFile(File);

impl DurableFile for OsDurableFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        Write::write_all(&mut self.0, bytes)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.0.sync_all()
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
    /// Creates or truncates `path` and opens it for writing.
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>>;

    /// Opens `path` for appending, creating it when it does not exist.
    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>>;

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
}

/// The durable-filesystem implementation backed by Rust's standard library.
#[derive(Clone, Copy, Debug, Default)]
pub struct OsDurableFs;

impl DurableFs for OsDurableFs {
    fn create(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(OsDurableFile(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?,
        )))
    }

    fn append(&self, path: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Ok(Box::new(OsDurableFile(
            OpenOptions::new().create(true).append(true).open(path)?,
        )))
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        File::open(path)?.sync_all()
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        std::fs::rename(source, destination)
    }
}
