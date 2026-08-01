use std::fs::{File, OpenOptions};
use std::path::Path;

/// Filesystem operations whose crash behavior matters to persistent metadata.
///
/// Keeping these operations behind a trait lets recovery tests substitute a
/// filesystem that fails at a chosen append, synchronization, or rename. A
/// successful write is not necessarily durable: [`DurableFs::sync_file`]
/// asks the operating system to push a file to stable storage, while
/// [`DurableFs::sync_directory`] persists directory-entry changes such as a
/// newly created or renamed file.
pub trait DurableFs: Send + Sync {
    /// Creates or truncates `path` and opens it for writing.
    fn create(&self, path: &Path) -> std::io::Result<File>;

    /// Opens `path` for appending, creating it when it does not exist.
    fn append(&self, path: &Path) -> std::io::Result<File>;

    /// Requests that file contents and metadata reach stable storage.
    fn sync_file(&self, file: &File) -> std::io::Result<()>;

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
    fn create(&self, path: &Path) -> std::io::Result<File> {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
    }

    fn append(&self, path: &Path) -> std::io::Result<File> {
        OpenOptions::new().create(true).append(true).open(path)
    }

    fn sync_file(&self, file: &File) -> std::io::Result<()> {
        file.sync_all()
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        File::open(path)?.sync_all()
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        std::fs::rename(source, destination)
    }
}
