use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

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

    /// Reads an existing regular file without following symlinks.
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
        let mut options = OpenOptions::new();
        options.read(true);
        open_regular(path, &mut options).map(drop)
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

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        File::open(path)?.sync_all()
    }

    fn atomic_replace(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        std::fs::rename(source, destination)
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
