use std::path::PathBuf;

/// Failures that MeteorDB can return through its public API.
///
/// Variants carry structured context so applications can react without parsing
/// display messages. Display text is intended for people and may evolve.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A caller supplied an invalid option or request.
    ///
    /// The message names the rejected argument and explains its constraint.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// An operating-system I/O operation failed.
    #[error("{operation} failed for {}: {source}", path.display())]
    Io {
        /// A short description of the attempted operation, such as `"open"`.
        operation: &'static str,
        /// The file or directory involved in the failed operation.
        path: PathBuf,
        /// The original operating-system error, preserved for inspection.
        #[source]
        source: std::io::Error,
    },
    /// Stored data failed an integrity or structural check.
    #[error("corruption in {context}: {detail}")]
    Corruption {
        /// The subsystem or structure in which corruption was detected.
        context: &'static str,
        /// A diagnostic description of the violated invariant.
        detail: String,
    },
    /// Persistent data uses a format version this build cannot read.
    #[error("unsupported {kind} format version {version}")]
    UnsupportedFormat {
        /// The kind of structure carrying the unsupported version.
        kind: &'static str,
        /// The version number found in persistent data.
        version: u32,
    },
    /// Another process or database handle owns the lock at the given path.
    #[error("database is locked: {}", .0.display())]
    Locked(PathBuf),
    /// An operation was attempted after the database handle was closed.
    #[error("database is closed")]
    Closed,
    /// A background worker failed and could not continue safely.
    ///
    /// The contained message preserves the worker's diagnostic context.
    #[error("background worker failed: {0}")]
    Background(String),
    /// Writes cannot proceed until immutable in-memory tables are flushed.
    #[error("write stalled with {immutable_memtables} immutable memtables")]
    WriteStall {
        /// The number of immutable memory tables present when writing stalled.
        immutable_memtables: usize,
    },
}

/// MeteorDB's standard result type.
///
/// Using one alias keeps public signatures concise while preserving the
/// structured [`Error`] value for callers.
pub type Result<T> = std::result::Result<T, Error>;
