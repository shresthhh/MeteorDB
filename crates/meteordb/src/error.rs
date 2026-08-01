use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{operation} failed for {}: {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("corruption in {context}: {detail}")]
    Corruption {
        context: &'static str,
        detail: String,
    },
    #[error("unsupported {kind} format version {version}")]
    UnsupportedFormat { kind: &'static str, version: u32 },
    #[error("database is locked: {}", .0.display())]
    Locked(PathBuf),
    #[error("database is closed")]
    Closed,
    #[error("background worker failed: {0}")]
    Background(String),
    #[error("write stalled with {immutable_memtables} immutable memtables")]
    WriteStall { immutable_memtables: usize },
}

pub type Result<T> = std::result::Result<T, Error>;
