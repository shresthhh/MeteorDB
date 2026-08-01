use std::path::{Path, PathBuf};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    #[default]
    Sync,
    Buffered,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Compression {
    #[default]
    None,
    Snappy,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub path: PathBuf,
    pub durability: Durability,
    pub memtable_bytes: usize,
    pub target_sstable_bytes: usize,
    pub block_bytes: usize,
    pub restart_interval: usize,
    pub bloom_bits_per_key: u8,
    pub block_cache_bytes: usize,
    pub max_immutable_memtables: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_batch_bytes: usize,
}

impl Options {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            durability: Durability::default(),
            memtable_bytes: 64 * 1024 * 1024,
            target_sstable_bytes: 64 * 1024 * 1024,
            block_bytes: 4 * 1024,
            restart_interval: 16,
            bloom_bits_per_key: 10,
            block_cache_bytes: 64 * 1024 * 1024,
            max_immutable_memtables: 4,
            max_key_bytes: 1024 * 1024,
            max_value_bytes: 64 * 1024 * 1024,
            max_batch_bytes: 64 * 1024 * 1024,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_nonzero("memtable_bytes", self.memtable_bytes)?;
        validate_nonzero("target_sstable_bytes", self.target_sstable_bytes)?;
        validate_nonzero("block_bytes", self.block_bytes)?;
        validate_nonzero("restart_interval", self.restart_interval)?;
        validate_nonzero("bloom_bits_per_key", usize::from(self.bloom_bits_per_key))?;
        validate_nonzero("block_cache_bytes", self.block_cache_bytes)?;
        validate_nonzero("max_immutable_memtables", self.max_immutable_memtables)?;
        validate_nonzero("max_key_bytes", self.max_key_bytes)?;
        validate_nonzero("max_value_bytes", self.max_value_bytes)?;
        validate_nonzero("max_batch_bytes", self.max_batch_bytes)
    }
}

fn validate_nonzero(field: &'static str, value: usize) -> Result<()> {
    if value == 0 {
        return Err(Error::InvalidArgument(format!(
            "{field} must be greater than zero"
        )));
    }
    Ok(())
}
