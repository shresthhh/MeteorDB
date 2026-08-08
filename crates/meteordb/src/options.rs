use std::path::{Path, PathBuf};

use crate::{Error, Result};

/// Controls when a successful write is considered durable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    /// Synchronize writes to stable storage before reporting success.
    ///
    /// This is the safer default, trading write latency for stronger crash
    /// durability.
    #[default]
    Sync,
    /// Allow writes to remain in operating-system buffers after success.
    ///
    /// This can reduce latency, but a power loss may discard recently
    /// acknowledged writes.
    Buffered,
}

/// Selects how table blocks are compressed on disk.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Compression {
    /// Store blocks without compression.
    ///
    /// This avoids compression CPU cost and is the initial conservative
    /// default.
    #[default]
    None,
    /// Compress blocks with Snappy.
    ///
    /// Snappy favors fast compression and decompression over maximum size
    /// reduction.
    Snappy,
}

/// Configuration used when opening a MeteorDB database.
///
/// [`Options::new`] supplies conservative, nonzero defaults. Fields remain
/// public so applications can tune individual resource and input limits before
/// calling [`Options::validate`].
#[derive(Clone, Debug)]
pub struct Options {
    /// Directory containing the database's persistent files.
    pub path: PathBuf,
    /// Durability guarantee applied to writes by default.
    pub durability: Durability,
    /// Approximate bytes accepted by the active in-memory table before rotation.
    pub memtable_bytes: usize,
    /// Approximate target size for newly generated sorted-table files.
    ///
    /// This is a target rather than a hard limit because preserving record
    /// boundaries can produce a slightly larger table.
    pub target_sstable_bytes: usize,
    /// Approximate uncompressed data bytes grouped into one table block.
    pub block_bytes: usize,
    /// Number of keys between full-key restart points in prefix-compressed blocks.
    ///
    /// Smaller values improve seek work at the cost of additional stored key
    /// bytes.
    pub restart_interval: usize,
    /// Bloom-filter bits allocated per key.
    ///
    /// More bits reduce false positives but consume more memory and disk space.
    pub bloom_bits_per_key: u8,
    /// Maximum bytes reserved for cached table blocks.
    pub block_cache_bytes: usize,
    /// Maximum immutable memory tables tolerated before writes must stall.
    pub max_immutable_memtables: usize,
    /// Maximum accepted key length in bytes.
    pub max_key_bytes: usize,
    /// Maximum accepted value length in bytes.
    pub max_value_bytes: usize,
    /// Maximum combined key and value payload bytes in one write batch.
    pub max_batch_bytes: usize,
}

impl Options {
    /// Creates options for a database stored at `path`.
    ///
    /// The path is copied into an owned [`PathBuf`], so the returned options do
    /// not borrow the caller's input.
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

    /// Checks that every size and count limit is nonzero.
    ///
    /// The first invalid field is returned as [`Error::InvalidArgument`] with
    /// its exact field name, making configuration mistakes actionable.
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

    /// Returns a trusted ceiling for metadata emitted by configured engine writers.
    ///
    /// Flushes may pass the memtable target by one complete batch. Compaction
    /// outputs may pass their target by one indivisible user-key version group.
    /// This bound expands those bytes for worst-case internal keys and engine
    /// values, then covers next-fit block fragmentation, Bloom bits, index
    /// entries/restarts/handles, properties, and stored-block trailers.
    pub(crate) fn sstable_metadata_bytes_limit(&self) -> usize {
        const INTERNAL_KEY_FIXED_BYTES: usize = 11;
        const ENGINE_VALUE_MAX_OVERHEAD_BYTES: usize = 14;
        const ENGINE_VALUE_MIN_BYTES: usize = 6;
        const INDEX_ENTRY_VARINT_BYTES: usize = 30;
        const BLOCK_HANDLE_MAX_BYTES: usize = 20;
        const RESTART_BYTES: usize = 4;
        const PROPERTIES_FIXED_BYTES: usize = 68;

        let batch_entries = self.max_batch_bytes.saturating_add(1);
        let oversized_group_bytes =
            self.max_batch_bytes
                .saturating_mul(2)
                .saturating_add(batch_entries.saturating_mul(
                    INTERNAL_KEY_FIXED_BYTES.saturating_add(ENGINE_VALUE_MAX_OVERHEAD_BYTES),
                ));

        let prior_memtable_entries = ceil_div(self.memtable_bytes, INTERNAL_KEY_FIXED_BYTES).max(1);
        let flushed_entry_bytes = self
            .memtable_bytes
            .saturating_add(prior_memtable_entries.saturating_mul(ENGINE_VALUE_MIN_BYTES))
            .saturating_add(oversized_group_bytes);
        let compacted_entry_bytes = self
            .target_sstable_bytes
            .saturating_add(oversized_group_bytes);
        let table_entry_bytes = flushed_entry_bytes.max(compacted_entry_bytes);

        let max_entries = (table_entry_bytes
            / INTERNAL_KEY_FIXED_BYTES.saturating_add(ENGINE_VALUE_MIN_BYTES))
        .max(1);
        let fragmentation_bound =
            ceil_div(table_entry_bytes.saturating_mul(2), self.block_bytes).saturating_add(1);
        let max_blocks = max_entries.min(fragmentation_bound).max(1);

        let filter_payload_bytes = ceil_div(
            max_entries.saturating_mul(usize::from(self.bloom_bits_per_key)),
            8,
        )
        .max(8)
        .saturating_add(1);
        let filter_stored_bytes = filter_payload_bytes.saturating_add(crate::BLOCK_TRAILER_BYTES);

        let index_restart_count = ceil_div(max_blocks, self.restart_interval).max(1);
        let index_stored_bytes =
            table_entry_bytes
                .saturating_add(max_blocks.saturating_mul(
                    INDEX_ENTRY_VARINT_BYTES.saturating_add(BLOCK_HANDLE_MAX_BYTES),
                ))
                .saturating_add(index_restart_count.saturating_mul(RESTART_BYTES))
                .saturating_add(RESTART_BYTES)
                .saturating_add(crate::BLOCK_TRAILER_BYTES);

        let max_internal_key_bytes = self
            .max_key_bytes
            .saturating_mul(2)
            .saturating_add(INTERNAL_KEY_FIXED_BYTES);
        let properties_stored_bytes =
            PROPERTIES_FIXED_BYTES.saturating_add(max_internal_key_bytes.saturating_mul(2));

        index_stored_bytes
            .saturating_add(filter_stored_bytes)
            .saturating_add(properties_stored_bytes)
    }
}

fn ceil_div(value: usize, divisor: usize) -> usize {
    value
        .checked_add(divisor.saturating_sub(1))
        .map_or(usize::MAX, |adjusted| adjusted / divisor)
}

fn validate_nonzero(field: &'static str, value: usize) -> Result<()> {
    if value == 0 {
        return Err(Error::InvalidArgument(format!(
            "{field} must be greater than zero"
        )));
    }
    Ok(())
}
