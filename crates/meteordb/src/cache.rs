use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{Error, Result};

/// Selects one independently budgeted block-cache partition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CachePartition {
    /// Index and Bloom-filter blocks used to navigate SSTables.
    Metadata,
    /// Data blocks containing internal keys and values.
    Data,
}

/// Identifies the on-disk role of a cached SSTable block.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BlockKind {
    /// Bloom-filter metadata.
    Filter,
    /// Data-block index metadata.
    Index,
    /// Internal keys and values.
    Data,
}

impl BlockKind {
    fn partition(self) -> CachePartition {
        match self {
            Self::Filter | Self::Index => CachePartition::Metadata,
            Self::Data => CachePartition::Data,
        }
    }
}

/// A deterministic, independently partitioned least-recently-used block cache.
pub struct BlockCache {
    state: Mutex<CacheState>,
}

struct CacheState {
    metadata: PartitionState,
    data: PartitionState,
    next_stamp: u64,
}

struct PartitionState {
    capacity_bytes: usize,
    usage_bytes: usize,
    entries: HashMap<CacheKey, CacheEntry>,
    hits: u64,
    misses: u64,
    admissions: u64,
    evictions: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CacheKey {
    file_number: u64,
    block_offset: u64,
    kind: BlockKind,
}

struct CacheEntry {
    value: Arc<[u8]>,
    stamp: u64,
}

impl BlockCache {
    /// Creates a cache reserving 20% for metadata and 80% for data blocks.
    pub fn new(total_bytes: usize) -> Result<Self> {
        if total_bytes == 0 {
            return Err(Error::InvalidArgument(
                "block cache bytes must be greater than zero".to_owned(),
            ));
        }
        let metadata_bytes = total_bytes / 5;
        Ok(Self {
            state: Mutex::new(CacheState {
                metadata: PartitionState::new(metadata_bytes),
                data: PartitionState::new(total_bytes - metadata_bytes),
                next_stamp: 0,
            }),
        })
    }

    /// Returns the configured byte budget for one partition.
    pub fn capacity_bytes(&self, partition: CachePartition) -> usize {
        let state = self.lock();
        state.partition(partition).capacity_bytes
    }

    /// Returns the currently charged bytes in one partition.
    pub fn usage_bytes(&self, partition: CachePartition) -> usize {
        let state = self.lock();
        state.partition(partition).usage_bytes
    }

    /// Captures owned usage and activity counters for both partitions.
    pub fn snapshot(&self) -> CacheSnapshot {
        let state = self.lock();
        CacheSnapshot {
            metadata: state.metadata.snapshot(),
            data: state.data.snapshot(),
        }
    }

    /// Returns and refreshes an entry identified by file, offset, and kind.
    pub fn get(
        &self,
        file_number: u64,
        block_offset: u64,
        kind: BlockKind,
    ) -> Result<Option<Arc<[u8]>>> {
        let mut state = self.lock_result()?;
        let stamp = state.bump_stamp();
        let key = CacheKey {
            file_number,
            block_offset,
            kind,
        };
        let partition = state.partition_mut(kind.partition());
        let value = partition.entries.get_mut(&key).map(|entry| {
            entry.stamp = stamp;
            entry.value.clone()
        });
        if value.is_some() {
            partition.hits = partition.hits.saturating_add(1);
        } else {
            partition.misses = partition.misses.saturating_add(1);
        }
        Ok(value)
    }

    /// Admits a block and returns whether it fit in its independent partition.
    pub fn insert(
        &self,
        file_number: u64,
        block_offset: u64,
        kind: BlockKind,
        value: Arc<[u8]>,
    ) -> Result<bool> {
        let mut state = self.lock_result()?;
        let key = CacheKey {
            file_number,
            block_offset,
            kind,
        };
        let partition_kind = kind.partition();
        if value.len() > state.partition(partition_kind).capacity_bytes {
            return Ok(false);
        }
        let stamp = state.bump_stamp();
        let partition = state.partition_mut(partition_kind);
        if let Some(replaced) = partition.entries.remove(&key) {
            partition.usage_bytes = partition
                .usage_bytes
                .checked_sub(replaced.value.len())
                .expect("cache usage includes every retained entry");
        }
        while !fits_in_budget(partition.usage_bytes, value.len(), partition.capacity_bytes) {
            partition.evict_oldest();
            partition.evictions = partition.evictions.saturating_add(1);
        }
        partition.usage_bytes = partition
            .usage_bytes
            .checked_add(value.len())
            .expect("cache admission was checked against partition capacity");
        partition.entries.insert(key, CacheEntry { value, stamp });
        partition.admissions = partition.admissions.saturating_add(1);
        Ok(true)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_result(&self) -> Result<std::sync::MutexGuard<'_, CacheState>> {
        self.state
            .lock()
            .map_err(|_| Error::Background("block cache lock was poisoned".to_owned()))
    }
}

impl CacheState {
    fn bump_stamp(&mut self) -> u64 {
        if self.next_stamp == u64::MAX {
            self.renormalize_stamps();
        }
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.saturating_add(1);
        stamp
    }

    fn renormalize_stamps(&mut self) {
        let mut ordered = self
            .metadata
            .entries
            .iter()
            .map(|(key, entry)| (entry.stamp, CachePartition::Metadata, *key))
            .chain(
                self.data
                    .entries
                    .iter()
                    .map(|(key, entry)| (entry.stamp, CachePartition::Data, *key)),
            )
            .collect::<Vec<_>>();
        ordered.sort_unstable();
        for (stamp, (_, partition, key)) in ordered.into_iter().enumerate() {
            self.partition_mut(partition)
                .entries
                .get_mut(&key)
                .expect("ranked cache entry still exists")
                .stamp = u64::try_from(stamp).unwrap_or(u64::MAX);
        }
        self.next_stamp = u64::try_from(
            self.metadata
                .entries
                .len()
                .saturating_add(self.data.entries.len()),
        )
        .unwrap_or(u64::MAX);
    }

    fn partition(&self, partition: CachePartition) -> &PartitionState {
        match partition {
            CachePartition::Metadata => &self.metadata,
            CachePartition::Data => &self.data,
        }
    }

    fn partition_mut(&mut self, partition: CachePartition) -> &mut PartitionState {
        match partition {
            CachePartition::Metadata => &mut self.metadata,
            CachePartition::Data => &mut self.data,
        }
    }
}

impl PartitionState {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            usage_bytes: 0,
            entries: HashMap::new(),
            hits: 0,
            misses: 0,
            admissions: 0,
            evictions: 0,
        }
    }

    fn snapshot(&self) -> CachePartitionSnapshot {
        debug_assert_eq!(
            self.usage_bytes,
            self.entries
                .values()
                .try_fold(0_usize, |total, entry| total.checked_add(entry.value.len()))
                .expect("retained cache entry sizes fit in the partition budget")
        );
        CachePartitionSnapshot {
            capacity_bytes: self.capacity_bytes,
            usage_bytes: self.usage_bytes,
            entries: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
            admissions: self.admissions,
            evictions: self.evictions,
        }
    }

    fn evict_oldest(&mut self) {
        let oldest = self
            .entries
            .iter()
            .min_by_key(|(key, entry)| (entry.stamp, **key))
            .map(|(key, _)| *key)
            .expect("a partition needing room contains an entry");
        let removed = self
            .entries
            .remove(&oldest)
            .expect("selected cache entry still exists");
        self.usage_bytes = self
            .usage_bytes
            .checked_sub(removed.value.len())
            .expect("cache usage includes every retained entry");
    }
}

fn fits_in_budget(usage_bytes: usize, entry_bytes: usize, capacity_bytes: usize) -> bool {
    entry_bytes <= capacity_bytes.saturating_sub(usage_bytes)
}

/// Owned statistics for one independently budgeted cache partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CachePartitionSnapshot {
    /// Configured byte budget.
    pub capacity_bytes: usize,
    /// Bytes currently charged to retained entries.
    pub usage_bytes: usize,
    /// Number of retained entries.
    pub entries: usize,
    /// Successful cache lookups.
    pub hits: u64,
    /// Cache lookups that required storage work.
    pub misses: u64,
    /// Entries accepted into the partition.
    pub admissions: u64,
    /// Entries removed by the LRU budget.
    pub evictions: u64,
}

/// Owned statistics for the metadata and data cache partitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheSnapshot {
    /// Index and filter partition.
    pub metadata: CachePartitionSnapshot,
    /// Data-block partition.
    pub data: CachePartitionSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(byte: u8, len: usize) -> Arc<[u8]> {
        vec![byte; len].into()
    }

    fn assert_exact_usage(partition: &PartitionState) {
        assert_eq!(
            partition.usage_bytes,
            partition
                .entries
                .values()
                .try_fold(0_usize, |total, entry| total.checked_add(entry.value.len()))
                .unwrap()
        );
    }

    #[test]
    fn near_overflow_recency_keeps_new_and_recent_entries() {
        let cache = BlockCache::new(15).unwrap();
        cache.insert(1, 1, BlockKind::Data, value(1, 4)).unwrap();
        cache.insert(1, 2, BlockKind::Data, value(2, 4)).unwrap();
        {
            let mut state = cache.lock();
            state.next_stamp = u64::MAX;
        }

        assert!(cache.get(1, 1, BlockKind::Data).unwrap().is_some());
        cache.insert(1, 3, BlockKind::Data, value(3, 5)).unwrap();

        assert!(cache.get(1, 1, BlockKind::Data).unwrap().is_some());
        assert!(cache.get(1, 2, BlockKind::Data).unwrap().is_none());
        assert!(cache.get(1, 3, BlockKind::Data).unwrap().is_some());
    }

    #[test]
    fn accounting_stays_exact_across_replacement_and_eviction() {
        let cache = BlockCache::new(10).unwrap();
        cache.insert(1, 1, BlockKind::Data, value(1, 3)).unwrap();
        cache.insert(1, 2, BlockKind::Data, value(2, 4)).unwrap();
        cache.insert(1, 1, BlockKind::Data, value(3, 6)).unwrap();

        let state = cache.lock();
        assert_exact_usage(&state.data);
        assert!(state.data.usage_bytes <= state.data.capacity_bytes);
    }

    #[test]
    fn near_max_accounting_rejects_addition_that_cannot_fit() {
        assert!(!fits_in_budget(usize::MAX - 1, 2, usize::MAX));
        assert!(fits_in_budget(usize::MAX - 1, 1, usize::MAX));
    }
}
