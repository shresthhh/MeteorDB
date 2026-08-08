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
        let stamp = state.bump_stamp();
        let key = CacheKey {
            file_number,
            block_offset,
            kind,
        };
        let partition = state.partition_mut(kind.partition());
        if value.len() > partition.capacity_bytes {
            return Ok(false);
        }
        if let Some(replaced) = partition.entries.remove(&key) {
            partition.usage_bytes -= replaced.value.len();
        }
        partition.usage_bytes += value.len();
        partition.entries.insert(key, CacheEntry { value, stamp });
        partition.admissions = partition.admissions.saturating_add(1);
        while partition.usage_bytes > partition.capacity_bytes {
            let oldest = partition
                .entries
                .iter()
                .min_by_key(|(key, entry)| (entry.stamp, **key))
                .map(|(key, _)| *key)
                .expect("an over-budget partition contains an entry");
            let removed = partition
                .entries
                .remove(&oldest)
                .expect("selected cache entry still exists");
            partition.usage_bytes -= removed.value.len();
            partition.evictions = partition.evictions.saturating_add(1);
        }
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
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.wrapping_add(1);
        stamp
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
