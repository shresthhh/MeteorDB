use std::sync::atomic::{AtomicU64, Ordering};

use crate::{BlockCache, CacheSnapshot, NUM_LEVELS};

/// An owned, structured view of engine read-path activity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsSnapshot {
    /// Valid point reads, including snapshot reads and misses.
    pub point_reads: u64,
    /// Total SSTables opened and queried by those reads.
    pub sstable_probes: u64,
    /// Bloom-filter queries performed by point reads.
    pub bloom_checks: u64,
    /// Bloom queries that proved a candidate absent before a data-block read.
    pub bloom_useful_negatives: u64,
    /// Block-cache capacity, usage, and activity by partition.
    pub cache: CacheSnapshot,
    /// SSTables actually opened and probed in each on-disk level.
    pub level_table_probes: [u64; NUM_LEVELS],
}

impl StatsSnapshot {
    /// Returns average SSTable probes per point read.
    pub fn read_amplification(&self) -> f64 {
        if self.point_reads == 0 {
            0.0
        } else {
            self.sstable_probes as f64 / self.point_reads as f64
        }
    }
}

pub(crate) struct ReadStats {
    point_reads: AtomicU64,
    bloom_checks: AtomicU64,
    bloom_useful_negatives: AtomicU64,
    level_table_probes: [AtomicU64; NUM_LEVELS],
}

impl Default for ReadStats {
    fn default() -> Self {
        Self {
            point_reads: AtomicU64::new(0),
            bloom_checks: AtomicU64::new(0),
            bloom_useful_negatives: AtomicU64::new(0),
            level_table_probes: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl ReadStats {
    pub(crate) fn record_point_read(&self) {
        self.point_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_bloom_check(&self, useful_negative: bool) {
        self.bloom_checks.fetch_add(1, Ordering::Relaxed);
        if useful_negative {
            self.bloom_useful_negatives.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_table_probe(&self, level: usize) {
        self.level_table_probes[level].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, cache: &BlockCache) -> StatsSnapshot {
        let level_table_probes =
            std::array::from_fn(|level| self.level_table_probes[level].load(Ordering::Relaxed));
        StatsSnapshot {
            point_reads: self.point_reads.load(Ordering::Relaxed),
            sstable_probes: level_table_probes.iter().sum(),
            bloom_checks: self.bloom_checks.load(Ordering::Relaxed),
            bloom_useful_negatives: self.bloom_useful_negatives.load(Ordering::Relaxed),
            cache: cache.snapshot(),
            level_table_probes,
        }
    }
}
