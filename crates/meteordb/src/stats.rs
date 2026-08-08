use std::sync::Mutex;

use crate::{BlockCache, CacheSnapshot, NUM_LEVELS};

/// An owned, structured view of engine read-path activity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsSnapshot {
    /// Valid point reads, including snapshot reads and misses.
    pub point_reads: u64,
    /// Saturating sum of SSTables opened and queried across all levels.
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
    state: Mutex<ReadStatsState>,
}

#[derive(Default)]
struct ReadStatsState {
    point_reads: u64,
    bloom_checks: u64,
    bloom_useful_negatives: u64,
    level_table_probes: [u64; NUM_LEVELS],
}

impl ReadStats {
    pub(crate) fn record_point_read(&self) {
        let mut state = self.lock();
        state.point_reads = state.point_reads.saturating_add(1);
    }

    pub(crate) fn record_bloom_check(&self, useful_negative: bool) {
        let mut state = self.lock();
        state.bloom_checks = state.bloom_checks.saturating_add(1);
        if useful_negative {
            state.bloom_useful_negatives = state.bloom_useful_negatives.saturating_add(1);
        }
    }

    pub(crate) fn record_table_probe(&self, level: usize) {
        let mut state = self.lock();
        state.level_table_probes[level] = state.level_table_probes[level].saturating_add(1);
    }

    pub(crate) fn snapshot(&self, cache: &BlockCache) -> StatsSnapshot {
        let (point_reads, bloom_checks, bloom_useful_negatives, level_table_probes) = {
            let state = self.lock();
            (
                state.point_reads,
                state.bloom_checks,
                state.bloom_useful_negatives,
                state.level_table_probes,
            )
        };
        StatsSnapshot {
            point_reads,
            sstable_probes: level_table_probes
                .iter()
                .copied()
                .fold(0_u64, u64::saturating_add),
            bloom_checks,
            bloom_useful_negatives,
            cache: cache.snapshot(),
            level_table_probes,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ReadStatsState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for ReadStats {
    fn default() -> Self {
        Self {
            state: Mutex::new(ReadStatsState::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    use super::*;

    #[test]
    fn related_statistics_remain_coherent_during_concurrent_updates() {
        let stats = Arc::new(ReadStats::default());
        let cache = Arc::new(BlockCache::new(10).unwrap());
        let done = Arc::new(AtomicBool::new(false));
        let writer_stats = stats.clone();
        let writer_done = done.clone();
        let writer = thread::spawn(move || {
            for index in 0..250_000 {
                writer_stats.record_bloom_check(true);
                writer_stats.record_table_probe(index % NUM_LEVELS);
            }
            writer_done.store(true, Ordering::Release);
        });

        while !done.load(Ordering::Acquire) {
            let snapshot = stats.snapshot(&cache);
            assert!(snapshot.bloom_useful_negatives <= snapshot.bloom_checks);
            assert_eq!(
                snapshot.sstable_probes,
                snapshot
                    .level_table_probes
                    .iter()
                    .copied()
                    .fold(0_u64, u64::saturating_add)
            );
        }
        writer.join().unwrap();
    }

    #[test]
    fn read_statistics_saturate_instead_of_wrapping() {
        let stats = ReadStats::default();
        {
            let mut state = stats.lock();
            state.point_reads = u64::MAX;
            state.bloom_checks = u64::MAX;
            state.bloom_useful_negatives = u64::MAX;
            state.level_table_probes[0] = u64::MAX;
            state.level_table_probes[1] = 1;
        }

        stats.record_point_read();
        stats.record_bloom_check(true);
        stats.record_table_probe(0);
        let snapshot = stats.snapshot(&BlockCache::new(10).unwrap());

        assert_eq!(snapshot.point_reads, u64::MAX);
        assert_eq!(snapshot.bloom_checks, u64::MAX);
        assert_eq!(snapshot.bloom_useful_negatives, u64::MAX);
        assert_eq!(snapshot.level_table_probes[0], u64::MAX);
        assert_eq!(snapshot.sstable_probes, u64::MAX);
    }
}
