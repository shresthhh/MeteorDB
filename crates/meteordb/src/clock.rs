use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Error, Result};

/// Supplies wall-clock Unix time for expiration decisions.
///
/// The trait keeps time-dependent engine code deterministic in tests: production
/// uses [`SystemClock`], while tests can provide a fixed implementation.
/// Implementations must be thread-safe because database work may use the clock
/// from foreground and background threads. Custom implementations must not
/// decrease while an engine is open or when the same clock is reused to reopen
/// an engine.
pub trait Clock: Send + Sync {
    /// Returns the current wall-clock time in milliseconds since the Unix epoch.
    ///
    /// This value is intended for persisted expiration timestamps, not
    /// elapsed-time measurement. Implementations must clamp or reject rollback
    /// over the lifetime in which an engine uses them.
    fn now_unix_ms(&self) -> u64;
}

/// A [`Clock`] backed by the operating system's wall clock.
///
/// Times before the Unix epoch map to zero, and timestamps too large for `u64`
/// saturate at [`u64::MAX`], keeping the infallible [`Clock`] contract. All
/// instances share a process-global high-water mark, so same-process engine
/// reopens cannot observe rollback.
///
/// The high-water mark is intentionally not persisted. Host wall-clock rollback
/// across process restarts is unsupported: persisted TTL deadlines assume a new
/// process does not start behind wall time observed by the previous process.
/// MeteorDB does not add hidden durable writes to reads to enforce that
/// environmental assumption.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

static SYSTEM_HIGH_WATER_UNIX_MS: AtomicU64 = AtomicU64::new(0);

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        let milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let observed = u64::try_from(milliseconds).unwrap_or(u64::MAX);
        Self::clamp_observed(observed)
    }
}

impl SystemClock {
    fn clamp_observed(observed: u64) -> u64 {
        SYSTEM_HIGH_WATER_UNIX_MS
            .fetch_max(observed, Ordering::AcqRel)
            .max(observed)
    }
}

/// A deterministic wall clock whose Unix-millisecond value tests can replace.
///
/// Clones share one atomic value, so an engine can own one clone while a test
/// advances another. This clock models wall-clock time, not MVCC sequence time:
/// snapshots freeze a sequence number but consult the clock again on each read.
#[derive(Clone, Debug)]
pub struct ManualClock {
    now_unix_ms: Arc<AtomicU64>,
}

impl ManualClock {
    /// Creates a clock fixed initially at `now_unix_ms`.
    pub fn new(now_unix_ms: u64) -> Self {
        Self {
            now_unix_ms: Arc::new(AtomicU64::new(now_unix_ms)),
        }
    }

    /// Advances the wall-clock value returned by [`Clock::now_unix_ms`].
    ///
    /// Equal values are accepted. A value below the current time returns
    /// [`Error::InvalidArgument`] and leaves the clock unchanged.
    pub fn set(&self, now_unix_ms: u64) -> Result<()> {
        self.now_unix_ms
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (now_unix_ms >= current).then_some(now_unix_ms)
            })
            .map(|_| ())
            .map_err(|current| {
                Error::InvalidArgument(format!(
                    "manual clock cannot move backward from {current} to {now_unix_ms}"
                ))
            })
    }
}

impl Clock for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        self.now_unix_ms.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_clamps_rollback_across_same_process_reconstruction() {
        let first_instance = SystemClock;
        let high_water = first_instance.now_unix_ms();
        let _reopened_instance = SystemClock;

        assert!(SystemClock::clamp_observed(high_water.saturating_sub(1)) >= high_water);
    }
}
