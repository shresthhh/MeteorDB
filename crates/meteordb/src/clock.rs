use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Error, Result};

/// Supplies wall-clock Unix time for expiration decisions.
///
/// The trait keeps time-dependent engine code deterministic in tests: production
/// uses [`SystemClock`], while tests can provide a fixed implementation.
/// Implementations must be thread-safe because database work may use the clock
/// from foreground and background threads. Within one clock instance, returned
/// values must never decrease.
pub trait Clock: Send + Sync {
    /// Returns the current wall-clock time in milliseconds since the Unix epoch.
    ///
    /// This value is intended for persisted expiration timestamps, not
    /// elapsed-time measurement. Implementations clamp or reject wall-clock
    /// rollback so expiration decisions cannot reverse during one engine
    /// process.
    fn now_unix_ms(&self) -> u64;
}

/// A [`Clock`] backed by the operating system's wall clock.
///
/// Times before the Unix epoch map to zero, and timestamps too large for `u64`
/// saturate at [`u64::MAX`], keeping the infallible [`Clock`] contract. Each
/// instance clamps observed rollback to its highest value. That high-water mark
/// is process-local and is not persisted: after restart, persisted TTL deadlines
/// assume the host wall clock has not moved behind time observed by the previous
/// process.
#[derive(Clone, Debug, Default)]
pub struct SystemClock {
    high_water_unix_ms: Arc<AtomicU64>,
}

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        let milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let observed = u64::try_from(milliseconds).unwrap_or(u64::MAX);
        self.high_water_unix_ms
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
