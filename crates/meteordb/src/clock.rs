use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Supplies wall-clock Unix time for expiration decisions.
///
/// The trait keeps time-dependent engine code deterministic in tests: production
/// uses [`SystemClock`], while tests can provide a fixed implementation.
/// Implementations must be thread-safe because database work may use the clock
/// from foreground and background threads.
pub trait Clock: Send + Sync {
    /// Returns the current wall-clock time in milliseconds since the Unix epoch.
    ///
    /// This value is intended for expiration timestamps, not elapsed-time
    /// measurement; wall clocks can move backward when the system clock changes.
    fn now_unix_ms(&self) -> u64;
}

/// A [`Clock`] backed by the operating system's wall clock.
///
/// Times before the Unix epoch map to zero, and timestamps too large for `u64`
/// saturate at [`u64::MAX`], keeping the infallible [`Clock`] contract.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        let milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        u64::try_from(milliseconds).unwrap_or(u64::MAX)
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

    /// Replaces the wall-clock value returned by [`Clock::now_unix_ms`].
    pub fn set(&self, now_unix_ms: u64) {
        self.now_unix_ms.store(now_unix_ms, Ordering::Release);
    }
}

impl Clock for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        self.now_unix_ms.load(Ordering::Acquire)
    }
}
