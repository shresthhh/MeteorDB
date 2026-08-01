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
