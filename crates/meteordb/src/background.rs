use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

use crate::Version;

pub(crate) struct ObsoleteSstables {
    pub(crate) version: Arc<Version>,
    pub(crate) files: VecDeque<u64>,
}

impl ObsoleteSstables {
    pub(crate) fn is_unreferenced(&self) -> bool {
        Arc::strong_count(&self.version) == 1
    }
}

/// Wakes the flush worker and engine callers waiting for flush progress.
#[derive(Default)]
pub(crate) struct BackgroundSignal {
    state_changed: Condvar,
    worker_generation: Mutex<u64>,
    worker_changed: Condvar,
}

impl BackgroundSignal {
    pub(crate) fn wake_all(&self) {
        let mut generation = self
            .worker_generation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *generation = generation.wrapping_add(1);
        self.worker_changed.notify_all();
        self.state_changed.notify_all();
    }

    pub(crate) fn wait<'a, T>(
        &self,
        guard: std::sync::MutexGuard<'a, T>,
    ) -> std::sync::MutexGuard<'a, T> {
        self.state_changed
            .wait(guard)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn generation(&self) -> u64 {
        *self
            .worker_generation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn wait_for_work(&self, observed: u64) -> u64 {
        let mut generation = self
            .worker_generation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *generation == observed {
            generation = self
                .worker_changed
                .wait(generation)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *generation
    }
}
