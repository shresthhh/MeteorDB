use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::SequenceNumber;

type SnapshotCounts = Arc<Mutex<BTreeMap<SequenceNumber, usize>>>;

/// Tracks the sequence numbers currently protected by read snapshots.
///
/// Clones share one reference-counted map through [`Arc`], so snapshots
/// acquired from any clone affect every clone's view. A [`Mutex`] serializes
/// short map updates between threads; this simple design favors correctness
/// over a more complicated lock-free registry.
#[derive(Clone, Debug, Default)]
pub struct SnapshotRegistry {
    active: SnapshotCounts,
}

impl SnapshotRegistry {
    /// Registers `sequence` and returns a guard that keeps it active.
    ///
    /// Multiple guards may protect the same sequence; the registry counts each
    /// one separately. The registration is automatically undone when the guard
    /// is dropped, including during early returns and panic unwinding.
    ///
    /// # Panics
    ///
    /// Panics only if one sequence somehow accumulates more than [`usize::MAX`]
    /// simultaneous guards.
    pub fn acquire(&self, sequence: SequenceNumber) -> SnapshotGuard {
        let mut active = lock_counts(&self.active);
        let count = active.entry(sequence).or_default();
        *count = count
            .checked_add(1)
            .expect("snapshot reference count overflowed usize");
        drop(active);

        SnapshotGuard {
            active: Arc::clone(&self.active),
            sequence,
        }
    }

    /// Returns the smallest sequence protected by any active snapshot.
    ///
    /// Compaction uses this boundary to avoid removing a historical version
    /// that an older reader could still observe. `None` means no snapshot is
    /// currently registered.
    pub fn oldest_active(&self) -> Option<SequenceNumber> {
        lock_counts(&self.active)
            .first_key_value()
            .map(|(&key, _)| key)
    }
}

/// An RAII handle that keeps one snapshot sequence registered while it exists.
///
/// RAII means resource lifetime follows ordinary Rust value lifetime: acquiring
/// creates the registration, and [`Drop`] releases it automatically. Guards are
/// intentionally not cloneable because cloning without incrementing the
/// registry would make release counts incorrect.
#[derive(Debug)]
pub struct SnapshotGuard {
    active: SnapshotCounts,
    sequence: SequenceNumber,
}

impl SnapshotGuard {
    /// Returns the sequence number visible through this snapshot.
    ///
    /// A later read should ignore internal-key versions with larger sequences.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        let mut active = lock_counts(&self.active);
        match active.entry(self.sequence) {
            Entry::Occupied(mut entry) if *entry.get() > 1 => {
                *entry.get_mut() -= 1;
            }
            Entry::Occupied(entry) => {
                entry.remove();
            }
            Entry::Vacant(_) => {
                debug_assert!(false, "snapshot guard was not registered");
            }
        }
    }
}

fn lock_counts(counts: &SnapshotCounts) -> MutexGuard<'_, BTreeMap<SequenceNumber, usize>> {
    counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
