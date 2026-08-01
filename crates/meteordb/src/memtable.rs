use std::collections::BTreeMap;

use crate::{InternalKey, Result, SequenceNumber, ValueKind, WriteBatch, WriteOp};

/// The payload stored beside an [`InternalKey`] in a [`MemTable`].
///
/// A tombstone is kept as a real record instead of removing an older value.
/// That older value may still be needed by a snapshot whose sequence predates
/// the deletion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueRecord {
    /// A user value and its optional absolute expiration timestamp.
    Value {
        /// Owned value bytes.
        value: Vec<u8>,
        /// Unix time in milliseconds after which the value is expired.
        expires_at_unix_ms: Option<u64>,
    },
    /// A deletion marker that hides earlier values from newer readers.
    Tombstone,
}

impl ValueRecord {
    /// Creates an owned value record by copying `value`.
    ///
    /// Copying gives the memtable independent ownership, so the caller may
    /// reuse or drop its input buffer after this function returns.
    pub fn value(value: impl AsRef<[u8]>, expires_at_unix_ms: Option<u64>) -> Self {
        Self::Value {
            value: value.as_ref().to_vec(),
            expires_at_unix_ms,
        }
    }

    /// Creates a deletion tombstone.
    pub fn tombstone() -> Self {
        Self::Tombstone
    }

    /// Borrows the user bytes, or returns `None` for a tombstone.
    pub fn as_value(&self) -> Option<&[u8]> {
        match self {
            Self::Value { value, .. } => Some(value),
            Self::Tombstone => None,
        }
    }

    /// Returns the absolute expiration timestamp stored with a value.
    ///
    /// Both a tombstone and a non-expiring value return `None`; callers can
    /// distinguish them with [`ValueRecord::as_value`].
    pub fn expires_at_unix_ms(&self) -> Option<u64> {
        match self {
            Self::Value {
                expires_at_unix_ms, ..
            } => *expires_at_unix_ms,
            Self::Tombstone => None,
        }
    }
}

/// An ordered in-memory collection of MVCC versions.
///
/// [`BTreeMap`] is Rust's sorted map. Unlike a hash map, it keeps
/// [`InternalKey`] values in comparison order, so all versions of one user key
/// are adjacent and the newest sequence appears first. That ordering supports
/// point reads now and ordered iteration for future flushing without adding an
/// SSTable implementation to this task.
#[derive(Debug, Default)]
pub struct MemTable {
    entries: BTreeMap<InternalKey, ValueRecord>,
}

impl MemTable {
    /// Applies one complete batch at `sequence`.
    ///
    /// The batch is consumed so its owned key and value buffers can move into
    /// the memtable instead of being cloned. If a key occurs more than once,
    /// only its final operation is stored, matching ordered batch semantics.
    /// All internal keys are constructed before `entries` is changed, so an
    /// invalid sequence cannot leave a partially applied batch.
    pub fn apply(&mut self, sequence: SequenceNumber, batch: WriteBatch) -> Result<()> {
        let mut final_operations = BTreeMap::<Vec<u8>, ValueRecord>::new();
        for operation in batch.into_operations() {
            match operation {
                WriteOp::Put {
                    key,
                    value,
                    expires_at_unix_ms,
                } => {
                    final_operations.insert(
                        key,
                        ValueRecord::Value {
                            value,
                            expires_at_unix_ms,
                        },
                    );
                }
                WriteOp::Delete { key } => {
                    final_operations.insert(key, ValueRecord::Tombstone);
                }
            }
        }

        let mut prepared = Vec::with_capacity(final_operations.len());
        for (user_key, record) in final_operations {
            let kind = match record {
                ValueRecord::Value { .. } => ValueKind::Value,
                ValueRecord::Tombstone => ValueKind::Deletion,
            };
            prepared.push((InternalKey::try_new(user_key, sequence, kind)?, record));
        }
        self.entries.extend(prepared);
        Ok(())
    }

    /// Returns the newest record for `user_key` visible at `sequence`.
    ///
    /// Snapshot visibility means versions newer than `sequence` are skipped.
    /// The returned record is borrowed from the memtable; the caller may read
    /// it only while the memtable borrow remains valid, avoiding an allocation
    /// on this internal lookup path.
    pub fn get(&self, user_key: &[u8], sequence: SequenceNumber) -> Result<Option<&ValueRecord>> {
        let seek = InternalKey::try_new(user_key, sequence, ValueKind::Deletion)?;
        Ok(self
            .entries
            .range(seek..)
            .next()
            .filter(|(key, _)| key.user_key() == user_key)
            .map(|(_, record)| record))
    }

    /// Iterates over every internal key and record in storage order.
    ///
    /// The iterator borrows the memtable, so Rust prevents mutation while the
    /// iteration is active. Entries are ordered by user key ascending and
    /// sequence descending.
    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &ValueRecord)> {
        self.entries.iter()
    }
}
