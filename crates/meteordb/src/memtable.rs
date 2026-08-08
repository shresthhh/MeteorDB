#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeMap;

use crate::{Error, InternalKey, Result, SequenceNumber, ValueKind, WriteBatch, WriteOp};

const ENGINE_VALUE_MAGIC: [u8; 3] = *b"MEV";
const ENGINE_VALUE_VERSION: u8 = 1;
const VALUE_TAG: u8 = 0;
const TOMBSTONE_TAG: u8 = 1;
const EXPIRATION_FLAG: u8 = 1;
const ENGINE_VALUE_HEADER_BYTES: usize = 6;

#[cfg(test)]
thread_local! {
    static OWNED_ENTRY_CLONES: Cell<usize> = const { Cell::new(0) };
}

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

    pub(crate) fn encode_engine_value(&self) -> Vec<u8> {
        let (tag, flags, expiration, value) = match self {
            Self::Value {
                value,
                expires_at_unix_ms,
            } => (
                VALUE_TAG,
                u8::from(expires_at_unix_ms.is_some()) * EXPIRATION_FLAG,
                *expires_at_unix_ms,
                value.as_slice(),
            ),
            Self::Tombstone => (TOMBSTONE_TAG, 0, None, &[][..]),
        };
        let mut encoded = Vec::with_capacity(
            ENGINE_VALUE_HEADER_BYTES
                .saturating_add(expiration.map_or(0, |_| 8))
                .saturating_add(value.len()),
        );
        encoded.extend_from_slice(&ENGINE_VALUE_MAGIC);
        encoded.push(ENGINE_VALUE_VERSION);
        encoded.push(tag);
        encoded.push(flags);
        if let Some(expiration) = expiration {
            encoded.extend_from_slice(&expiration.to_le_bytes());
        }
        encoded.extend_from_slice(value);
        encoded
    }

    pub(crate) fn decode_sstable(
        internal_kind: ValueKind,
        encoded: Vec<u8>,
        engine_encoded: bool,
    ) -> Result<Self> {
        if !engine_encoded {
            return Ok(if internal_kind == ValueKind::Deletion {
                Self::Tombstone
            } else {
                Self::value(encoded, None)
            });
        }
        if encoded.len() < ENGINE_VALUE_HEADER_BYTES {
            return Err(engine_value_corruption("header is truncated"));
        }
        if encoded[..3] != ENGINE_VALUE_MAGIC {
            return Err(engine_value_corruption("magic is not MEV"));
        }
        if encoded[3] != ENGINE_VALUE_VERSION {
            return Err(engine_value_corruption(format!(
                "unsupported encoding version {}",
                encoded[3]
            )));
        }
        let tag = encoded[4];
        let flags = encoded[5];
        if flags & !EXPIRATION_FLAG != 0 {
            return Err(engine_value_corruption(format!(
                "unknown flags {flags:#04x}"
            )));
        }
        match tag {
            VALUE_TAG => {
                if internal_kind != ValueKind::Value {
                    return Err(engine_value_corruption(
                        "value payload has a deletion internal key",
                    ));
                }
                let (expires_at_unix_ms, value_start) = if flags & EXPIRATION_FLAG == 0 {
                    (None, ENGINE_VALUE_HEADER_BYTES)
                } else {
                    let end = ENGINE_VALUE_HEADER_BYTES + 8;
                    let expiration = encoded
                        .get(ENGINE_VALUE_HEADER_BYTES..end)
                        .ok_or_else(|| engine_value_corruption("expiration is truncated"))?;
                    (
                        Some(u64::from_le_bytes(
                            expiration
                                .try_into()
                                .expect("checked expiration has eight bytes"),
                        )),
                        end,
                    )
                };
                Ok(Self::Value {
                    value: encoded[value_start..].to_vec(),
                    expires_at_unix_ms,
                })
            }
            TOMBSTONE_TAG => {
                if internal_kind != ValueKind::Deletion {
                    return Err(engine_value_corruption(
                        "tombstone payload has a value internal key",
                    ));
                }
                if flags != 0 || encoded.len() != ENGINE_VALUE_HEADER_BYTES {
                    return Err(engine_value_corruption(
                        "tombstone carries flags or trailing bytes",
                    ));
                }
                Ok(Self::Tombstone)
            }
            _ => Err(engine_value_corruption(format!("unknown record tag {tag}"))),
        }
    }
}

fn engine_value_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable engine value",
        detail: detail.into(),
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
    approximate_bytes: usize,
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
        for (key, record) in prepared {
            let record_bytes = match &record {
                ValueRecord::Value { value, .. } => {
                    value.len() + std::mem::size_of::<Option<u64>>()
                }
                ValueRecord::Tombstone => 0,
            };
            self.approximate_bytes = self
                .approximate_bytes
                .saturating_add(key.as_bytes().len())
                .saturating_add(record_bytes);
            self.entries.insert(key, record);
        }
        Ok(())
    }

    /// Returns the newest record for `user_key` visible at `sequence`.
    ///
    /// Snapshot visibility means versions newer than `sequence` are skipped.
    /// The returned record is borrowed from the memtable; the caller may read
    /// it only while the memtable borrow remains valid, avoiding an allocation
    /// on this internal lookup path.
    pub fn get(&self, user_key: &[u8], sequence: SequenceNumber) -> Result<Option<&ValueRecord>> {
        Ok(self
            .get_entry(user_key, sequence)?
            .map(|(_, record)| record))
    }

    /// Returns the newest visible internal key and record for `user_key`.
    pub fn get_entry(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<(&InternalKey, &ValueRecord)>> {
        let seek = InternalKey::try_new(user_key, sequence, ValueKind::Deletion)?;
        Ok(self
            .entries
            .range(seek..)
            .next()
            .filter(|(key, _)| key.user_key() == user_key))
    }

    /// Returns the approximate bytes retained by this table.
    pub fn approximate_bytes(&self) -> usize {
        self.approximate_bytes
    }

    /// Reports whether the table contains no records.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates over every internal key and record in storage order.
    ///
    /// The iterator borrows the memtable, so Rust prevents mutation while the
    /// iteration is active. Entries are ordered by user key ascending and
    /// sequence descending.
    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &ValueRecord)> {
        self.entries.iter()
    }

    pub(crate) fn owned_entries_matching(
        &self,
        mut include: impl FnMut(&InternalKey) -> bool,
    ) -> Vec<(InternalKey, ValueRecord)> {
        self.entries
            .iter()
            .filter(|(key, _)| include(key))
            .map(|(key, record)| {
                #[cfg(test)]
                OWNED_ENTRY_CLONES.set(OWNED_ENTRY_CLONES.get().saturating_add(1));
                (key.clone(), record.clone())
            })
            .collect()
    }
}

#[cfg(test)]
pub(crate) fn reset_owned_entry_clone_count() {
    OWNED_ENTRY_CLONES.set(0);
}

#[cfg(test)]
pub(crate) fn owned_entry_clone_count() -> usize {
    OWNED_ENTRY_CLONES.get()
}

#[cfg(test)]
mod engine_value_tests {
    use super::ValueRecord;
    use crate::{Error, ValueKind};

    #[test]
    fn checked_engine_value_round_trips_expiration_and_tombstones() {
        for (kind, record) in [
            (ValueKind::Value, ValueRecord::value(b"value", Some(42))),
            (ValueKind::Value, ValueRecord::value(b"plain", None)),
            (ValueKind::Deletion, ValueRecord::Tombstone),
        ] {
            let encoded = record.encode_engine_value();
            assert_eq!(
                ValueRecord::decode_sstable(kind, encoded, true).unwrap(),
                record
            );
        }
    }

    #[test]
    fn malformed_engine_value_metadata_is_corruption() {
        for (kind, encoded) in [
            (ValueKind::Value, b"short".to_vec()),
            (ValueKind::Value, b"BAD\x01\0\0".to_vec()),
            (ValueKind::Value, b"MEV\x02\0\0".to_vec()),
            (ValueKind::Value, b"MEV\x01\0\x80".to_vec()),
            (ValueKind::Value, b"MEV\x01\0\x01tiny".to_vec()),
            (ValueKind::Deletion, b"MEV\x01\0\0".to_vec()),
            (ValueKind::Value, b"MEV\x01\x01\0".to_vec()),
            (ValueKind::Deletion, b"MEV\x01\x01\0extra".to_vec()),
        ] {
            assert!(matches!(
                ValueRecord::decode_sstable(kind, encoded, true),
                Err(Error::Corruption { .. })
            ));
        }
    }
}
