use std::collections::HashSet;

use crate::sstable::format::{put_varint, read_varint};
use crate::{Error, Result};

const FIXED_RESTART_BYTES: usize = 4;

/// Builds one sorted, prefix-compressed SSTable data block.
///
/// Each entry stores three unsigned varints—shared key-prefix bytes, unshared
/// key-suffix bytes, and value bytes—then the suffix and value. Varints make
/// small lengths cheap while still supporting large records.
///
/// Every `restart_interval` entries, compression resets and the complete key
/// is stored. Those restart points cost extra space but bound key
/// reconstruction work during seeks. Smaller intervals use more memory and
/// disk bandwidth; larger intervals compress better but require more CPU and
/// sequential decoding per seek.
#[derive(Debug)]
pub struct BlockBuilder {
    restart_interval: usize,
    entries_since_restart: usize,
    entries: Vec<u8>,
    restarts: Vec<u32>,
    previous_key: Vec<u8>,
    entry_count: usize,
}

impl BlockBuilder {
    /// Creates a builder with a positive restart interval.
    ///
    /// # Panics
    ///
    /// Panics when `restart_interval` is zero. Use [`BlockBuilder::try_new`] to
    /// validate untrusted configuration without panicking.
    pub fn new(restart_interval: usize) -> Self {
        Self::try_new(restart_interval).expect("restart_interval must be greater than zero")
    }

    /// Creates a builder after validating its restart interval.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] when `restart_interval` is zero.
    pub fn try_new(restart_interval: usize) -> Result<Self> {
        if restart_interval == 0 {
            return Err(Error::InvalidArgument(
                "restart_interval must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            restart_interval,
            entries_since_restart: 0,
            entries: Vec::new(),
            restarts: vec![0],
            previous_key: Vec::new(),
            entry_count: 0,
        })
    }

    /// Adds a key/value pair whose key is strictly greater than the previous key.
    ///
    /// Table code should pass [`crate::InternalKey::as_bytes`] rather than an
    /// ad-hoc user-key/trailer representation. Those bytes preserve the engine's
    /// required user-key-ascending and sequence-descending order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] for duplicate or descending keys and
    /// for lengths or restart offsets that cannot be represented safely.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.entry_count != 0 && key <= self.previous_key.as_slice() {
            return Err(Error::InvalidArgument(
                "SSTable block keys must be strictly increasing".to_owned(),
            ));
        }

        let shared = if self.entries_since_restart == self.restart_interval {
            let offset = u32::try_from(self.entries.len()).map_err(|_| {
                Error::InvalidArgument("SSTable data block exceeds u32 restart offsets".to_owned())
            })?;
            self.restarts.push(offset);
            self.entries_since_restart = 0;
            0
        } else {
            shared_prefix(&self.previous_key, key)
        };
        let unshared = key.len() - shared;
        put_varint(
            &mut self.entries,
            u64::try_from(shared).map_err(|_| length_error("shared key prefix"))?,
        );
        put_varint(
            &mut self.entries,
            u64::try_from(unshared).map_err(|_| length_error("key suffix"))?,
        );
        put_varint(
            &mut self.entries,
            u64::try_from(value.len()).map_err(|_| length_error("value"))?,
        );
        self.entries.extend_from_slice(&key[shared..]);
        self.entries.extend_from_slice(value);
        self.previous_key.clear();
        self.previous_key.extend_from_slice(key);
        self.entries_since_restart += 1;
        self.entry_count += 1;
        Ok(())
    }

    /// Finishes the block by appending fixed-width restart offsets and count.
    ///
    /// Even an empty block contains restart offset zero and a count of one,
    /// making its structural footer unambiguous to a checked decoder.
    pub fn finish(mut self) -> Vec<u8> {
        let restart_count = self.restart_count() as u32;
        for restart in self.restarts {
            self.entries.extend_from_slice(&restart.to_le_bytes());
        }
        self.entries.extend_from_slice(&restart_count.to_le_bytes());
        self.entries
    }

    fn restart_count(&self) -> usize {
        self.restarts.len()
    }
}

/// A validated prefix-compressed SSTable data block.
///
/// The object retains compressed bytes instead of expanding every entry. This
/// reduces memory use in a block cache, while iteration and seeking spend CPU
/// reconstructing keys. Decoding validates the entire structure first, so
/// later operations never expose partially checked data.
#[derive(Clone, Debug)]
pub struct Block {
    entries: Vec<u8>,
    restarts: Vec<usize>,
    entry_count: usize,
}

impl Block {
    /// Validates and copies an encoded data block.
    ///
    /// Validation checks the restart footer, offset ordering and boundaries,
    /// every varint, all checked length additions, key-prefix reconstruction,
    /// and that every restart points to an entry with a zero shared prefix.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] before retaining any malformed block.
    pub fn decode(encoded: impl AsRef<[u8]>) -> Result<Self> {
        let encoded = encoded.as_ref();
        if encoded.len() < FIXED_RESTART_BYTES * 2 {
            return Err(data_corruption("block is too short for restart metadata"));
        }
        let count_start = encoded.len() - FIXED_RESTART_BYTES;
        let restart_count =
            u32::from_le_bytes(encoded[count_start..].try_into().expect("four bytes")) as usize;
        if restart_count == 0 {
            return Err(data_corruption("restart count is zero"));
        }
        let restart_bytes = restart_count
            .checked_mul(FIXED_RESTART_BYTES)
            .ok_or_else(|| data_corruption("restart array length overflows usize"))?;
        let entries_end = count_start
            .checked_sub(restart_bytes)
            .ok_or_else(|| data_corruption("restart array extends before the block"))?;

        let mut restarts = Vec::with_capacity(restart_count);
        for index in 0..restart_count {
            let start = entries_end
                .checked_add(
                    index
                        .checked_mul(FIXED_RESTART_BYTES)
                        .ok_or_else(|| data_corruption("restart-array cursor overflows usize"))?,
                )
                .ok_or_else(|| data_corruption("restart-array cursor overflows usize"))?;
            let offset =
                u32::from_le_bytes(encoded[start..start + 4].try_into().expect("four bytes"))
                    as usize;
            if index == 0 && offset != 0 {
                return Err(data_corruption("first restart offset is not zero"));
            }
            if index > 0 && offset <= restarts[index - 1] {
                return Err(data_corruption(
                    "restart offsets are not strictly increasing",
                ));
            }
            if entries_end == 0 {
                if restart_count != 1 || offset != 0 {
                    return Err(data_corruption("empty block has invalid restart offsets"));
                }
            } else if offset >= entries_end {
                return Err(data_corruption(format!(
                    "restart offset {offset} is outside {entries_end} entry bytes"
                )));
            }
            restarts.push(offset);
        }

        let entries = encoded[..entries_end].to_vec();
        let restart_set: HashSet<usize> = restarts.iter().copied().collect();
        let mut cursor = 0;
        let mut previous_key = Vec::new();
        let mut entry_count = 0;
        let mut observed_restarts = 0;
        while cursor < entries.len() {
            if restart_set.contains(&cursor) {
                observed_restarts += 1;
            }
            let decoded = decode_entry(&entries, cursor, &previous_key)?;
            if restart_set.contains(&cursor) && decoded.shared != 0 {
                return Err(data_corruption(format!(
                    "restart at offset {cursor} has shared prefix {}",
                    decoded.shared
                )));
            }
            if entry_count != 0 && decoded.key <= previous_key {
                return Err(data_corruption("decoded keys are not strictly increasing"));
            }
            previous_key = decoded.key;
            cursor = decoded.next;
            entry_count += 1;
        }
        if cursor != entries.len() {
            return Err(data_corruption(
                "entry cursor does not end at restart array",
            ));
        }
        if entries.is_empty() {
            observed_restarts = 1;
        }
        if observed_restarts != restarts.len() {
            return Err(data_corruption(
                "a restart offset does not point to an entry boundary",
            ));
        }

        Ok(Self {
            entries,
            restarts,
            entry_count,
        })
    }

    /// Returns whether the block contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Returns the number of entries in the block.
    pub fn len(&self) -> usize {
        self.entry_count
    }

    /// Returns a forward iterator that reconstructs keys in sorted order.
    pub fn iter(&self) -> BlockIter<'_> {
        BlockIter {
            block: self,
            cursor: 0,
            previous_key: Vec::new(),
        }
    }

    /// Finds the first entry whose encoded key is greater than or equal to `target`.
    ///
    /// Seek binary-searches full keys at restart points, then reconstructs only
    /// the bounded run after the chosen point. Callers seeking MVCC records must
    /// supply the order-preserving bytes from [`crate::InternalKey::as_bytes`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] if retained bytes unexpectedly fail
    /// reconstruction. Normal construction through [`Block::decode`] validates
    /// these bytes eagerly, so this mainly protects future internal changes.
    pub fn seek(&self, target: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.is_empty() {
            return Ok(None);
        }

        let first_greater = self.restarts.partition_point(|&offset| {
            self.key_at_restart(offset)
                .is_ok_and(|key| key.as_slice() <= target)
        });
        let restart_index = first_greater.saturating_sub(1);
        let mut cursor = self.restarts[restart_index];
        let mut previous_key = Vec::new();
        while cursor < self.entries.len() {
            let decoded = decode_entry(&self.entries, cursor, &previous_key)?;
            if decoded.key.as_slice() >= target {
                return Ok(Some((decoded.key, decoded.value)));
            }
            previous_key = decoded.key;
            cursor = decoded.next;
        }
        Ok(None)
    }

    fn key_at_restart(&self, offset: usize) -> Result<Vec<u8>> {
        Ok(decode_entry(&self.entries, offset, &[])?.key)
    }
}

/// Forward iterator over the reconstructed entries of a [`Block`].
///
/// Each item remains a [`Result`] so corruption can never be silently converted
/// into end-of-iteration if future block implementations decode more lazily.
#[derive(Debug)]
pub struct BlockIter<'a> {
    block: &'a Block,
    cursor: usize,
    previous_key: Vec<u8>,
}

impl Iterator for BlockIter<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.block.entries.len() {
            return None;
        }
        match decode_entry(&self.block.entries, self.cursor, &self.previous_key) {
            Ok(decoded) => {
                self.cursor = decoded.next;
                self.previous_key = decoded.key.clone();
                Some(Ok((decoded.key, decoded.value)))
            }
            Err(error) => {
                self.cursor = self.block.entries.len();
                Some(Err(error))
            }
        }
    }
}

struct DecodedEntry {
    shared: usize,
    key: Vec<u8>,
    value: Vec<u8>,
    next: usize,
}

fn decode_entry(entries: &[u8], start: usize, previous_key: &[u8]) -> Result<DecodedEntry> {
    let (shared, shared_bytes) =
        read_varint(entries, start, "shared key length").map_err(data_corruption)?;
    let unshared_start = checked_add(start, shared_bytes, "unshared key length cursor")?;
    let (unshared, unshared_bytes) =
        read_varint(entries, unshared_start, "unshared key length").map_err(data_corruption)?;
    let value_start = checked_add(unshared_start, unshared_bytes, "value length cursor")?;
    let (value_len, value_bytes) =
        read_varint(entries, value_start, "value length").map_err(data_corruption)?;
    let suffix_start = checked_add(value_start, value_bytes, "key suffix cursor")?;

    let shared =
        usize::try_from(shared).map_err(|_| data_corruption("shared key length exceeds usize"))?;
    let unshared = usize::try_from(unshared)
        .map_err(|_| data_corruption("unshared key length exceeds usize"))?;
    let value_len =
        usize::try_from(value_len).map_err(|_| data_corruption("value length exceeds usize"))?;
    if shared > previous_key.len() {
        return Err(data_corruption(format!(
            "shared key length {shared} exceeds previous key length {}",
            previous_key.len()
        )));
    }
    let suffix_end = checked_add(suffix_start, unshared, "key suffix end")?;
    let value_end = checked_add(suffix_end, value_len, "value end")?;
    let suffix = entries
        .get(suffix_start..suffix_end)
        .ok_or_else(|| data_corruption("key suffix extends beyond entry bytes"))?;
    let value = entries
        .get(suffix_end..value_end)
        .ok_or_else(|| data_corruption("value extends beyond entry bytes"))?;
    let key_capacity = shared
        .checked_add(unshared)
        .ok_or_else(|| data_corruption("reconstructed key length overflows usize"))?;
    let mut key = Vec::with_capacity(key_capacity);
    key.extend_from_slice(&previous_key[..shared]);
    key.extend_from_slice(suffix);
    Ok(DecodedEntry {
        shared,
        key,
        value: value.to_vec(),
        next: value_end,
    })
}

fn shared_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn checked_add(left: usize, right: usize, field: &'static str) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| data_corruption(format!("{field} overflows usize")))
}

fn length_error(field: &'static str) -> Error {
    Error::InvalidArgument(format!("{field} length exceeds u64"))
}

fn data_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable data block",
        detail: detail.into(),
    }
}
