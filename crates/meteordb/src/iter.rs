use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ops::Bound;
use std::sync::Arc;

use crate::{Error, InternalKey, Result, SequenceNumber, SnapshotGuard, ValueRecord, Version};

/// Owned lower and upper user-key bounds for a forward scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanBounds {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl ScanBounds {
    /// Creates bounds with standard [`Bound`] inclusive, exclusive, or unbounded semantics.
    pub fn new(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self {
        Self { start, end }
    }

    /// Creates a range containing every user key.
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Borrows the lower user-key bound.
    pub fn start(&self) -> Bound<&[u8]> {
        self.start.as_ref().map(Vec::as_slice)
    }

    /// Borrows the upper user-key bound.
    pub fn end(&self) -> Bound<&[u8]> {
        self.end.as_ref().map(Vec::as_slice)
    }
}

pub(crate) struct InternalEntry {
    pub(crate) key: InternalKey,
    pub(crate) record: ValueRecord,
}

pub(crate) type ChildIterator = Box<dyn Iterator<Item = Result<InternalEntry>>>;

struct HeapEntry {
    entry: InternalEntry,
    child: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key && self.child == other.child
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .entry
            .key
            .cmp(&self.entry.key)
            .then_with(|| other.child.cmp(&self.child))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct InternalMergingIterator {
    children: Vec<ChildIterator>,
    heap: BinaryHeap<HeapEntry>,
    failed: bool,
    pending_error: Option<Error>,
}

impl InternalMergingIterator {
    pub(crate) fn new(children: Vec<ChildIterator>) -> Self {
        let mut iterator = Self {
            heap: BinaryHeap::new(),
            children,
            failed: false,
            pending_error: None,
        };
        for child in 0..iterator.children.len() {
            if !iterator.advance_child(child) {
                break;
            }
        }
        iterator
    }

    fn advance_child(&mut self, child: usize) -> bool {
        match self.children[child].next() {
            Some(Ok(entry)) => {
                self.heap.push(HeapEntry { entry, child });
                true
            }
            Some(Err(error)) => {
                self.pending_error = Some(error);
                false
            }
            None => true,
        }
    }
}

impl Iterator for InternalMergingIterator {
    type Item = Result<InternalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Some(error) = self.pending_error.take() {
            self.failed = true;
            self.heap.clear();
            return Some(Err(error));
        }
        let next = self.heap.pop()?;
        self.advance_child(next.child);
        Some(Ok(next.entry))
    }
}

/// Lazy merged iterator yielding visible `(user_key, value)` pairs in key order.
///
/// Each item can fail because SSTable data blocks are checksummed and decoded
/// lazily. After yielding one error the iterator is exhausted.
pub struct KvIterator {
    merged: InternalMergingIterator,
    bounds: ScanBounds,
    sequence: SequenceNumber,
    read_time_unix_ms: u64,
    remaining: usize,
    failed: bool,
    pending: Option<InternalEntry>,
    _version: Option<Arc<Version>>,
    _snapshot_guard: Option<SnapshotGuard>,
    _reader_lease: Option<Arc<()>>,
}

impl KvIterator {
    pub(crate) fn empty(
        bounds: ScanBounds,
        sequence: SequenceNumber,
        read_time_unix_ms: u64,
    ) -> Self {
        Self {
            merged: InternalMergingIterator::new(Vec::new()),
            bounds,
            sequence,
            read_time_unix_ms,
            remaining: 0,
            failed: false,
            pending: None,
            _version: None,
            _snapshot_guard: None,
            _reader_lease: None,
        }
    }

    pub(crate) fn new(
        children: Vec<ChildIterator>,
        bounds: ScanBounds,
        sequence: SequenceNumber,
        read_time_unix_ms: u64,
        limit: usize,
        version: Arc<Version>,
        snapshot_guard: SnapshotGuard,
        reader_lease: Arc<()>,
    ) -> Self {
        Self {
            merged: InternalMergingIterator::new(children),
            bounds,
            sequence,
            read_time_unix_ms,
            remaining: limit,
            failed: false,
            pending: None,
            _version: Some(version),
            _snapshot_guard: Some(snapshot_guard),
            _reader_lease: Some(reader_lease),
        }
    }

    fn fail(&mut self, error: Error) -> Option<Result<(Vec<u8>, Vec<u8>)>> {
        self.failed = true;
        Some(Err(error))
    }
}

impl Iterator for KvIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.remaining == 0 {
            return None;
        }
        loop {
            let first = match self.pending.take().map(Ok).or_else(|| self.merged.next())? {
                Ok(entry) => entry,
                Err(error) => return self.fail(error),
            };
            let user_key = first.key.user_key().to_vec();
            let mut newest: Option<(SequenceNumber, ValueRecord)> = None;
            consider_visible(&mut newest, first, self.sequence);
            loop {
                match self.merged.next() {
                    Some(Ok(entry)) if entry.key.user_key() == user_key => {
                        consider_visible(&mut newest, entry, self.sequence);
                    }
                    Some(Ok(entry)) => {
                        self.pending = Some(entry);
                        break;
                    }
                    Some(Err(error)) => return self.fail(error),
                    None => break,
                }
            }

            if !lower_allows(&self.bounds, &user_key) {
                continue;
            }
            if !upper_allows(&self.bounds, &user_key) {
                return None;
            }

            let Some((_, record)) = newest else {
                continue;
            };
            match record {
                ValueRecord::Value {
                    value,
                    expires_at_unix_ms,
                } if expires_at_unix_ms.is_none_or(|expires| expires > self.read_time_unix_ms) => {
                    self.remaining -= 1;
                    return Some(Ok((user_key, value)));
                }
                ValueRecord::Value { .. } | ValueRecord::Tombstone => continue,
            }
        }
    }
}

fn consider_visible(
    newest: &mut Option<(SequenceNumber, ValueRecord)>,
    entry: InternalEntry,
    sequence: SequenceNumber,
) {
    let entry_sequence = entry.key.sequence();
    if entry_sequence <= sequence
        && newest
            .as_ref()
            .is_none_or(|(newest_sequence, _)| entry_sequence > *newest_sequence)
    {
        *newest = Some((entry_sequence, entry.record));
    }
}

pub(crate) fn disk_entry(
    key: InternalKey,
    value: Vec<u8>,
    engine_encoded: bool,
) -> Result<InternalEntry> {
    let record = ValueRecord::decode_sstable(key.kind(), value, engine_encoded)?;
    Ok(InternalEntry { key, record })
}

pub(crate) fn prefix_bounds(prefix: &[u8]) -> ScanBounds {
    let start = Bound::Included(prefix.to_vec());
    let end = match prefix_successor(prefix) {
        Some(successor) => Bound::Excluded(successor),
        None => Bound::Unbounded,
    };
    ScanBounds::new(start, end)
}

pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    let index = successor.iter().rposition(|&byte| byte != 0xff)?;
    successor[index] += 1;
    successor.truncate(index + 1);
    Some(successor)
}

pub(crate) fn overlaps_bounds(smallest: &[u8], largest: &[u8], bounds: &ScanBounds) -> bool {
    let starts_before_file_ends = match bounds.start() {
        Bound::Included(start) => start <= largest,
        Bound::Excluded(start) => start < largest,
        Bound::Unbounded => true,
    };
    let ends_after_file_starts = match bounds.end() {
        Bound::Included(end) => end >= smallest,
        Bound::Excluded(end) => end > smallest,
        Bound::Unbounded => true,
    };
    starts_before_file_ends && ends_after_file_starts
}

fn lower_allows(bounds: &ScanBounds, key: &[u8]) -> bool {
    match bounds.start() {
        Bound::Included(start) => key >= start,
        Bound::Excluded(start) => key > start,
        Bound::Unbounded => true,
    }
}

fn upper_allows(bounds: &ScanBounds, key: &[u8]) -> bool {
    match bounds.end() {
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
        Bound::Unbounded => true,
    }
}

pub(crate) fn user_key_in_bounds(bounds: &ScanBounds, key: &[u8]) -> bool {
    lower_allows(bounds, key) && upper_allows(bounds, key)
}
