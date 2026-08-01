/// One owned mutation in a [`WriteBatch`].
///
/// Keys and values are stored as owned byte vectors so a queued operation does
/// not borrow from, or observe later changes to, the caller's buffers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOp {
    /// Insert or replace a key with a value and optional expiration time.
    Put {
        /// The owned key bytes.
        key: Vec<u8>,
        /// The owned value bytes.
        value: Vec<u8>,
        /// Absolute expiration time in Unix milliseconds, or `None` to retain
        /// the value until it is overwritten or deleted.
        expires_at_unix_ms: Option<u64>,
    },
    /// Remove a key if it exists.
    Delete {
        /// The owned key bytes to remove.
        key: Vec<u8>,
    },
}

/// An ordered group of writes intended to be applied atomically.
///
/// Operations retain insertion order because later mutations to the same key
/// must observe earlier mutations in the batch. The batch also tracks payload
/// bytes so callers can enforce configured batch limits before submission.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteBatch {
    operations: Vec<WriteOp>,
    approximate_bytes: usize,
}

impl WriteBatch {
    /// Appends a non-expiring put and returns the batch for method chaining.
    ///
    /// Both inputs are copied immediately, allowing their source buffers to be
    /// changed or dropped after this call.
    ///
    /// # Panics
    ///
    /// Panics if the cumulative payload byte count exceeds [`usize::MAX`].
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> &mut Self {
        self.put_with_expiration(key, value, None)
    }

    /// Appends a put with optional expiration and returns the batch for chaining.
    ///
    /// `expires_at_unix_ms` is an absolute Unix timestamp in milliseconds.
    /// Passing `None` creates a value without an expiration deadline. Key and
    /// value bytes are copied into the batch.
    ///
    /// # Panics
    ///
    /// Panics if the cumulative payload byte count exceeds [`usize::MAX`].
    pub fn put_with_expiration(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        expires_at_unix_ms: Option<u64>,
    ) -> &mut Self {
        let key = key.as_ref();
        let value = value.as_ref();
        self.add_bytes(key.len());
        self.add_bytes(value.len());
        self.operations.push(WriteOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            expires_at_unix_ms,
        });
        self
    }

    /// Appends a delete and returns the batch for method chaining.
    ///
    /// The key is copied immediately, so the operation owns its input.
    ///
    /// # Panics
    ///
    /// Panics if the cumulative payload byte count exceeds [`usize::MAX`].
    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> &mut Self {
        let key = key.as_ref();
        self.add_bytes(key.len());
        self.operations.push(WriteOp::Delete { key: key.to_vec() });
        self
    }

    /// Returns the number of operations in the batch.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Returns `true` when the batch contains no operations.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Returns the combined key and value payload bytes in the batch.
    ///
    /// This deliberately excludes vector and enum overhead, so it is suitable
    /// for enforcing payload limits rather than estimating heap allocation.
    pub fn approximate_bytes(&self) -> usize {
        self.approximate_bytes
    }

    /// Borrows the operations in insertion order.
    ///
    /// The returned slice cannot reorder or mutate the batch.
    pub fn operations(&self) -> &[WriteOp] {
        &self.operations
    }

    /// Consumes the batch and returns its operations in insertion order.
    ///
    /// Consuming avoids cloning the owned key and value buffers.
    pub fn into_operations(self) -> Vec<WriteOp> {
        self.operations
    }

    fn add_bytes(&mut self, bytes: usize) {
        self.approximate_bytes = self
            .approximate_bytes
            .checked_add(bytes)
            .expect("write batch byte count overflow");
    }
}
