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

pub(crate) const BATCH_FORMAT_VERSION: u8 = 1;
const PUT_TAG: u8 = 1;
const DELETE_TAG: u8 = 2;

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

pub(crate) fn encode_batch(sequence: u64, batch: &WriteBatch) -> crate::Result<Vec<u8>> {
    let operation_count = u32::try_from(batch.len()).map_err(|_| {
        crate::Error::InvalidArgument("write batch contains more than u32::MAX operations".into())
    })?;
    let mut encoded = Vec::new();
    encoded.push(BATCH_FORMAT_VERSION);
    encoded.extend_from_slice(&sequence.to_le_bytes());
    encoded.extend_from_slice(&operation_count.to_le_bytes());

    for operation in batch.operations() {
        match operation {
            WriteOp::Put {
                key,
                value,
                expires_at_unix_ms,
            } => {
                encoded.push(PUT_TAG);
                encode_length(&mut encoded, "key", key.len())?;
                encode_length(&mut encoded, "value", value.len())?;
                match expires_at_unix_ms {
                    Some(expiration) => {
                        encoded.push(1);
                        encoded.extend_from_slice(&expiration.to_le_bytes());
                    }
                    None => encoded.push(0),
                }
                encoded.extend_from_slice(key);
                encoded.extend_from_slice(value);
            }
            WriteOp::Delete { key } => {
                encoded.push(DELETE_TAG);
                encode_length(&mut encoded, "key", key.len())?;
                encoded.extend_from_slice(key);
            }
        }
    }
    Ok(encoded)
}

pub(crate) fn decode_batch(encoded: &[u8]) -> crate::Result<(u64, WriteBatch)> {
    let mut decoder = BatchDecoder::new(encoded);
    let version = decoder.read_u8("format version")?;
    if version != BATCH_FORMAT_VERSION {
        return Err(crate::Error::UnsupportedFormat {
            kind: "WAL batch",
            version: u32::from(version),
        });
    }
    let sequence = decoder.read_u64("sequence")?;
    let operation_count = decoder.read_u32("operation count")?;
    if operation_count == 0 {
        return Err(corruption("batch contains no operations"));
    }

    let mut batch = WriteBatch::default();
    for _ in 0..operation_count {
        match decoder.read_u8("operation tag")? {
            PUT_TAG => {
                let key_length = decoder.read_length("put key length")?;
                let value_length = decoder.read_length("put value length")?;
                let expiration = match decoder.read_u8("expiration marker")? {
                    0 => None,
                    1 => Some(decoder.read_u64("expiration timestamp")?),
                    marker => {
                        return Err(corruption(format!("unknown expiration marker {marker}")));
                    }
                };
                let key = decoder.read_bytes("put key", key_length)?;
                let value = decoder.read_bytes("put value", value_length)?;
                batch.put_with_expiration(key, value, expiration);
            }
            DELETE_TAG => {
                let key_length = decoder.read_length("delete key length")?;
                let key = decoder.read_bytes("delete key", key_length)?;
                batch.delete(key);
            }
            tag => return Err(corruption(format!("unknown operation tag {tag}"))),
        }
    }
    if decoder.remaining() != 0 {
        return Err(corruption(format!(
            "{} trailing bytes after write batch",
            decoder.remaining()
        )));
    }
    Ok((sequence, batch))
}

fn encode_length(encoded: &mut Vec<u8>, name: &'static str, length: usize) -> crate::Result<()> {
    let length = u32::try_from(length).map_err(|_| {
        crate::Error::InvalidArgument(format!("{name} length exceeds the WAL u32 limit"))
    })?;
    encoded.extend_from_slice(&length.to_le_bytes());
    Ok(())
}

struct BatchDecoder<'a> {
    encoded: &'a [u8],
    position: usize,
}

impl<'a> BatchDecoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self {
            encoded,
            position: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.encoded.len() - self.position
    }

    fn read_bytes(&mut self, field: &'static str, length: usize) -> crate::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.encoded.len())
            .ok_or_else(|| {
                corruption(format!(
                    "{field} declares {length} bytes with only {} remaining",
                    self.remaining()
                ))
            })?;
        let bytes = &self.encoded[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn read_u8(&mut self, field: &'static str) -> crate::Result<u8> {
        Ok(self.read_bytes(field, 1)?[0])
    }

    fn read_u32(&mut self, field: &'static str) -> crate::Result<u32> {
        let bytes: [u8; 4] = self
            .read_bytes(field, 4)?
            .try_into()
            .expect("fixed-size slice");
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self, field: &'static str) -> crate::Result<u64> {
        let bytes: [u8; 8] = self
            .read_bytes(field, 8)?
            .try_into()
            .expect("fixed-size slice");
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_length(&mut self, field: &'static str) -> crate::Result<usize> {
        usize::try_from(self.read_u32(field)?)
            .map_err(|_| corruption(format!("{field} does not fit this platform")))
    }
}

fn corruption(detail: impl Into<String>) -> crate::Error {
    crate::Error::Corruption {
        context: "WAL batch",
        detail: detail.into(),
    }
}
