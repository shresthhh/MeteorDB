use crate::{Error, Result};

const CHECKSUM_MASK_DELTA: u32 = 0xa282_ead8;

/// Number of bytes after a stored block payload: one codec byte and a CRC32C.
pub const BLOCK_TRAILER_BYTES: usize = 5;
/// Codec marker for an uncompressed stored block.
pub const NO_COMPRESSION: u8 = 0;
/// Codec marker reserved for a Snappy-compressed stored block.
pub const SNAPPY_COMPRESSION: u8 = 1;
/// Version written into the fixed SSTable footer by complete table builders.
pub const SSTABLE_FORMAT_VERSION: u32 = 1;
/// Eight-byte identifier written into a complete SSTable footer.
pub const SSTABLE_MAGIC: [u8; 8] = *b"METEOR01";

/// Locates one encoded block within an SSTable file.
///
/// Offset and size use varints on disk: small values usually occupy fewer
/// bytes than fixed-width integers. Decoding still performs checked arithmetic
/// so corrupt lengths cannot wrap around into apparently valid slices.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockHandle {
    offset: u64,
    size: u64,
}

impl BlockHandle {
    /// Creates a handle for `size` bytes starting at `offset`.
    pub const fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    /// Returns the block's byte offset from the start of its file.
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Returns the encoded block size, excluding any surrounding metadata.
    pub const fn size(self) -> u64 {
        self.size
    }

    /// Encodes this handle as two consecutive unsigned varints.
    pub fn encode(self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(20);
        put_varint(&mut encoded, self.offset);
        put_varint(&mut encoded, self.size);
        encoded
    }

    /// Decodes a handle and returns it with the number of consumed bytes.
    ///
    /// Returning the consumed length lets a footer decoder continue with the
    /// next field without assuming varints have a fixed size.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] for truncated or overflowing varints.
    pub fn decode(encoded: &[u8]) -> Result<(Self, usize)> {
        let (offset, offset_bytes) =
            read_varint(encoded, 0, "block offset").map_err(handle_error)?;
        let (size, size_bytes) =
            read_varint(encoded, offset_bytes, "block size").map_err(handle_error)?;
        let consumed = offset_bytes
            .checked_add(size_bytes)
            .ok_or_else(|| handle_corruption("encoded length overflows usize"))?;
        offset
            .checked_add(size)
            .ok_or_else(|| handle_corruption("block offset plus size overflows u64"))?;
        Ok((Self { offset, size }, consumed))
    }
}

/// Appends a checksum trailer to a stored block payload.
///
/// The checksum covers both the payload and compression marker, preventing a
/// damaged marker from making readers choose the wrong codec. This function
/// frames bytes only; compression itself belongs to the complete SSTable task.
///
/// # Errors
///
/// Returns [`Error::InvalidArgument`] for an unknown compression marker or if
/// the output allocation length overflows.
pub fn encode_stored_block(payload: &[u8], compression: u8) -> Result<Vec<u8>> {
    validate_compression(compression).map_err(Error::InvalidArgument)?;
    let capacity = payload
        .len()
        .checked_add(BLOCK_TRAILER_BYTES)
        .ok_or_else(|| Error::InvalidArgument("stored block length overflows usize".to_owned()))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(payload);
    encoded.push(compression);
    encoded.extend_from_slice(&masked_checksum(payload, compression).to_le_bytes());
    Ok(encoded)
}

/// Verifies and separates a checksummed stored block.
///
/// # Errors
///
/// Returns [`Error::Corruption`] when the trailer is truncated, its codec is
/// unknown, or the stored checksum does not match the bytes.
pub fn decode_stored_block(encoded: &[u8]) -> Result<(Vec<u8>, u8)> {
    if encoded.len() < BLOCK_TRAILER_BYTES {
        return Err(block_corruption("stored block is shorter than its trailer"));
    }
    let payload_end = encoded.len() - BLOCK_TRAILER_BYTES;
    let payload = &encoded[..payload_end];
    let compression = encoded[payload_end];
    validate_compression(compression).map_err(block_corruption)?;
    let stored = u32::from_le_bytes(
        encoded[payload_end + 1..]
            .try_into()
            .expect("the checked trailer contains four checksum bytes"),
    );
    if unmask_checksum(stored) != checksum(payload, compression) {
        return Err(block_corruption("checksum mismatch"));
    }
    Ok((payload.to_vec(), compression))
}

pub(super) fn put_varint(encoded: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        encoded.push((value as u8) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

pub(super) fn read_varint(
    encoded: &[u8],
    start: usize,
    field: &'static str,
) -> std::result::Result<(u64, usize), String> {
    let mut value = 0_u64;
    for index in 0..10 {
        let position = start
            .checked_add(index)
            .ok_or_else(|| format!("{field} cursor overflows usize"))?;
        let byte = *encoded
            .get(position)
            .ok_or_else(|| format!("truncated {field} varint"))?;
        if index == 9 && byte > 1 {
            return Err(format!("{field} varint overflows u64"));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(format!("{field} varint exceeds ten bytes"))
}

fn validate_compression(compression: u8) -> std::result::Result<(), String> {
    match compression {
        NO_COMPRESSION | SNAPPY_COMPRESSION => Ok(()),
        _ => Err(format!("unknown compression marker {compression}")),
    }
}

fn checksum(payload: &[u8], compression: u8) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(payload), &[compression])
}

fn masked_checksum(payload: &[u8], compression: u8) -> u32 {
    checksum(payload, compression)
        .rotate_right(15)
        .wrapping_add(CHECKSUM_MASK_DELTA)
}

fn unmask_checksum(masked: u32) -> u32 {
    masked.wrapping_sub(CHECKSUM_MASK_DELTA).rotate_left(15)
}

fn handle_error(detail: String) -> Error {
    handle_corruption(detail)
}

fn handle_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable block handle",
        detail: detail.into(),
    }
}

fn block_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable block",
        detail: detail.into(),
    }
}
