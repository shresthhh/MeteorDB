use std::io::{BufReader, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::batch::{decode_batch, encode_batch};
use crate::{
    Durability, DurableFile, DurableFs, Error, OsDurableFs, Result, SequenceNumber, WriteBatch,
};

const BLOCK_BYTES: usize = 32 * 1024;
const HEADER_BYTES: usize = 7;
const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;
const CHECKSUM_MASK_DELTA: u32 = 0xa282_ead8;
const LOGICAL_HEADER_BYTES: usize = 1 + 8 + 4;
const MAX_OPERATION_OVERHEAD_BYTES: usize = 1 + 4 + 4 + 1 + 8;

/// One complete atomic write recovered from a write-ahead log.
///
/// A value appears only after replay has validated every physical fragment and
/// decoded every operation, so callers never observe half of a batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredBatch {
    /// Sequence number assigned when the batch was appended.
    pub sequence: SequenceNumber,
    /// Operations in their original insertion order.
    pub batch: WriteBatch,
}

/// Appends atomic, checksummed batches to one write-ahead-log segment.
///
/// Each batch is one *logical record*. Records larger than the space left in a
/// 32 KiB physical block are split into fragments. Fragmentation avoids
/// requiring one enormous contiguous disk write and lets recovery identify a
/// torn final write without exposing a partial batch.
///
/// # Stable storage format
///
/// Every physical fragment begins with seven bytes:
///
/// ```text
/// masked CRC32C:  u32 little-endian
/// payload length: u16 little-endian
/// fragment type:  u8 (FULL, FIRST, MIDDLE, or LAST)
/// ```
///
/// The checksum covers the fragment type followed by its payload. It is
/// rotated and offset before storage, a conventional CRC "mask" that prevents
/// common raw CRC patterns from recurring unchanged when checksummed data
/// contains other checksums.
///
/// Reassembling the physical fragments produces this logical batch:
///
/// ```text
/// format version:  u8
/// sequence:        u64 little-endian
/// operation count: u32 little-endian
/// operations...
///
/// put:    tag=1, key length u32, value length u32,
///         expiration marker u8, optional expiration u64, key, value
/// delete: tag=2, key length u32, key
/// ```
///
/// All multi-byte integers in this WAL use little-endian order: the least
/// significant byte is written first. Lengths are checked against remaining
/// input before keys or values are copied, so corrupt lengths cannot request
/// an allocation larger than the validated record.
pub struct WalWriter {
    file: Box<dyn DurableFile>,
    path: PathBuf,
    max_batch_bytes: usize,
    max_logical_record_bytes: usize,
    block_offset: usize,
}

impl WalWriter {
    /// Exclusively creates a new WAL segment.
    ///
    /// `max_batch_bytes` limits the combined key and value payload accepted by
    /// [`WalWriter::append`]. It must be nonzero. The same value must be passed
    /// to [`replay_wal`] so writing and recovery enforce identical payload and
    /// checked encoded-overhead limits. If `path` already exists, creation
    /// fails without modifying its bytes because recovery is not implemented.
    pub fn create(path: impl AsRef<Path>, max_batch_bytes: usize) -> Result<Self> {
        Self::create_with_fs(path, max_batch_bytes, Arc::new(OsDurableFs))
    }

    /// Creates a writer using a replaceable durable-filesystem implementation.
    ///
    /// Production callers normally use [`WalWriter::create`]. Supplying the
    /// trait explicitly is useful for crash-injection tests that need to fail
    /// an append or count synchronization calls deterministically.
    pub fn create_with_fs(
        path: impl AsRef<Path>,
        max_batch_bytes: usize,
        fs: Arc<dyn DurableFs>,
    ) -> Result<Self> {
        let max_logical_record_bytes = encoded_record_limit(max_batch_bytes)?;
        let path = path.as_ref().to_path_buf();
        let file = fs
            .create(&path)
            .map_err(|source| io_error("create new WAL", &path, source))?;
        let directory = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs.sync_directory(directory)
            .map_err(|source| io_error("sync WAL directory", directory, source))?;
        Ok(Self {
            file,
            path,
            max_batch_bytes,
            max_logical_record_bytes,
            block_offset: 0,
        })
    }

    /// Appends one batch as an indivisible logical WAL record.
    ///
    /// Empty batches and payloads above the configured limit are rejected
    /// before bytes are written. [`Durability::Sync`] synchronizes the file
    /// before returning; [`Durability::Buffered`] only hands bytes to the
    /// operating system, so a power loss may discard the acknowledged batch
    /// until [`WalWriter::sync`] succeeds.
    pub fn append(
        &mut self,
        sequence: SequenceNumber,
        batch: &WriteBatch,
        durability: Durability,
    ) -> Result<()> {
        if batch.is_empty() {
            return Err(Error::InvalidArgument(
                "cannot append an empty write batch".into(),
            ));
        }
        if batch.approximate_bytes() > self.max_batch_bytes {
            return Err(Error::InvalidArgument(format!(
                "write batch payload {} exceeds max_batch_bytes {}",
                batch.approximate_bytes(),
                self.max_batch_bytes
            )));
        }
        let logical = encode_batch(sequence, batch)?;
        if logical.len() > self.max_logical_record_bytes {
            return Err(Error::InvalidArgument(format!(
                "encoded write batch {} exceeds the checked WAL limit {} derived from \
                 max_batch_bytes {}",
                logical.len(),
                self.max_logical_record_bytes,
                self.max_batch_bytes
            )));
        }
        self.write_logical_record(&logical)?;
        if durability == Durability::Sync {
            self.sync()?;
        }
        Ok(())
    }

    /// Synchronizes all previously appended bytes to stable storage.
    ///
    /// This upgrades earlier buffered appends to the same file-level durability
    /// guarantee used by synchronous appends.
    pub fn sync(&self) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|source| io_error("sync WAL", &self.path, source))
    }

    fn write_logical_record(&mut self, logical: &[u8]) -> Result<()> {
        let mut position = 0;
        let mut first = true;
        while position < logical.len() {
            let remaining_in_block = BLOCK_BYTES - self.block_offset;
            if remaining_in_block < HEADER_BYTES {
                let padding = [0; HEADER_BYTES - 1];
                self.file
                    .write_all(&padding[..remaining_in_block])
                    .map_err(|source| io_error("pad WAL block", &self.path, source))?;
                self.block_offset = 0;
            }

            let available = BLOCK_BYTES - self.block_offset - HEADER_BYTES;
            let fragment_length = available.min(logical.len() - position);
            let last = position + fragment_length == logical.len();
            let fragment_type = match (first, last) {
                (true, true) => FULL,
                (true, false) => FIRST,
                (false, true) => LAST,
                (false, false) => MIDDLE,
            };
            let fragment = &logical[position..position + fragment_length];
            let checksum = masked_checksum(fragment_type, fragment);
            let length = u16::try_from(fragment_length).expect("fragment fits in a WAL block");
            let mut header = [0; HEADER_BYTES];
            header[..4].copy_from_slice(&checksum.to_le_bytes());
            header[4..6].copy_from_slice(&length.to_le_bytes());
            header[6] = fragment_type;
            self.file
                .write_all(&header)
                .and_then(|()| self.file.write_all(fragment))
                .map_err(|source| io_error("append WAL", &self.path, source))?;
            self.block_offset += HEADER_BYTES + fragment_length;
            position += fragment_length;
            first = false;
        }
        Ok(())
    }
}

/// Replays every complete, valid batch in a WAL segment.
///
/// A structurally short final header or payload, or an unfinished final
/// FIRST/MIDDLE chain, is treated as a torn tail and ignored. A checksum
/// mismatch is always [`Error::Corruption`], including in the final fragment,
/// because complete bytes with an invalid checksum are damage rather than
/// structural truncation.
///
/// `max_batch_bytes` must match the value used by [`WalWriter`]. Recovery uses
/// it both to reject decoded payloads that are too large and to derive a
/// checked encoded-record ceiling before accumulating fragments. Checksums are
/// stored in a masked form so their on-disk bytes are decorrelated from common
/// CRC values.
pub fn replay_wal(path: impl AsRef<Path>, max_batch_bytes: usize) -> Result<Vec<RecoveredBatch>> {
    replay_wal_with_fs(path, max_batch_bytes, Arc::new(OsDurableFs))
}

/// Replays a WAL through the no-follow durable-filesystem abstraction.
pub fn replay_wal_with_fs(
    path: impl AsRef<Path>,
    max_batch_bytes: usize,
    fs: Arc<dyn DurableFs>,
) -> Result<Vec<RecoveredBatch>> {
    let max_logical_record_bytes = encoded_record_limit(max_batch_bytes)?;
    let path = path.as_ref();
    let contents = fs
        .read_file(path)
        .map_err(|source| io_error("read WAL", path, source))?;
    let file_length =
        u64::try_from(contents.len()).map_err(|_| wal_corruption("WAL length exceeds u64"))?;
    let mut reader = BufReader::new(Cursor::new(contents));
    let mut block = [0_u8; BLOCK_BYTES];
    let mut block_start = 0_u64;
    let mut logical = Vec::new();
    let mut assembling = false;
    let mut recovered = Vec::new();

    loop {
        let block_length = read_block(&mut reader, &mut block)
            .map_err(|source| io_error("read WAL", path, source))?;
        if block_length == 0 {
            break;
        }
        let final_block = block_start + u64::try_from(block_length).expect("block length fits u64")
            == file_length;
        let mut offset = 0;
        while offset < block_length {
            let remaining = block_length - offset;
            if remaining < HEADER_BYTES {
                if final_block {
                    return Ok(recovered);
                }
                if block[offset..block_length].iter().any(|byte| *byte != 0) {
                    return Err(wal_corruption("nonzero bytes in physical block trailer"));
                }
                break;
            }

            let header = &block[offset..offset + HEADER_BYTES];
            let stored_checksum = u32::from_le_bytes(header[..4].try_into().expect("four bytes"));
            let fragment_length = usize::from(u16::from_le_bytes(
                header[4..6].try_into().expect("two bytes"),
            ));
            let fragment_type = header[6];
            let fragment_end = offset
                .checked_add(HEADER_BYTES)
                .and_then(|position| position.checked_add(fragment_length))
                .ok_or_else(|| wal_corruption("physical fragment length overflow"))?;
            if fragment_end > block_length {
                if final_block {
                    return Ok(recovered);
                }
                return Err(wal_corruption(format!(
                    "physical fragment length {fragment_length} crosses a block boundary"
                )));
            }
            let fragment = &block[offset + HEADER_BYTES..fragment_end];
            if unmask_checksum(stored_checksum) != checksum(fragment_type, fragment) {
                return Err(wal_corruption("physical fragment checksum mismatch"));
            }

            match fragment_type {
                FULL if !assembling => decode_record(
                    fragment,
                    &mut recovered,
                    max_batch_bytes,
                    max_logical_record_bytes,
                )?,
                FIRST if !assembling => {
                    logical.clear();
                    append_fragment(&mut logical, fragment, max_logical_record_bytes)?;
                    assembling = true;
                }
                MIDDLE if assembling => {
                    append_fragment(&mut logical, fragment, max_logical_record_bytes)?
                }
                LAST if assembling => {
                    append_fragment(&mut logical, fragment, max_logical_record_bytes)?;
                    decode_record(
                        &logical,
                        &mut recovered,
                        max_batch_bytes,
                        max_logical_record_bytes,
                    )?;
                    logical.clear();
                    assembling = false;
                }
                FULL => {
                    return Err(wal_corruption(
                        "FULL fragment encountered before the previous record ended",
                    ));
                }
                FIRST => {
                    return Err(wal_corruption(
                        "FIRST fragment encountered before the previous record ended",
                    ));
                }
                MIDDLE | LAST => {
                    return Err(wal_corruption(format!(
                        "fragment type {fragment_type} has no preceding FIRST fragment"
                    )));
                }
                _ => {
                    return Err(wal_corruption(format!(
                        "unknown physical fragment type {fragment_type}"
                    )));
                }
            }
            offset = fragment_end;
        }
        block_start += u64::try_from(block_length).expect("block length fits u64");
        if final_block {
            break;
        }
    }
    Ok(recovered)
}

fn read_block<R: Read>(
    reader: &mut BufReader<R>,
    block: &mut [u8; BLOCK_BYTES],
) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < block.len() {
        let read = reader.read(&mut block[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

fn append_fragment(
    logical: &mut Vec<u8>,
    fragment: &[u8],
    max_logical_record_bytes: usize,
) -> Result<()> {
    let new_length = logical
        .len()
        .checked_add(fragment.len())
        .filter(|length| *length <= max_logical_record_bytes)
        .ok_or_else(|| {
            wal_corruption(format!(
                "logical record exceeds the {max_logical_record_bytes}-byte limit derived from \
                 max_batch_bytes"
            ))
        })?;
    logical.reserve(new_length - logical.len());
    logical.extend_from_slice(fragment);
    Ok(())
}

fn decode_record(
    encoded: &[u8],
    recovered: &mut Vec<RecoveredBatch>,
    max_batch_bytes: usize,
    max_logical_record_bytes: usize,
) -> Result<()> {
    if encoded.len() > max_logical_record_bytes {
        return Err(wal_corruption(format!(
            "logical record exceeds the {max_logical_record_bytes}-byte limit derived from \
             max_batch_bytes"
        )));
    }
    let (sequence, batch) = decode_batch(encoded)?;
    if batch.approximate_bytes() > max_batch_bytes {
        return Err(wal_corruption(format!(
            "write batch payload {} exceeds max_batch_bytes {max_batch_bytes}",
            batch.approximate_bytes()
        )));
    }
    recovered.push(RecoveredBatch { sequence, batch });
    Ok(())
}

fn encoded_record_limit(max_batch_bytes: usize) -> Result<usize> {
    if max_batch_bytes == 0 {
        return Err(Error::InvalidArgument(
            "max_batch_bytes must be greater than zero".into(),
        ));
    }
    max_batch_bytes
        .checked_mul(MAX_OPERATION_OVERHEAD_BYTES)
        .and_then(|overhead| overhead.checked_add(max_batch_bytes))
        .and_then(|with_payload| with_payload.checked_add(LOGICAL_HEADER_BYTES))
        .ok_or_else(|| {
            Error::InvalidArgument(
                "max_batch_bytes causes the encoded WAL record limit to overflow".into(),
            )
        })
}

fn checksum(fragment_type: u8, fragment: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&[fragment_type]), fragment)
}

fn masked_checksum(fragment_type: u8, fragment: &[u8]) -> u32 {
    checksum(fragment_type, fragment)
        .rotate_right(15)
        .wrapping_add(CHECKSUM_MASK_DELTA)
}

fn unmask_checksum(masked: u32) -> u32 {
    masked.wrapping_sub(CHECKSUM_MASK_DELTA).rotate_left(15)
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn wal_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "WAL",
        detail: detail.into(),
    }
}
