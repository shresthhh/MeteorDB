use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::sstable::builder::TableProperties;
use crate::sstable::format::read_varint;
use crate::sstable::{
    Block, BlockHandle, NO_COMPRESSION, SNAPPY_COMPRESSION, SSTABLE_FOOTER_BYTES,
    SSTABLE_FORMAT_VERSION, SSTABLE_MAGIC, decode_stored_block,
};
use crate::{BloomFilter, Compression, Error, InternalKey, Result};

/// Conservative default ceiling for one uncompressed SSTable data block.
pub const DEFAULT_MAX_UNCOMPRESSED_DATA_BLOCK_BYTES: usize = 64 * 1024 * 1024;

/// Trusted resource limits applied while opening and reading an SSTable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableReaderOptions {
    /// Maximum accepted uncompressed bytes in one data block.
    ///
    /// This limit comes from the caller, not the file. It bounds stored-block
    /// reads and Snappy output before allocation or decompression.
    pub max_uncompressed_data_block_bytes: usize,
}

impl Default for TableReaderOptions {
    fn default() -> Self {
        Self {
            max_uncompressed_data_block_bytes: DEFAULT_MAX_UNCOMPRESSED_DATA_BLOCK_BYTES,
        }
    }
}

/// Open immutable table with eagerly checked metadata and lazily read data blocks.
///
/// Opening validates the fixed footer, metadata handles, metadata checksums,
/// index structure, Bloom filter, and properties. Data-block payloads are not
/// read until `get` or iteration needs them, keeping open cost independent of
/// table data size and allowing a Bloom negative to avoid data I/O entirely.
pub struct TableReader {
    path: PathBuf,
    file: Mutex<File>,
    file_size: u64,
    data_end: u64,
    index: Vec<(Vec<u8>, BlockHandle)>,
    filter: BloomFilter,
    properties: TableProperties,
    max_uncompressed_data_block_bytes: usize,
}

impl TableReader {
    /// Opens and validates an immutable SSTable's footer and metadata blocks.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] for filesystem failures, [`Error::Corruption`] for
    /// malformed/checksum-invalid structure, or [`Error::UnsupportedFormat`]
    /// when the magic is recognized but the footer version is not `1`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, TableReaderOptions::default())
    }

    /// Opens an SSTable with caller-trusted data-block resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] for a zero limit and otherwise the
    /// same errors as [`TableReader::open`].
    pub fn open_with_options(path: impl AsRef<Path>, options: TableReaderOptions) -> Result<Self> {
        if options.max_uncompressed_data_block_bytes == 0 {
            return Err(Error::InvalidArgument(
                "max_uncompressed_data_block_bytes must be greater than zero".to_owned(),
            ));
        }
        let path = path.as_ref().to_path_buf();
        let mut file =
            File::open(&path).map_err(|source| io_error("open SSTable", &path, source))?;
        let file_size = file
            .metadata()
            .map_err(|source| io_error("stat SSTable", &path, source))?
            .len();
        let footer_size = u64::try_from(SSTABLE_FOOTER_BYTES).expect("footer size fits u64");
        let footer_start = file_size
            .checked_sub(footer_size)
            .ok_or_else(|| footer_corruption("file is shorter than the fixed footer"))?;
        file.seek(SeekFrom::Start(footer_start))
            .map_err(|source| io_error("seek SSTable footer", &path, source))?;
        let mut footer = [0; SSTABLE_FOOTER_BYTES];
        file.read_exact(&mut footer)
            .map_err(|source| io_error("read SSTable footer", &path, source))?;
        if footer[SSTABLE_FOOTER_BYTES - SSTABLE_MAGIC.len()..] != SSTABLE_MAGIC {
            return Err(footer_corruption("magic is not METEOR01"));
        }
        let version_start = SSTABLE_FOOTER_BYTES - SSTABLE_MAGIC.len() - 4;
        let version = u32::from_le_bytes(
            footer[version_start..version_start + 4]
                .try_into()
                .expect("fixed footer has four version bytes"),
        );
        if version != SSTABLE_FORMAT_VERSION {
            return Err(Error::UnsupportedFormat {
                kind: "SSTable",
                version,
            });
        }
        let index_handle = decode_fixed_handle(&footer, 0, footer_start)?;
        let filter_handle = decode_fixed_handle(&footer, 20, footer_start)?;
        let properties_handle = decode_fixed_handle(&footer, 40, footer_start)?;
        validate_metadata_handles(index_handle, filter_handle, properties_handle, footer_start)?;

        let index_payload = read_metadata_block(&mut file, &path, index_handle)?;
        let filter_payload = read_metadata_block(&mut file, &path, filter_handle)?;
        let properties_payload = read_metadata_block(&mut file, &path, properties_handle)?;
        let index_block = Block::decode(index_payload)?;
        let mut index = Vec::with_capacity(index_block.len());
        let mut previous_end = 0_u64;
        for entry in index_block.iter() {
            let (separator, encoded_handle) = entry?;
            let (handle, consumed) = BlockHandle::decode(&encoded_handle)?;
            if consumed != encoded_handle.len() {
                return Err(index_corruption("trailing bytes after data-block handle"));
            }
            validate_handle(handle, filter_handle.offset(), "data block")?;
            if handle.offset() != previous_end {
                return Err(index_corruption(
                    "data-block handles are not contiguous and in increasing order",
                ));
            }
            previous_end = handle
                .offset()
                .checked_add(handle.size())
                .ok_or_else(|| index_corruption("data-block range overflows u64"))?;
            index.push((separator, handle));
        }
        if index.is_empty() {
            return Err(index_corruption("index contains no data blocks"));
        }
        if previous_end != filter_handle.offset() {
            return Err(index_corruption(
                "final data block is not immediately followed by the filter block",
            ));
        }
        let filter = BloomFilter::decode(filter_payload)?;
        let properties = decode_properties(&properties_payload)?;
        let trusted_maximum =
            u64::try_from(options.max_uncompressed_data_block_bytes).map_err(|_| {
                Error::InvalidArgument("reader data-block limit exceeds u64".to_owned())
            })?;
        if properties.max_data_block_bytes > trusted_maximum {
            return Err(properties_corruption(format!(
                "declared maximum data-block bytes {} exceeds reader limit {trusted_maximum}",
                properties.max_data_block_bytes
            )));
        }
        if properties.data_blocks
            != u64::try_from(index.len())
                .map_err(|_| index_corruption("index entry count exceeds u64"))?
        {
            return Err(properties_corruption(
                "data-block count does not match the index",
            ));
        }

        Ok(Self {
            path,
            file: Mutex::new(file),
            file_size,
            data_end: filter_handle.offset(),
            index,
            filter,
            properties,
            max_uncompressed_data_block_bytes: options.max_uncompressed_data_block_bytes,
        })
    }

    /// Returns eagerly decoded table metadata.
    pub fn properties(&self) -> &TableProperties {
        &self.properties
    }

    /// Returns the complete immutable file length.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Reports the Bloom filter's answer for one exact internal key.
    ///
    /// `false` is a definite miss and means `get` will not read a data block.
    /// `true` remains probabilistic and must be verified against table data.
    pub fn may_contain(&self, key: &InternalKey) -> bool {
        self.filter.may_contain(key.as_bytes())
    }

    /// Looks up one exact internal key.
    ///
    /// The Bloom filter runs first. On a possible match, the index separator
    /// selects one block, whose checksum and compression are validated only
    /// then. A lower-bound seek must still equal the requested bytes because a
    /// Bloom positive may be false.
    ///
    /// # Errors
    ///
    /// Returns I/O or typed corruption errors from the lazily read data block.
    pub fn get(&self, key: &InternalKey) -> Result<Option<Vec<u8>>> {
        if !self.may_contain(key) {
            return Ok(None);
        }
        let Some((_, handle)) = self
            .index
            .iter()
            .find(|(separator, _)| separator.as_slice() >= key.as_bytes())
        else {
            return Ok(None);
        };
        let block = self.read_data_block(*handle)?;
        match block.seek(key.as_bytes())? {
            Some((found, value)) if found == key.as_bytes() => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    /// Returns a lazy forward iterator across all internal records.
    ///
    /// Index/filter/properties bytes are already resident, but each data block
    /// is read, checksummed, decompressed, decoded, and then discarded before
    /// advancing to the next one.
    pub fn iter(&self) -> TableIter<'_> {
        TableIter {
            reader: self,
            block_index: 0,
            entries: Vec::new().into_iter(),
            failed: false,
        }
    }

    fn read_data_block(&self, handle: BlockHandle) -> Result<Block> {
        validate_handle(handle, self.data_end, "data block")?;
        let maximum_stored =
            maximum_stored_data_block_bytes(self.max_uncompressed_data_block_bytes)?;
        if handle.size() > maximum_stored {
            return Err(data_corruption(format!(
                "stored block bytes {} exceeds reader allocation limit {maximum_stored}",
                handle.size()
            )));
        }
        let mut file = self
            .file
            .lock()
            .map_err(|_| Error::Background("SSTable file lock was poisoned".to_owned()))?;
        let encoded = read_exact_range(&mut file, &self.path, handle, "data block")?;
        let (payload, marker) = decode_stored_block(&encoded)?;
        let expected_marker = match self.properties.compression {
            Compression::None => NO_COMPRESSION,
            Compression::Snappy => SNAPPY_COMPRESSION,
        };
        if marker != expected_marker {
            return Err(data_corruption(format!(
                "compression marker {marker} does not match table property {expected_marker}"
            )));
        }
        let decoded = match marker {
            NO_COMPRESSION => {
                if payload.len() > self.max_uncompressed_data_block_bytes {
                    return Err(data_corruption(format!(
                        "uncompressed block bytes {} exceeds reader limit {}",
                        payload.len(),
                        self.max_uncompressed_data_block_bytes
                    )));
                }
                payload
            }
            SNAPPY_COMPRESSION => {
                let expected = snap::raw::decompress_len(&payload)
                    .map_err(|error| data_corruption(format!("invalid Snappy header: {error}")))?;
                let property_maximum = usize::try_from(self.properties.max_data_block_bytes)
                    .map_err(|_| data_corruption("maximum data-block size exceeds usize"))?;
                if expected > self.max_uncompressed_data_block_bytes {
                    return Err(data_corruption(format!(
                        "Snappy output length {expected} exceeds reader limit {}",
                        self.max_uncompressed_data_block_bytes
                    )));
                }
                if expected > property_maximum {
                    return Err(data_corruption(format!(
                        "Snappy output length {expected} exceeds property maximum {property_maximum}"
                    )));
                }
                snap::raw::Decoder::new()
                    .decompress_vec(&payload)
                    .map_err(|error| {
                        data_corruption(format!("Snappy decompression failed: {error}"))
                    })?
            }

            _ => unreachable!("stored-block decoding validates compression markers"),
        };
        Block::decode(decoded)
    }
}

fn maximum_stored_data_block_bytes(uncompressed_limit: usize) -> Result<u64> {
    let maximum = uncompressed_limit
        .checked_add(32)
        .and_then(|bytes| bytes.checked_add(uncompressed_limit / 6))
        .and_then(|bytes| bytes.checked_add(crate::sstable::BLOCK_TRAILER_BYTES))
        .ok_or_else(|| Error::InvalidArgument("reader data-block limit overflows usize".into()))?;
    u64::try_from(maximum)
        .map_err(|_| Error::InvalidArgument("reader data-block limit exceeds u64".into()))
}

/// Lazy forward iterator over every internal key/value pair in a table.
///
/// Items own their bytes so callers can retain them after the iterator advances.
/// Any I/O, checksum, compression, block, or internal-key decoding failure is
/// yielded once and then terminates iteration; corruption is never mistaken
/// for ordinary end-of-table.
pub struct TableIter<'a> {
    reader: &'a TableReader,
    block_index: usize,
    entries: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    failed: bool,
}

impl Iterator for TableIter<'_> {
    type Item = Result<(InternalKey, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            if let Some((key, value)) = self.entries.next() {
                return Some(match InternalKey::decode(key) {
                    Ok(key) => Ok((key, value)),
                    Err(error) => {
                        self.failed = true;
                        Err(error)
                    }
                });
            }
            let (_, handle) = self.reader.index.get(self.block_index)?;
            self.block_index += 1;
            match self.reader.read_data_block(*handle) {
                Ok(block) => match block.iter().collect::<Result<Vec<_>>>() {
                    Ok(entries) => self.entries = entries.into_iter(),
                    Err(error) => {
                        self.failed = true;
                        return Some(Err(error));
                    }
                },
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

fn decode_fixed_handle(encoded: &[u8], start: usize, limit: u64) -> Result<BlockHandle> {
    let end = start
        .checked_add(20)
        .ok_or_else(|| footer_corruption("handle cursor overflows usize"))?;
    let bytes = encoded
        .get(start..end)
        .ok_or_else(|| footer_corruption("truncated fixed block handle"))?;
    let (handle, consumed) =
        BlockHandle::decode(bytes).map_err(|error| footer_corruption(error.to_string()))?;
    if bytes[consumed..].iter().any(|&byte| byte != 0) {
        return Err(footer_corruption("nonzero block-handle slot padding"));
    }
    validate_handle(handle, limit, "metadata block")?;
    Ok(handle)
}

fn validate_metadata_handles(
    index: BlockHandle,
    filter: BlockHandle,
    properties: BlockHandle,
    footer_start: u64,
) -> Result<()> {
    let filter_end = checked_handle_end(filter, "filter")?;
    let index_end = checked_handle_end(index, "index")?;
    let properties_end = checked_handle_end(properties, "properties")?;
    if filter_end != index.offset()
        || index_end != properties.offset()
        || properties_end != footer_start
    {
        return Err(footer_corruption(
            "metadata blocks do not follow canonical filter-index-properties-footer layout",
        ));
    }
    Ok(())
}

fn checked_handle_end(handle: BlockHandle, kind: &'static str) -> Result<u64> {
    handle
        .offset()
        .checked_add(handle.size())
        .ok_or_else(|| footer_corruption(format!("{kind} block range overflows u64")))
}

fn validate_handle(handle: BlockHandle, limit: u64, kind: &'static str) -> Result<()> {
    if handle.size() == 0 {
        return Err(footer_corruption(format!("{kind} handle has zero size")));
    }
    let end = handle
        .offset()
        .checked_add(handle.size())
        .ok_or_else(|| footer_corruption(format!("{kind} range overflows u64")))?;
    if end > limit {
        return Err(footer_corruption(format!(
            "{kind} range {start}..{end} exceeds limit {limit}",
            start = handle.offset()
        )));
    }
    Ok(())
}

fn read_metadata_block(file: &mut File, path: &Path, handle: BlockHandle) -> Result<Vec<u8>> {
    let encoded = read_exact_range(file, path, handle, "metadata block")?;
    let (payload, marker) = decode_stored_block(&encoded)?;
    if marker != NO_COMPRESSION {
        return Err(footer_corruption("metadata blocks must not be compressed"));
    }
    Ok(payload)
}

fn read_exact_range(
    file: &mut File,
    path: &Path,
    handle: BlockHandle,
    kind: &'static str,
) -> Result<Vec<u8>> {
    let length = usize::try_from(handle.size())
        .map_err(|_| footer_corruption(format!("{kind} size exceeds usize")))?;
    let mut encoded = vec![0; length];
    file.seek(SeekFrom::Start(handle.offset()))
        .map_err(|source| io_error("seek SSTable block", path, source))?;
    file.read_exact(&mut encoded)
        .map_err(|source| io_error("read SSTable block", path, source))?;
    Ok(encoded)
}

fn decode_properties(encoded: &[u8]) -> Result<TableProperties> {
    let mut cursor = 0;
    let file_number = read_property_varint(encoded, &mut cursor, "file number")?;
    let entries = read_property_varint(encoded, &mut cursor, "entry count")?;
    let data_blocks = read_property_varint(encoded, &mut cursor, "data block count")?;
    let compression = match *encoded
        .get(cursor)
        .ok_or_else(|| properties_corruption("missing compression marker"))?
    {
        NO_COMPRESSION => Compression::None,
        SNAPPY_COMPRESSION => Compression::Snappy,
        marker => {
            return Err(properties_corruption(format!(
                "unknown compression marker {marker}"
            )));
        }
    };
    cursor = cursor
        .checked_add(1)
        .ok_or_else(|| properties_corruption("compression cursor overflows usize"))?;
    let max_data_block_bytes =
        read_property_varint(encoded, &mut cursor, "maximum data block bytes")?;
    let smallest = read_property_key(encoded, &mut cursor, "smallest key")?;
    let largest = read_property_key(encoded, &mut cursor, "largest key")?;
    if cursor != encoded.len() {
        return Err(properties_corruption("trailing bytes after properties"));
    }
    if smallest > largest {
        return Err(properties_corruption(
            "smallest internal key is greater than largest",
        ));
    }
    if entries == 0 || data_blocks == 0 || max_data_block_bytes == 0 {
        return Err(properties_corruption(
            "entry, block, and maximum block counts must be nonzero",
        ));
    }
    Ok(TableProperties {
        file_number,
        entries,
        data_blocks,
        compression,
        smallest,
        largest,
        max_data_block_bytes,
    })
}

fn read_property_varint(encoded: &[u8], cursor: &mut usize, field: &'static str) -> Result<u64> {
    let (value, consumed) = read_varint(encoded, *cursor, field).map_err(properties_corruption)?;
    *cursor = cursor
        .checked_add(consumed)
        .ok_or_else(|| properties_corruption(format!("{field} cursor overflows usize")))?;
    Ok(value)
}

fn read_property_key(
    encoded: &[u8],
    cursor: &mut usize,
    field: &'static str,
) -> Result<InternalKey> {
    let length = read_property_varint(encoded, cursor, field)?;
    let length = usize::try_from(length)
        .map_err(|_| properties_corruption(format!("{field} length exceeds usize")))?;
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| properties_corruption(format!("{field} end overflows usize")))?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or_else(|| properties_corruption(format!("{field} extends beyond properties")))?;
    *cursor = end;
    InternalKey::decode(bytes).map_err(|error| properties_corruption(error.to_string()))
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn footer_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable footer",
        detail: detail.into(),
    }
}

fn index_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable index",
        detail: detail.into(),
    }
}

fn properties_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable properties",
        detail: detail.into(),
    }
}

fn data_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable data block",
        detail: detail.into(),
    }
}
