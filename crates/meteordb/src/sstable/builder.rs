use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::sstable::format::put_varint;
use crate::sstable::{
    BlockBuilder, BlockHandle, NO_COMPRESSION, SNAPPY_COMPRESSION, SSTABLE_FOOTER_BYTES,
    SSTABLE_FORMAT_VERSION, SSTABLE_MAGIC, encode_stored_block,
};
use crate::{
    BloomFilter, Compression, DurableFile, DurableFs, Error, InternalKey, OsDurableFs, Result,
};

const FOOTER_HANDLE_BYTES: usize = 20;

/// Metadata stored in every complete immutable SSTable.
///
/// The smallest and largest values are full internal keys, not user keys.
/// Consequently, their byte order includes sequence-number and value-kind
/// ordering and can safely describe a table's exact on-disk range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableProperties {
    /// Number assigned to the table file by its caller.
    pub file_number: u64,
    /// Number of internal key/value records in the table.
    pub entries: u64,
    /// Number of independently checksummed data blocks.
    pub data_blocks: u64,
    /// Compression used by every data block.
    ///
    /// Snappy commonly reduces disk reads and file size with modest CPU cost,
    /// but incompressible data can become slightly larger. `None` avoids codec
    /// work and expansion at the cost of writing every uncompressed byte.
    pub compression: Compression,
    /// First internal key in bytewise table order.
    pub smallest: InternalKey,
    /// Last internal key in bytewise table order.
    pub largest: InternalKey,
    /// Largest uncompressed data-block payload in this table.
    pub max_data_block_bytes: u64,
}

/// Durable result returned after an SSTable file has been fully synchronized.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableBuildResult {
    /// Number assigned to the completed file.
    pub file_number: u64,
    /// Complete file length, including the fixed footer.
    pub file_size: u64,
    /// First internal key in the file.
    pub smallest: InternalKey,
    /// Last internal key in the file.
    pub largest: InternalKey,
    /// Number of records written.
    pub entries: u64,
}

#[derive(Debug)]
struct DataBlockMeta {
    first: Vec<u8>,
    last: Vec<u8>,
    handle: BlockHandle,
}

/// Builds one complete, immutable SSTable in strictly increasing internal-key order.
///
/// Data records are grouped into prefix-compressed blocks near `block_bytes`.
/// Oversized individual records remain intact, so the target is approximate.
/// A global Bloom filter permits definite misses to skip data I/O, while an
/// index maps separator keys to block handles. A separator is at least the
/// preceding block's last key but, when byte space exists, shorter than the
/// following block's first key. Looking for the first separator greater than
/// or equal to a target therefore chooses one candidate data block without
/// storing every block boundary twice.
///
/// `finish` writes properties and a fixed footer, then calls `sync_all`:
/// success means the temporary file's bytes and metadata reached stable
/// storage. A later rename and directory synchronization remain the
/// responsibility of manifest/flush code.
pub struct TableBuilder {
    path: PathBuf,
    file: Box<dyn DurableFile>,
    file_number: u64,
    offset: u64,
    block_bytes: usize,
    restart_interval: usize,
    bloom_bits_per_key: u8,
    compression: Compression,
    block: Option<BlockBuilder>,
    pending_bytes: usize,
    pending_first: Option<Vec<u8>>,
    pending_last: Option<Vec<u8>>,
    previous_key: Option<Vec<u8>>,
    filter_keys: Vec<Vec<u8>>,
    data_blocks: Vec<DataBlockMeta>,
    entries: u64,
    smallest: Option<InternalKey>,
    largest: Option<InternalKey>,
    max_data_block_bytes: u64,
}

impl TableBuilder {
    /// Exclusively creates a temporary SSTable file.
    ///
    /// `block_bytes` is an approximate uncompressed data-block target.
    /// `restart_interval` controls prefix-compression seek work, and
    /// `bloom_bits_per_key` trades filter space for fewer false positives.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] for zero configuration values, or
    /// [`Error::Io`] if the path already exists or cannot be created.
    pub fn create(
        path: impl AsRef<Path>,
        file_number: u64,
        block_bytes: usize,
        restart_interval: usize,
        bloom_bits_per_key: u8,
        compression: Compression,
    ) -> Result<Self> {
        Self::create_with_fs(
            path,
            file_number,
            block_bytes,
            restart_interval,
            bloom_bits_per_key,
            compression,
            Arc::new(OsDurableFs),
        )
    }

    /// Creates a builder using an injectable durable filesystem.
    ///
    /// Production callers normally use [`TableBuilder::create`]. Supplying the
    /// trait explicitly lets crash tests observe or fail the final temporary
    /// file synchronization without changing the serialized table format.
    ///
    /// # Errors
    ///
    /// Returns the same validation and creation errors as [`TableBuilder::create`].
    pub fn create_with_fs(
        path: impl AsRef<Path>,
        file_number: u64,
        block_bytes: usize,
        restart_interval: usize,
        bloom_bits_per_key: u8,
        compression: Compression,
        fs: Arc<dyn DurableFs>,
    ) -> Result<Self> {
        if block_bytes == 0 {
            return Err(Error::InvalidArgument(
                "block_bytes must be greater than zero".to_owned(),
            ));
        }
        if bloom_bits_per_key == 0 {
            return Err(Error::InvalidArgument(
                "bloom_bits_per_key must be greater than zero".to_owned(),
            ));
        }
        let block = BlockBuilder::try_new(restart_interval)?;
        let path = path.as_ref().to_path_buf();
        let file = fs
            .create(&path)
            .map_err(|source| io_error("create SSTable", &path, source))?;
        Ok(Self {
            path,
            file,
            file_number,
            offset: 0,
            block_bytes,
            restart_interval,
            bloom_bits_per_key,
            compression,
            block: Some(block),
            pending_bytes: 0,
            pending_first: None,
            pending_last: None,
            previous_key: None,
            filter_keys: Vec::new(),
            data_blocks: Vec::new(),
            entries: 0,
            smallest: None,
            largest: None,
            max_data_block_bytes: 0,
        })
    }

    /// Adds one internal key and value.
    ///
    /// Internal keys must be strictly increasing. This preserves MVCC ordering
    /// inside blocks and lets separator keys route a lookup to exactly one block.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] for duplicate/descending keys or
    /// overflowing lengths and [`Error::Io`] if flushing a full block fails.
    pub fn add(&mut self, key: &InternalKey, value: &[u8]) -> Result<()> {
        if self
            .previous_key
            .as_deref()
            .is_some_and(|previous| key.as_bytes() <= previous)
        {
            return Err(Error::InvalidArgument(
                "SSTable keys must be strictly increasing".to_owned(),
            ));
        }
        let entry_bytes = key
            .as_bytes()
            .len()
            .checked_add(value.len())
            .ok_or_else(|| Error::InvalidArgument("SSTable entry length overflows usize".into()))?;
        let next_bytes = self
            .pending_bytes
            .checked_add(entry_bytes)
            .ok_or_else(|| Error::InvalidArgument("SSTable block size overflows usize".into()))?;
        if self.pending_bytes != 0 && next_bytes > self.block_bytes {
            self.flush_data_block()?;
        }

        self.block
            .as_mut()
            .expect("unfinished builders retain a data block")
            .add(key.as_bytes(), value)?;
        self.pending_bytes = if self.pending_bytes == 0 {
            entry_bytes
        } else {
            next_bytes
        };
        self.pending_first
            .get_or_insert_with(|| key.as_bytes().to_vec());
        self.pending_last = Some(key.as_bytes().to_vec());
        self.previous_key = Some(key.as_bytes().to_vec());
        self.filter_keys.push(key.as_bytes().to_vec());
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("SSTable entry count overflows u64".into()))?;
        if self.smallest.is_none() {
            self.smallest = Some(key.clone());
        }
        self.largest = Some(key.clone());
        Ok(())
    }

    /// Completes, checksums, and synchronizes the immutable file.
    ///
    /// Data blocks are written first, followed by Bloom-filter, index, and
    /// properties blocks. The final fixed footer stores three checked
    /// offset/size handles, format version `1`, and `METEOR01` magic.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] for an empty table or arithmetic
    /// overflow, [`Error::Io`] for write/synchronization failures, and
    /// compression errors as [`Error::Corruption`] because a codec that cannot
    /// represent freshly built bytes cannot produce a valid table.
    pub fn finish(mut self) -> Result<TableBuildResult> {
        if self.entries == 0 {
            return Err(Error::InvalidArgument(
                "cannot finish an empty SSTable".to_owned(),
            ));
        }
        self.flush_data_block()?;

        let filter = BloomFilter::from_keys(&self.filter_keys, self.bloom_bits_per_key)?;
        let filter_handle = self.write_payload(filter.as_bytes(), Compression::None)?;

        let mut index = BlockBuilder::try_new(self.restart_interval)?;
        for position in 0..self.data_blocks.len() {
            let meta = &self.data_blocks[position];
            let separator = self.data_blocks.get(position + 1).map_or_else(
                || meta.last.clone(),
                |next| shortest_separator(&meta.last, &next.first),
            );
            index.add(&separator, &meta.handle.encode())?;
        }
        let index_handle = self.write_payload(&index.finish(), Compression::None)?;

        let properties = TableProperties {
            file_number: self.file_number,
            entries: self.entries,
            data_blocks: u64::try_from(self.data_blocks.len()).map_err(|_| {
                Error::InvalidArgument("SSTable data block count exceeds u64".to_owned())
            })?,
            compression: self.compression,
            smallest: self
                .smallest
                .clone()
                .expect("nonempty table has a first key"),
            largest: self.largest.clone().expect("nonempty table has a last key"),
            max_data_block_bytes: self.max_data_block_bytes,
        };
        let properties_handle =
            self.write_payload(&encode_properties(&properties)?, Compression::None)?;
        let footer = encode_footer(index_handle, filter_handle, properties_handle);
        self.write_raw(&footer)?;
        self.file
            .sync_all()
            .map_err(|source| io_error("sync SSTable", &self.path, source))?;

        Ok(TableBuildResult {
            file_number: self.file_number,
            file_size: self.offset,
            smallest: properties.smallest,
            largest: properties.largest,
            entries: self.entries,
        })
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.pending_bytes == 0 {
            return Ok(());
        }
        let block = self
            .block
            .take()
            .expect("unfinished builders retain a data block")
            .finish();
        self.max_data_block_bytes = self.max_data_block_bytes.max(
            u64::try_from(block.len())
                .map_err(|_| Error::InvalidArgument("data block length exceeds u64".into()))?,
        );
        let handle = self.write_payload(&block, self.compression)?;
        self.data_blocks.push(DataBlockMeta {
            first: self
                .pending_first
                .take()
                .expect("nonempty block has first key"),
            last: self
                .pending_last
                .take()
                .expect("nonempty block has last key"),
            handle,
        });
        self.block = Some(BlockBuilder::try_new(self.restart_interval)?);
        self.pending_bytes = 0;
        Ok(())
    }

    fn write_payload(&mut self, payload: &[u8], compression: Compression) -> Result<BlockHandle> {
        let (stored_payload, marker) = match compression {
            Compression::None => (payload.to_vec(), NO_COMPRESSION),
            Compression::Snappy => (
                snap::raw::Encoder::new()
                    .compress_vec(payload)
                    .map_err(|error| {
                        table_corruption(format!("Snappy compression failed: {error}"))
                    })?,
                SNAPPY_COMPRESSION,
            ),
        };
        let encoded = encode_stored_block(&stored_payload, marker)?;
        let size = u64::try_from(encoded.len())
            .map_err(|_| Error::InvalidArgument("stored block length exceeds u64".into()))?;
        let handle = BlockHandle::new(self.offset, size);
        self.write_raw(&encoded)?;
        Ok(handle)
    }

    fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| Error::InvalidArgument("SSTable write length exceeds u64".into()))?;
        let next = self
            .offset
            .checked_add(length)
            .ok_or_else(|| Error::InvalidArgument("SSTable file offset overflows u64".into()))?;
        self.file
            .write_all(bytes)
            .map_err(|source| io_error("write SSTable", &self.path, source))?;
        self.offset = next;
        Ok(())
    }
}

pub(super) fn encode_footer(
    index: BlockHandle,
    filter: BlockHandle,
    properties: BlockHandle,
) -> [u8; SSTABLE_FOOTER_BYTES] {
    let mut footer = [0; SSTABLE_FOOTER_BYTES];
    let mut cursor = 0;
    for handle in [index, filter, properties] {
        let encoded = handle.encode();
        footer[cursor..cursor + encoded.len()].copy_from_slice(&encoded);
        cursor += FOOTER_HANDLE_BYTES;
    }
    footer[cursor..cursor + 4].copy_from_slice(&SSTABLE_FORMAT_VERSION.to_le_bytes());
    footer[cursor + 4..].copy_from_slice(&SSTABLE_MAGIC);
    footer
}

pub(super) fn encode_properties(properties: &TableProperties) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    put_varint(&mut encoded, properties.file_number);
    put_varint(&mut encoded, properties.entries);
    put_varint(&mut encoded, properties.data_blocks);
    encoded.push(match properties.compression {
        Compression::None => NO_COMPRESSION,
        Compression::Snappy => SNAPPY_COMPRESSION,
    });
    put_varint(&mut encoded, properties.max_data_block_bytes);
    for key in [&properties.smallest, &properties.largest] {
        put_varint(
            &mut encoded,
            u64::try_from(key.as_bytes().len())
                .map_err(|_| Error::InvalidArgument("property key length exceeds u64".into()))?,
        );
        encoded.extend_from_slice(key.as_bytes());
    }
    Ok(encoded)
}

fn shortest_separator(start: &[u8], limit: &[u8]) -> Vec<u8> {
    let shared = start
        .iter()
        .zip(limit)
        .take_while(|(left, right)| left == right)
        .count();
    if let (Some(&start_byte), Some(&limit_byte)) = (start.get(shared), limit.get(shared))
        && start_byte < 0xff
        && start_byte + 1 < limit_byte
    {
        let mut separator = start[..=shared].to_vec();
        separator[shared] += 1;
        return separator;
    }
    start.to_vec()
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn table_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "SSTable",
        detail: detail.into(),
    }
}
