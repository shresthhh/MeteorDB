//! Checked building blocks shared by immutable sorted-table readers and writers.

mod block;
mod format;

pub use block::{Block, BlockBuilder, BlockIter};
pub use format::{
    BLOCK_TRAILER_BYTES, BlockHandle, NO_COMPRESSION, SNAPPY_COMPRESSION, SSTABLE_FORMAT_VERSION,
    SSTABLE_MAGIC, decode_stored_block, encode_stored_block,
};
