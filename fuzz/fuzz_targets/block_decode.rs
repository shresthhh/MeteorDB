#![no_main]

use libfuzzer_sys::fuzz_target;
use meteordb::{Block, decode_stored_block};

fuzz_target!(|data: &[u8]| {
    let _ = decode_stored_block(data);
    let _ = Block::decode(data);
});
