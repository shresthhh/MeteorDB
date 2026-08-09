#![no_main]

use libfuzzer_sys::fuzz_target;
use meteordb::InternalKey;

fuzz_target!(|data: &[u8]| {
    let _ = InternalKey::decode(data);
});
