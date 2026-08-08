#![no_main]

use std::path::Path;
use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use meteordb::{DurableFile, DurableFs, inspect_wal_with_fs};

struct InputFs(Vec<u8>);

impl DurableFs for InputFs {
    fn create(&self, _: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Err(std::io::Error::other("read-only fuzz filesystem"))
    }

    fn append(&self, _: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Err(std::io::Error::other("read-only fuzz filesystem"))
    }

    fn read_file(&self, _: &Path) -> std::io::Result<Vec<u8>> {
        Ok(self.0.clone())
    }

    fn sync_directory(&self, _: &Path) -> std::io::Result<()> {
        Err(std::io::Error::other("read-only fuzz filesystem"))
    }

    fn atomic_replace(&self, _: &Path, _: &Path) -> std::io::Result<()> {
        Err(std::io::Error::other("read-only fuzz filesystem"))
    }
}

fuzz_target!(|data: &[u8]| {
    let _ = inspect_wal_with_fs(
        Path::new("input.wal"),
        64 * 1024,
        Arc::new(InputFs(data.to_vec())),
    );
});
