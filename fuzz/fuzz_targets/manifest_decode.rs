#![no_main]

use std::path::Path;
use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use meteordb::{DurableFile, DurableFs, ManifestInspectionOptions, inspect_manifest_with_fs};

struct InputFs(Vec<u8>);

impl DurableFs for InputFs {
    fn create(&self, _: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Err(std::io::Error::other("read-only fuzz filesystem"))
    }

    fn append(&self, _: &Path) -> std::io::Result<Box<dyn DurableFile>> {
        Err(std::io::Error::other("read-only fuzz filesystem"))
    }

    fn read_file(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        if path.file_name().is_some_and(|name| name == "CURRENT") {
            Ok(b"MANIFEST-000001\n".to_vec())
        } else {
            Ok(self.0.clone())
        }
    }

    fn sync_directory(&self, _: &Path) -> std::io::Result<()> {
        Err(std::io::Error::other("read-only fuzz filesystem"))
    }

    fn atomic_replace(&self, _: &Path, _: &Path) -> std::io::Result<()> {
        Err(std::io::Error::other("read-only fuzz filesystem"))
    }
}

fuzz_target!(|data: &[u8]| {
    let limits = ManifestInspectionOptions {
        max_edits: 64,
        max_files: 128,
        max_bytes: 64 * 1024,
        max_historical_files: 128,
    };
    let _ = inspect_manifest_with_fs(Path::new("."), limits, Arc::new(InputFs(data.to_vec())));
});
