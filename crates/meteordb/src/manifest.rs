use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs2::FileExt;

use crate::fs::open_lock_file;
use crate::version::NUM_LEVELS;
use crate::{
    DurableFile, DurableFs, Error, FileMeta, InternalKey, OsDurableFs, Result, SequenceNumber,
    Version, VersionEdit,
};

const BLOCK_BYTES: usize = 32 * 1024;
const HEADER_BYTES: usize = 7;
const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;
const CHECKSUM_MASK_DELTA: u32 = 0xa282_ead8;
const FORMAT_VERSION: u8 = 1;
const MAX_EDIT_BYTES: usize = 64 * 1024 * 1024;

/// Owns the append-only manifest and the currently published immutable version.
///
/// Applying an edit first validates a copy of the current version. Every added
/// SSTable is then synchronized, the framed edit is appended and synchronized,
/// and only then is the new [`Arc<Version>`] published. Readers retaining the
/// old `Arc` continue to see the exact old live-file set.
pub struct VersionSet {
    directory: PathBuf,
    fs: Arc<dyn DurableFs>,
    manifest_number: u64,
    _lock: DatabaseLock,
    manifest: ManifestWriter,
    current: Arc<Version>,
    used_file_numbers: HashSet<u64>,
    next_file_number: u64,
    last_sequence: SequenceNumber,
    manifest_usable: bool,
}

impl VersionSet {
    /// Creates a new manifest and atomically installs its `CURRENT` pointer.
    pub fn create(directory: impl AsRef<Path>) -> Result<Self> {
        Self::create_with_fs(directory, Arc::new(OsDurableFs))
    }

    /// Creates a version set with injectable crash-sensitive filesystem calls.
    pub fn create_with_fs(directory: impl AsRef<Path>, fs: Arc<dyn DurableFs>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        let lock = DatabaseLock::acquire(&directory)?;
        let manifest_number = 1;
        let manifest_name = manifest_name(manifest_number);
        let manifest_path = directory.join(&manifest_name);
        let manifest_temp = directory.join(format!("{manifest_name}.tmp"));
        let current_path = directory.join("CURRENT");
        if current_path
            .try_exists()
            .map_err(|source| io_error("check CURRENT", &current_path, source))?
            || manifest_path
                .try_exists()
                .map_err(|source| io_error("check manifest", &manifest_path, source))?
        {
            return Err(Error::InvalidArgument(format!(
                "database already exists at {}",
                directory.display()
            )));
        }

        let initial = initial_edit();
        let encoded = encode_edit(&initial)?;
        let file = fs
            .create(&manifest_temp)
            .map_err(|source| io_error("create manifest temporary file", &manifest_temp, source))?;
        let mut writer = ManifestWriter::new(file, manifest_temp.clone(), 0);
        writer.append(&encoded)?;
        writer.sync("sync new manifest")?;
        fs.atomic_replace(&manifest_temp, &manifest_path)
            .map_err(|source| {
                io_error("replace manifest temporary file", &manifest_path, source)
            })?;
        fs.sync_directory(&directory)
            .map_err(|source| io_error("sync manifest directory", &directory, source))?;

        replace_current(&directory, &manifest_name, fs.as_ref())?;
        drop(writer);
        let manifest = fs
            .append_existing(&manifest_path)
            .map_err(|source| io_error("open manifest for append", &manifest_path, source))?;
        let block_offset = encoded.len() + HEADER_BYTES;

        Ok(Self {
            directory,
            fs,
            manifest_number,
            _lock: lock,
            manifest: ManifestWriter::new(manifest, manifest_path, block_offset),
            current: Arc::new(Version::empty()),
            used_file_numbers: HashSet::from([manifest_number]),
            next_file_number: 2,
            last_sequence: 0,
            manifest_usable: true,
        })
    }

    /// Recovers the version named by `CURRENT`.
    pub fn recover(directory: impl AsRef<Path>) -> Result<Self> {
        Self::recover_with_fs(directory, Arc::new(OsDurableFs))
    }

    /// Recovers with injectable filesystem operations for later durable edits.
    pub fn recover_with_fs(directory: impl AsRef<Path>, fs: Arc<dyn DurableFs>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        let lock = DatabaseLock::acquire(&directory)?;
        let current_path = directory.join("CURRENT");
        let current = fs
            .read_file(&current_path)
            .map_err(|source| io_error("read CURRENT", &current_path, source))?;
        let manifest_name = parse_current(&current)?;
        let manifest_number = parse_manifest_number(&manifest_name)?;
        let manifest_path = directory.join(&manifest_name);
        let replay = replay_manifest(&manifest_path, fs.as_ref())?;
        if replay.edits.is_empty() {
            return Err(manifest_corruption(
                "manifest contains no complete initial edit",
            ));
        }

        let mut version = Version::empty();
        let mut next_file_number = 0;
        let mut last_sequence = 0;
        let mut used_file_numbers = HashSet::from([manifest_number]);
        for edit in replay.edits {
            update_counters(&edit, &mut next_file_number, &mut last_sequence, true)?;
            validate_file_numbers(&edit, next_file_number, &used_file_numbers, true)?;
            let candidate = version.apply(&edit).map_err(recovery_edit_error)?;
            used_file_numbers.extend(edit.added_files.iter().map(|(_, file)| file.number()));
            version = candidate;
        }
        if next_file_number == 0 {
            return Err(manifest_corruption(
                "manifest never records the next file number",
            ));
        }
        validate_referenced_files(&directory, &version, fs.as_ref())?;

        if replay.valid_bytes < replay.file_length {
            fs.truncate_file(&manifest_path, replay.valid_bytes)
                .map_err(|source| {
                    io_error("truncate torn manifest tail", &manifest_path, source)
                })?;
            fs.sync_file(&manifest_path)
                .map_err(|source| io_error("sync truncated manifest", &manifest_path, source))?;
        }
        let manifest = fs
            .append_existing(&manifest_path)
            .map_err(|source| io_error("open manifest for append", &manifest_path, source))?;
        Ok(Self {
            directory,
            fs,
            manifest_number,
            _lock: lock,
            manifest: ManifestWriter::new(
                manifest,
                manifest_path,
                usize::try_from(replay.valid_bytes % BLOCK_BYTES as u64)
                    .expect("physical block offset fits usize"),
            ),
            current: Arc::new(version),
            used_file_numbers,
            next_file_number,
            last_sequence,
            manifest_usable: true,
        })
    }

    /// Durably installs one edit, then publishes its copy-on-write version.
    pub fn apply(&mut self, edit: VersionEdit) -> Result<()> {
        if !self.manifest_usable {
            return Err(Error::Background(
                "manifest writer is unusable after a failed append or sync".into(),
            ));
        }
        let mut next_file_number = self.next_file_number;
        let mut last_sequence = self.last_sequence;
        update_counters(&edit, &mut next_file_number, &mut last_sequence, false)?;
        validate_file_numbers(&edit, next_file_number, &self.used_file_numbers, false)?;
        let candidate = self.current.apply(&edit)?;
        let encoded = encode_edit(&edit)?;

        for (_, file) in &edit.added_files {
            let path = self.directory.join(sstable_name(file.number()));
            self.fs
                .sync_file(&path)
                .map_err(|source| io_error("sync referenced SSTable", &path, source))?;
        }

        if let Err(error) = self.manifest.append(&encoded) {
            self.manifest_usable = false;
            return Err(error);
        }
        if let Err(error) = self.manifest.sync("sync manifest edit") {
            self.manifest_usable = false;
            return Err(error);
        }

        self.current = Arc::new(candidate);
        self.used_file_numbers
            .extend(edit.added_files.iter().map(|(_, file)| file.number()));
        self.next_file_number = next_file_number;
        self.last_sequence = last_sequence;
        Ok(())
    }

    /// Clones the currently published immutable live-file version.
    pub fn current(&self) -> Arc<Version> {
        self.current.clone()
    }

    /// Returns the next file number that has not been allocated.
    pub fn next_file_number(&self) -> u64 {
        self.next_file_number
    }

    /// Returns the largest durable sequence recorded by the manifest.
    pub fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }

    /// Returns the active manifest's file number.
    pub fn manifest_number(&self) -> u64 {
        self.manifest_number
    }
}

struct DatabaseLock {
    _file: std::fs::File,
}

impl DatabaseLock {
    fn acquire(directory: &Path) -> Result<Self> {
        let path = directory.join("LOCK");
        let file = open_lock_file(&path)
            .map_err(|source| io_error("open database lock", &path, source))?;
        file.try_lock_exclusive().map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                Error::Locked(path.clone())
            } else {
                io_error("lock database", &path, source)
            }
        })?;
        Ok(Self { _file: file })
    }
}

fn initial_edit() -> VersionEdit {
    let mut edit = VersionEdit::new();
    edit.set_next_file_number(2);
    edit.set_last_sequence(0);
    edit
}

fn update_counters(
    edit: &VersionEdit,
    next_file_number: &mut u64,
    last_sequence: &mut SequenceNumber,
    recovery: bool,
) -> Result<()> {
    if let Some(next) = edit.next_file_number {
        if next < *next_file_number {
            return Err(counter_error(
                recovery,
                format!(
                    "next file number moved backward from {} to {next}",
                    *next_file_number
                ),
            ));
        }
        if next == 0 {
            return Err(counter_error(
                recovery,
                "next file number must be greater than zero".into(),
            ));
        }
        *next_file_number = next;
    }
    if let Some(sequence) = edit.last_sequence {
        if sequence < *last_sequence {
            return Err(counter_error(
                recovery,
                format!(
                    "last sequence moved backward from {} to {sequence}",
                    *last_sequence
                ),
            ));
        }
        *last_sequence = sequence;
    }
    Ok(())
}

fn counter_error(recovery: bool, detail: String) -> Error {
    if recovery {
        manifest_corruption(detail)
    } else {
        Error::InvalidArgument(detail)
    }
}

fn validate_file_numbers(
    edit: &VersionEdit,
    next_file_number: u64,
    used_file_numbers: &HashSet<u64>,
    recovery: bool,
) -> Result<()> {
    if used_file_numbers
        .iter()
        .any(|number| *number >= next_file_number)
    {
        return Err(counter_error(
            recovery,
            format!(
                "next file number {next_file_number} does not exceed every previously used file number"
            ),
        ));
    }
    let deleted: HashSet<u64> = edit
        .deleted_files
        .iter()
        .map(|(_, number)| *number)
        .collect();
    let mut added = HashSet::new();
    for (_, file) in &edit.added_files {
        let number = file.number();
        if number >= next_file_number {
            return Err(counter_error(
                recovery,
                format!(
                    "SSTable file number {number} is not below next file number {next_file_number}"
                ),
            ));
        }
        if !added.insert(number) {
            return Err(counter_error(
                recovery,
                format!("SSTable file number {number} is added more than once"),
            ));
        }
        if deleted.contains(&number) {
            return Err(counter_error(
                recovery,
                format!("SSTable file number {number} is deleted and re-added in the same edit"),
            ));
        }
        if used_file_numbers.contains(&number) {
            return Err(counter_error(
                recovery,
                format!("SSTable file number {number} has already been used"),
            ));
        }
    }
    Ok(())
}

fn recovery_edit_error(error: Error) -> Error {
    match error {
        Error::InvalidArgument(detail) => manifest_corruption(detail),
        other => other,
    }
}

fn validate_referenced_files(
    directory: &Path,
    version: &Version,
    fs: &dyn DurableFs,
) -> Result<()> {
    for level in 0..NUM_LEVELS {
        for file in version.files(level) {
            let path = directory.join(sstable_name(file.number()));
            fs.validate_file(&path).map_err(|source| {
                manifest_corruption(format!(
                    "level {level} references missing SSTable or invalid file {}: {source}",
                    path.display()
                ))
            })?;
        }
    }
    Ok(())
}

fn replace_current(directory: &Path, manifest_name: &str, fs: &dyn DurableFs) -> Result<()> {
    let current_temp = directory.join("CURRENT.tmp");
    let current_path = directory.join("CURRENT");
    let mut file = fs
        .create(&current_temp)
        .map_err(|source| io_error("create CURRENT temporary file", &current_temp, source))?;
    file.write_all(format!("{manifest_name}\n").as_bytes())
        .map_err(|source| io_error("write CURRENT temporary file", &current_temp, source))?;
    file.sync_all()
        .map_err(|source| io_error("sync CURRENT temporary file", &current_temp, source))?;
    drop(file);
    fs.atomic_replace(&current_temp, &current_path)
        .map_err(|source| io_error("replace CURRENT", &current_path, source))?;
    fs.sync_directory(directory)
        .map_err(|source| io_error("sync CURRENT directory", directory, source))
}

struct ManifestWriter {
    file: Box<dyn DurableFile>,
    path: PathBuf,
    block_offset: usize,
}

impl ManifestWriter {
    fn new(file: Box<dyn DurableFile>, path: PathBuf, block_offset: usize) -> Self {
        Self {
            file,
            path,
            block_offset,
        }
    }

    fn append(&mut self, logical: &[u8]) -> Result<()> {
        let mut position = 0;
        let mut first = true;
        while position < logical.len() {
            let remaining = BLOCK_BYTES - self.block_offset;
            if remaining < HEADER_BYTES {
                let padding = [0; HEADER_BYTES - 1];
                self.file
                    .write_all(&padding[..remaining])
                    .map_err(|source| io_error("pad manifest block", &self.path, source))?;
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
            let length = u16::try_from(fragment_length).expect("fragment fits physical block");
            let mut header = [0; HEADER_BYTES];
            header[..4].copy_from_slice(&masked_checksum(fragment_type, fragment).to_le_bytes());
            header[4..6].copy_from_slice(&length.to_le_bytes());
            header[6] = fragment_type;
            self.file
                .write_all(&header)
                .and_then(|()| self.file.write_all(fragment))
                .map_err(|source| io_error("append manifest edit", &self.path, source))?;
            self.block_offset += HEADER_BYTES + fragment_length;
            position += fragment_length;
            first = false;
        }
        Ok(())
    }

    fn sync(&self, operation: &'static str) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|source| io_error(operation, &self.path, source))
    }
}

struct ManifestReplay {
    edits: Vec<VersionEdit>,
    valid_bytes: u64,
    file_length: u64,
}

fn replay_manifest(path: &Path, fs: &dyn DurableFs) -> Result<ManifestReplay> {
    let contents = fs
        .read_file(path)
        .map_err(|source| io_error("open manifest", path, source))?;
    let file_length = contents.len() as u64;
    let mut block_start = 0_u64;
    let mut logical = Vec::new();
    let mut assembling = false;
    let mut record_start = 0_u64;
    let mut edits = Vec::new();

    loop {
        let start = usize::try_from(block_start).expect("manifest length fits usize");
        let block_length = (contents.len() - start).min(BLOCK_BYTES);
        if block_length == 0 {
            break;
        }
        let block = &contents[start..start + block_length];
        let final_block = block_start + block_length as u64 == file_length;
        let mut offset = 0;
        while offset < block_length {
            let remaining = block_length - offset;
            if remaining < HEADER_BYTES {
                if final_block {
                    let valid_bytes = if assembling {
                        record_start
                    } else {
                        block_start + offset as u64
                    };
                    return Ok(ManifestReplay {
                        edits,
                        valid_bytes,
                        file_length,
                    });
                }
                if block[offset..block_length].iter().any(|byte| *byte != 0) {
                    return Err(manifest_corruption(
                        "nonzero bytes in physical block trailer",
                    ));
                }
                break;
            }
            let header = &block[offset..offset + HEADER_BYTES];
            let stored_checksum =
                u32::from_le_bytes(header[..4].try_into().expect("four checksum bytes"));
            let fragment_length = usize::from(u16::from_le_bytes(
                header[4..6].try_into().expect("two length bytes"),
            ));
            let fragment_type = header[6];
            let fragment_end = offset
                .checked_add(HEADER_BYTES)
                .and_then(|value| value.checked_add(fragment_length))
                .ok_or_else(|| manifest_corruption("physical fragment length overflow"))?;
            if fragment_end > block_length {
                if final_block {
                    return Ok(ManifestReplay {
                        edits,
                        valid_bytes: if assembling {
                            record_start
                        } else {
                            block_start + offset as u64
                        },
                        file_length,
                    });
                }
                return Err(manifest_corruption(
                    "physical fragment crosses a block boundary",
                ));
            }
            let fragment = &block[offset + HEADER_BYTES..fragment_end];
            if unmask_checksum(stored_checksum) != checksum(fragment_type, fragment) {
                return Err(manifest_corruption("physical fragment checksum mismatch"));
            }

            match fragment_type {
                FULL if !assembling => edits.push(decode_edit(fragment)?),
                FIRST if !assembling => {
                    record_start = block_start + offset as u64;
                    logical.clear();
                    append_fragment(&mut logical, fragment)?;
                    assembling = true;
                }
                MIDDLE if assembling => append_fragment(&mut logical, fragment)?,
                LAST if assembling => {
                    append_fragment(&mut logical, fragment)?;
                    edits.push(decode_edit(&logical)?);
                    logical.clear();
                    assembling = false;
                }
                FULL | FIRST => {
                    return Err(manifest_corruption(
                        "new record starts before the previous record ended",
                    ));
                }
                MIDDLE | LAST => {
                    return Err(manifest_corruption(
                        "continuation fragment has no preceding FIRST",
                    ));
                }
                _ => return Err(manifest_corruption("unknown physical fragment type")),
            }
            offset = fragment_end;
        }
        block_start += block_length as u64;
        if final_block {
            break;
        }
    }
    Ok(ManifestReplay {
        edits,
        valid_bytes: if assembling {
            record_start
        } else {
            file_length
        },
        file_length,
    })
}

fn append_fragment(logical: &mut Vec<u8>, fragment: &[u8]) -> Result<()> {
    let length = logical
        .len()
        .checked_add(fragment.len())
        .filter(|length| *length <= MAX_EDIT_BYTES)
        .ok_or_else(|| manifest_corruption("logical edit exceeds the checked size limit"))?;
    logical.reserve(length - logical.len());
    logical.extend_from_slice(fragment);
    Ok(())
}

fn encode_edit(edit: &VersionEdit) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    encoded.push(FORMAT_VERSION);
    put_optional_u64(&mut encoded, edit.next_file_number);
    put_optional_u64(&mut encoded, edit.last_sequence);
    put_count(&mut encoded, edit.deleted_files.len(), "deleted file count")?;
    for &(level, number) in &edit.deleted_files {
        put_level(&mut encoded, level)?;
        encoded.extend_from_slice(&number.to_le_bytes());
    }
    put_count(&mut encoded, edit.added_files.len(), "added file count")?;
    for (level, file) in &edit.added_files {
        put_level(&mut encoded, *level)?;
        encoded.extend_from_slice(&file.number().to_le_bytes());
        encoded.extend_from_slice(&file.file_size().to_le_bytes());
        put_bytes(&mut encoded, file.smallest().as_bytes())?;
        put_bytes(&mut encoded, file.largest().as_bytes())?;
    }
    if encoded.len() > MAX_EDIT_BYTES {
        return Err(Error::InvalidArgument(format!(
            "encoded manifest edit exceeds {MAX_EDIT_BYTES} bytes"
        )));
    }
    Ok(encoded)
}

fn decode_edit(encoded: &[u8]) -> Result<VersionEdit> {
    if encoded.len() > MAX_EDIT_BYTES {
        return Err(manifest_corruption(
            "logical edit exceeds the checked size limit",
        ));
    }
    let mut cursor = Cursor::new(encoded);
    let version = cursor.u8("format version")?;
    if version != FORMAT_VERSION {
        return Err(Error::UnsupportedFormat {
            kind: "manifest",
            version: u32::from(version),
        });
    }
    let next_file_number = cursor.optional_u64("next file number")?;
    let last_sequence = cursor.optional_u64("last sequence")?;
    let deleted_count = cursor.count("deleted file count")?;
    let mut edit = VersionEdit::new();
    edit.next_file_number = next_file_number;
    edit.last_sequence = last_sequence;
    for _ in 0..deleted_count {
        let level = cursor.level()?;
        let number = cursor.u64("deleted file number")?;
        edit.deleted_files.push((level, number));
    }
    let added_count = cursor.count("added file count")?;
    for _ in 0..added_count {
        let level = cursor.level()?;
        let number = cursor.u64("added file number")?;
        let file_size = cursor.u64("added file size")?;
        let smallest = InternalKey::decode(cursor.bytes("smallest key")?)
            .map_err(|error| nested_key_error("smallest", error))?;
        let largest = InternalKey::decode(cursor.bytes("largest key")?)
            .map_err(|error| nested_key_error("largest", error))?;
        let file =
            FileMeta::new(number, file_size, smallest, largest).map_err(recovery_edit_error)?;
        edit.added_files.push((level, file));
    }
    if cursor.remaining() != 0 {
        return Err(manifest_corruption("trailing bytes after version edit"));
    }
    Ok(edit)
}

fn nested_key_error(which: &str, error: Error) -> Error {
    manifest_corruption(format!("invalid {which} internal key: {error}"))
}

fn put_optional_u64(encoded: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(&value.to_le_bytes());
        }
        None => encoded.push(0),
    }
}

fn put_count(encoded: &mut Vec<u8>, count: usize, name: &str) -> Result<()> {
    let count = u32::try_from(count)
        .map_err(|_| Error::InvalidArgument(format!("{name} exceeds u32::MAX")))?;
    encoded.extend_from_slice(&count.to_le_bytes());
    Ok(())
}

fn put_level(encoded: &mut Vec<u8>, level: usize) -> Result<()> {
    if level >= NUM_LEVELS {
        return Err(Error::InvalidArgument(format!(
            "level {level} is outside the supported range 0..{NUM_LEVELS}"
        )));
    }
    encoded.extend_from_slice(&(level as u32).to_le_bytes());
    Ok(())
}

fn put_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| Error::InvalidArgument("manifest key length exceeds u32::MAX".into()))?;
    encoded.extend_from_slice(&length.to_le_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn u8(&mut self, name: &str) -> Result<u8> {
        Ok(self.take(1, name)?[0])
    }

    fn u32(&mut self, name: &str) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4, name)?.try_into().expect("four bytes"),
        ))
    }

    fn u64(&mut self, name: &str) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8, name)?.try_into().expect("eight bytes"),
        ))
    }

    fn optional_u64(&mut self, name: &str) -> Result<Option<u64>> {
        match self.u8(&format!("{name} marker"))? {
            0 => Ok(None),
            1 => self.u64(name).map(Some),
            marker => Err(manifest_corruption(format!(
                "unknown {name} marker {marker}"
            ))),
        }
    }

    fn count(&mut self, name: &str) -> Result<usize> {
        usize::try_from(self.u32(name)?).map_err(|_| manifest_corruption("count overflows usize"))
    }

    fn level(&mut self) -> Result<usize> {
        let level = self.count("level")?;
        if level >= NUM_LEVELS {
            return Err(manifest_corruption(format!(
                "level {level} is outside the supported range 0..{NUM_LEVELS}"
            )));
        }
        Ok(level)
    }

    fn bytes(&mut self, name: &str) -> Result<&'a [u8]> {
        let length = self.count(&format!("{name} length"))?;
        self.take(length, name)
    }

    fn take(&mut self, length: usize, name: &str) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| manifest_corruption(format!("truncated {name}")))?;
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }
}

fn parse_current(bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() || bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(manifest_corruption(
            "CURRENT must contain exactly one newline-terminated manifest name",
        ));
    }
    let name = std::str::from_utf8(&bytes[..bytes.len() - 1])
        .map_err(|_| manifest_corruption("CURRENT manifest name is not UTF-8"))?;
    parse_manifest_number(name)?;
    Ok(name.to_owned())
}

fn parse_manifest_number(name: &str) -> Result<u64> {
    let digits = name
        .strip_prefix("MANIFEST-")
        .filter(|digits| digits.len() == 6 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| manifest_corruption("CURRENT contains an invalid manifest name"))?;
    digits
        .parse()
        .map_err(|_| manifest_corruption("manifest number does not fit u64"))
}

fn manifest_name(number: u64) -> String {
    format!("MANIFEST-{number:06}")
}

fn sstable_name(number: u64) -> String {
    format!("{number:06}.sst")
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

fn manifest_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "manifest",
        detail: detail.into(),
    }
}
