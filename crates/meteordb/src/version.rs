use crate::{Error, InternalKey, Result, SequenceNumber};

/// Number of on-disk levels represented by one immutable database version.
pub const NUM_LEVELS: usize = 7;

/// Immutable metadata for one live SSTable.
///
/// A version stores metadata rather than open table readers. Old versions can
/// therefore remain alive behind [`std::sync::Arc`] while a new version is
/// assembled without changing either the SSTable or the old metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileMeta {
    number: u64,
    file_size: u64,
    smallest: InternalKey,
    largest: InternalKey,
}

impl FileMeta {
    /// Creates checked metadata for one nonempty SSTable.
    pub fn new(
        number: u64,
        file_size: u64,
        smallest: InternalKey,
        largest: InternalKey,
    ) -> Result<Self> {
        if number == 0 {
            return Err(Error::InvalidArgument(
                "SSTable file number must be greater than zero".into(),
            ));
        }
        if file_size == 0 {
            return Err(Error::InvalidArgument(
                "SSTable file size must be greater than zero".into(),
            ));
        }
        if smallest > largest {
            return Err(Error::InvalidArgument(
                "SSTable smallest internal key must not follow its largest key".into(),
            ));
        }
        Ok(Self {
            number,
            file_size,
            smallest,
            largest,
        })
    }

    /// Returns the persistent file number.
    pub fn number(&self) -> u64 {
        self.number
    }

    /// Returns the complete SSTable size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Returns the first internal key stored in the table.
    pub fn smallest(&self) -> &InternalKey {
        &self.smallest
    }

    /// Returns the last internal key stored in the table.
    pub fn largest(&self) -> &InternalKey {
        &self.largest
    }
}

/// One immutable snapshot of all live SSTable metadata.
///
/// Level zero may overlap because independent memtables become independent
/// files. Levels one and above are sorted by user-key range and cannot overlap,
/// allowing a future read to choose at most one file per such level.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Version {
    levels: Vec<Vec<FileMeta>>,
}

impl Version {
    pub(crate) fn empty() -> Self {
        Self {
            levels: vec![Vec::new(); NUM_LEVELS],
        }
    }

    /// Borrows the live files in `level`.
    ///
    /// # Panics
    ///
    /// Panics when `level` is not less than [`NUM_LEVELS`].
    pub fn files(&self, level: usize) -> &[FileMeta] {
        &self.levels[level]
    }

    pub(crate) fn apply(&self, edit: &VersionEdit) -> Result<Self> {
        let mut candidate = self.clone();

        for &(level, number) in &edit.deleted_files {
            let files = candidate.level_mut(level)?;
            let position = files
                .iter()
                .position(|file| file.number == number)
                .ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "cannot delete absent SSTable {number} from level {level}"
                    ))
                })?;
            files.remove(position);
        }

        for (level, file) in &edit.added_files {
            candidate.check_new_file_number(file.number)?;
            candidate.level_mut(*level)?.push(file.clone());
        }

        candidate.levels[0].sort_by(|left, right| right.number.cmp(&left.number));
        for level in 1..NUM_LEVELS {
            candidate.levels[level]
                .sort_by(|left, right| left.smallest.user_key().cmp(right.smallest.user_key()));
            validate_non_overlapping(level, &candidate.levels[level])?;
        }
        Ok(candidate)
    }

    fn level_mut(&mut self, level: usize) -> Result<&mut Vec<FileMeta>> {
        self.levels.get_mut(level).ok_or_else(|| {
            Error::InvalidArgument(format!(
                "level {level} is outside the supported range 0..{NUM_LEVELS}"
            ))
        })
    }

    fn check_new_file_number(&self, number: u64) -> Result<()> {
        if self
            .levels
            .iter()
            .flatten()
            .any(|file| file.number == number)
        {
            return Err(Error::InvalidArgument(format!(
                "SSTable file number {number} is already live"
            )));
        }
        Ok(())
    }
}

fn validate_non_overlapping(level: usize, files: &[FileMeta]) -> Result<()> {
    for pair in files.windows(2) {
        if pair[0].largest.user_key() >= pair[1].smallest.user_key() {
            return Err(Error::InvalidArgument(format!(
                "level {level} SSTable user-key ranges overlap"
            )));
        }
    }
    Ok(())
}

/// A durable, atomic change to the live SSTable set and recovery counters.
///
/// Deletions and additions are applied together. Persistent allocation,
/// sequence, and WAL-ownership counters are monotonic so recovery cannot
/// silently reuse files or release required log segments.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VersionEdit {
    pub(crate) added_files: Vec<(usize, FileMeta)>,
    pub(crate) deleted_files: Vec<(usize, u64)>,
    pub(crate) next_file_number: Option<u64>,
    pub(crate) last_sequence: Option<SequenceNumber>,
    pub(crate) log_number: Option<u64>,
    pub(crate) active_log_number: Option<u64>,
    pub(crate) wal_sequence: Option<SequenceNumber>,
}

impl VersionEdit {
    /// Creates an empty edit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a live SSTable to `level`.
    pub fn add_file(&mut self, level: usize, file: FileMeta) -> &mut Self {
        self.added_files.push((level, file));
        self
    }

    /// Removes a live SSTable from `level`.
    pub fn delete_file(&mut self, level: usize, file_number: u64) -> &mut Self {
        self.deleted_files.push((level, file_number));
        self
    }

    /// Records the next file number that has not been allocated.
    pub fn set_next_file_number(&mut self, number: u64) -> &mut Self {
        self.next_file_number = Some(number);
        self
    }

    /// Records the greatest sequence known durable in lower storage layers.
    pub fn set_last_sequence(&mut self, sequence: SequenceNumber) -> &mut Self {
        self.last_sequence = Some(sequence);
        self
    }

    /// Records the oldest WAL segment still required for recovery.
    pub fn set_log_number(&mut self, number: u64) -> &mut Self {
        self.log_number = Some(number);
        self
    }

    /// Records the WAL segment currently accepting writes.
    pub fn set_active_log_number(&mut self, number: u64) -> &mut Self {
        self.active_log_number = Some(number);
        self
    }

    /// Records the greatest sequence known to have reached a required WAL.
    pub fn set_wal_sequence(&mut self, sequence: SequenceNumber) -> &mut Self {
        self.wal_sequence = Some(sequence);
        self
    }
}
