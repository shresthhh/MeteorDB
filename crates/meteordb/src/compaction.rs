use std::path::Path;
use std::sync::Arc;

use crate::iter::{ChildIterator, InternalEntry, InternalMergingIterator, disk_entry};
use crate::stats::ReadStats;
use crate::{
    BlockCache, DurableFs, Error, FileMeta, NUM_LEVELS, Options, Result, SequenceNumber,
    TableBuilder, TableReader, TableReaderOptions, ValueKind, ValueRecord, Version,
};

/// Default number of level-zero files tolerated before compaction is selected.
pub const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;

/// Selects the most overfull level and expands its cross-level overlaps.
#[derive(Clone, Debug)]
pub struct CompactionPicker {
    l0_trigger: usize,
    level_base_bytes: u64,
}

impl CompactionPicker {
    /// Creates a picker with a level-zero file trigger and per-level byte target.
    pub fn new(l0_trigger: usize, level_base_bytes: u64) -> Self {
        Self {
            l0_trigger: l0_trigger.max(1),
            level_base_bytes: level_base_bytes.max(1),
        }
    }

    /// Returns the highest-scoring compaction whose score is greater than one.
    pub fn pick(&self, version: &Version) -> Option<CompactionPlan> {
        let mut selected = None;
        for level in 0..NUM_LEVELS - 1 {
            let score = if level == 0 {
                version.files(0).len() as f64 / self.l0_trigger as f64
            } else {
                let bytes = version
                    .files(level)
                    .iter()
                    .map(FileMeta::file_size)
                    .sum::<u64>();
                bytes as f64 / self.level_target(level) as f64
            };
            if score > 1.0
                && selected
                    .as_ref()
                    .is_none_or(|plan: &CompactionPlan| score > plan.score)
            {
                selected = make_plan(version, level, score);
            }
        }
        selected
    }

    fn level_target(&self, _level: usize) -> u64 {
        self.level_base_bytes
    }
}

/// Immutable description of one leveled compaction.
#[derive(Clone, Debug)]
pub struct CompactionPlan {
    input_level: usize,
    output_level: usize,
    inputs: Vec<FileMeta>,
    overlaps: Vec<FileMeta>,
    score: f64,
}

impl CompactionPlan {
    /// Returns the level supplying the selected input files.
    pub fn input_level(&self) -> usize {
        self.input_level
    }

    /// Returns the level receiving the generated output files.
    pub fn output_level(&self) -> usize {
        self.output_level
    }

    /// Returns selected files from the input level.
    pub fn input_files(&self) -> &[FileMeta] {
        &self.inputs
    }

    /// Returns every overlapping file from the output level.
    pub fn overlap_files(&self) -> &[FileMeta] {
        &self.overlaps
    }

    /// Returns the selected level's fullness ratio.
    pub fn score(&self) -> f64 {
        self.score
    }
}

/// Executable wrapper around one immutable compaction plan.
#[derive(Clone, Debug)]
pub struct CompactionJob {
    plan: CompactionPlan,
}

impl CompactionJob {
    /// Creates a job for `plan`.
    pub fn new(plan: CompactionPlan) -> Self {
        Self { plan }
    }

    /// Borrows the plan executed by this job.
    pub fn plan(&self) -> &CompactionPlan {
        &self.plan
    }

    /// Executes this exact plan against `engine`.
    pub fn execute(&self, engine: &crate::Engine) -> Result<()> {
        engine.execute_compaction_plan(self.plan.clone())
    }
}

fn make_plan(version: &Version, level: usize, score: f64) -> Option<CompactionPlan> {
    let mut inputs = if level == 0 {
        version.files(0).to_vec()
    } else {
        version.files(level).first().cloned().into_iter().collect()
    };
    if inputs.is_empty() {
        return None;
    }
    let output_level = level + 1;
    let mut overlaps = Vec::new();
    loop {
        let (smallest, largest) = user_range(inputs.iter().chain(overlaps.iter()));
        let next_overlaps = version
            .files(output_level)
            .iter()
            .filter(|file| ranges_overlap(file, &smallest, &largest))
            .cloned()
            .collect::<Vec<_>>();
        let (expanded_smallest, expanded_largest) =
            user_range(inputs.iter().chain(next_overlaps.iter()));
        let expanded_inputs = if level == 0 {
            inputs.clone()
        } else {
            version
                .files(level)
                .iter()
                .filter(|file| ranges_overlap(file, &expanded_smallest, &expanded_largest))
                .cloned()
                .collect()
        };
        if next_overlaps == overlaps && expanded_inputs == inputs {
            break;
        }
        overlaps = next_overlaps;
        inputs = expanded_inputs;
    }
    Some(CompactionPlan {
        input_level: level,
        output_level,
        inputs,
        overlaps,
        score,
    })
}

fn user_range<'a>(files: impl Iterator<Item = &'a FileMeta>) -> (Vec<u8>, Vec<u8>) {
    let mut smallest: Option<Vec<u8>> = None;
    let mut largest: Option<Vec<u8>> = None;
    for file in files {
        let file_smallest = file.smallest().user_key();
        let file_largest = file.largest().user_key();
        if smallest.as_deref().is_none_or(|key| file_smallest < key) {
            smallest = Some(file_smallest.to_vec());
        }
        if largest.as_deref().is_none_or(|key| file_largest > key) {
            largest = Some(file_largest.to_vec());
        }
    }
    (
        smallest.expect("compaction has at least one input"),
        largest.expect("compaction has at least one input"),
    )
}

fn ranges_overlap(file: &FileMeta, smallest: &[u8], largest: &[u8]) -> bool {
    file.smallest().user_key() <= largest && file.largest().user_key() >= smallest
}

pub(crate) struct CompactionOutput {
    pub(crate) files: Vec<FileMeta>,
    pub(crate) next_file_number: u64,
    cleanup: CompactionCleanup,
}

impl CompactionOutput {
    pub(crate) fn preserve_files(&mut self) {
        self.cleanup.armed = false;
    }
}

pub(crate) struct CompactionContext<'a> {
    pub(crate) directory: &'a Path,
    pub(crate) options: &'a Options,
    pub(crate) fs: Arc<dyn DurableFs>,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) read_stats: Arc<ReadStats>,
    pub(crate) version: &'a Version,
    pub(crate) oldest_active_snapshot: Option<SequenceNumber>,
    pub(crate) read_time_unix_ms: u64,
    pub(crate) next_file_number: u64,
}

pub(crate) fn run(
    plan: &CompactionPlan,
    context: CompactionContext<'_>,
) -> Result<CompactionOutput> {
    let mut children: Vec<ChildIterator> = Vec::new();
    for file in plan.inputs.iter().chain(&plan.overlaps) {
        let reader = TableReader::open_cached(
            context.directory.join(format!("{:06}.sst", file.number())),
            file.number(),
            context.block_cache.clone(),
            context.read_stats.clone(),
            TableReaderOptions {
                max_uncompressed_data_block_bytes: reader_block_limit(context.options),
            },
            context.fs.clone(),
        )?;
        if reader.file_size() != file.file_size() {
            return Err(Error::Corruption {
                context: "SSTable",
                detail: format!(
                    "{:06}.sst has length {}, expected {}",
                    file.number(),
                    reader.file_size(),
                    file.file_size()
                ),
            });
        }
        let engine_encoded = reader.properties().engine_value_encoding;
        children.push(Box::new(reader.into_iter().map(move |entry| {
            entry.and_then(|(key, value)| disk_entry(key, value, engine_encoded))
        })));
    }

    let mut merged = InternalMergingIterator::new(children).peekable();
    let mut next_file_number = context.next_file_number;
    let mut outputs = Vec::new();
    let mut cleanup = CompactionCleanup::new(context.directory, context.fs.clone());
    let mut pending: Vec<InternalEntry> = Vec::new();
    let mut builder: Option<OutputBuilder> = None;

    while let Some(entry) = merged.next() {
        let entry = entry?;
        let user_key = entry.key.user_key().to_vec();
        pending.push(entry);
        while merged.peek().is_some_and(|entry| {
            entry
                .as_ref()
                .is_ok_and(|entry| entry.key.user_key() == user_key)
        }) {
            pending.push(merged.next().expect("peeked entry exists")?);
        }

        let retained = retain_versions(
            std::mem::take(&mut pending),
            context.oldest_active_snapshot,
            key_may_exist_below(context.version, plan.output_level, &user_key),
            context.read_time_unix_ms,
        );
        if retained.is_empty() {
            continue;
        }
        let group_bytes = retained.iter().try_fold(0usize, |bytes, entry| {
            bytes
                .checked_add(entry.key.as_bytes().len())
                .and_then(|bytes| bytes.checked_add(entry.record.encode_engine_value().len()))
                .ok_or_else(|| {
                    Error::InvalidArgument("compaction output size overflows usize".into())
                })
        })?;
        if builder.as_ref().is_some_and(|output| {
            output.estimated_bytes != 0
                && output.estimated_bytes.saturating_add(group_bytes)
                    > context.options.target_sstable_bytes
        }) {
            outputs.push(finish_output(
                builder.take().expect("checked output exists"),
                &context,
                &mut cleanup,
            )?);
        }
        if builder.is_none() {
            let number = next_file_number;
            next_file_number = next_file_number
                .checked_add(1)
                .ok_or_else(|| Error::InvalidArgument("file number space is exhausted".into()))?;
            let output = start_output(number, &context)?;
            cleanup.track_temporary(number);
            builder = Some(output);
        }
        let output = builder.as_mut().expect("output was just created");
        for entry in retained {
            output
                .builder
                .add(&entry.key, &entry.record.encode_engine_value())?;
        }
        output.estimated_bytes = output.estimated_bytes.saturating_add(group_bytes);
    }
    if let Some(output) = builder {
        outputs.push(finish_output(output, &context, &mut cleanup)?);
    }
    Ok(CompactionOutput {
        files: outputs,
        next_file_number,
        cleanup,
    })
}

fn retain_versions(
    mut entries: Vec<InternalEntry>,
    oldest_snapshot: Option<SequenceNumber>,
    key_may_exist_below: bool,
    read_time_unix_ms: u64,
) -> Vec<InternalEntry> {
    for entry in &mut entries {
        if matches!(
            entry.record,
            ValueRecord::Value {
                expires_at_unix_ms: Some(expires),
                ..
            } if expires <= read_time_unix_ms
        ) {
            entry.key = crate::InternalKey::try_new(
                entry.key.user_key(),
                entry.key.sequence(),
                ValueKind::Deletion,
            )
            .expect("an existing internal key remains valid when changed to a tombstone");
            entry.record = ValueRecord::Tombstone;
        }
    }
    let mut retained = Vec::new();
    let mut kept_snapshot_base = false;
    for entry in entries {
        let keep = if retained.is_empty() {
            if oldest_snapshot.is_some_and(|snapshot| entry.key.sequence() <= snapshot) {
                kept_snapshot_base = true;
            }
            true
        } else if let Some(snapshot) = oldest_snapshot {
            if entry.key.sequence() > snapshot {
                true
            } else if !kept_snapshot_base {
                kept_snapshot_base = true;
                true
            } else {
                false
            }
        } else {
            false
        };
        if keep {
            retained.push(entry);
        }
    }
    if retained.first().is_some_and(|entry| {
        entry.key.kind() == ValueKind::Deletion
            && !key_may_exist_below
            && oldest_snapshot.is_none_or(|snapshot| entry.key.sequence() <= snapshot)
    }) {
        retained.clear();
    }
    retained
}

fn key_may_exist_below(version: &Version, output_level: usize, key: &[u8]) -> bool {
    ((output_level + 1)..NUM_LEVELS).any(|level| {
        version
            .files(level)
            .iter()
            .any(|file| file.smallest().user_key() <= key && file.largest().user_key() >= key)
    })
}

struct OutputBuilder {
    builder: TableBuilder,
    final_path: std::path::PathBuf,
    estimated_bytes: usize,
}

fn start_output(number: u64, context: &CompactionContext<'_>) -> Result<OutputBuilder> {
    let temporary = context.directory.join(format!("{number:06}.sst.tmp"));
    let final_path = context.directory.join(format!("{number:06}.sst"));
    let mut builder = TableBuilder::create_with_fs(
        temporary,
        number,
        context.options.block_bytes,
        context.options.restart_interval,
        context.options.bloom_bits_per_key,
        crate::Compression::None,
        context.fs.clone(),
    )?;
    builder.use_engine_value_encoding();
    Ok(OutputBuilder {
        builder,
        final_path,
        estimated_bytes: 0,
    })
}

fn finish_output(
    output: OutputBuilder,
    context: &CompactionContext<'_>,
    cleanup: &mut CompactionCleanup,
) -> Result<FileMeta> {
    let built = output.builder.finish()?;
    let temporary = context
        .directory
        .join(format!("{:06}.sst.tmp", built.file_number));
    // Installation may create the destination before failing to remove the temporary link.
    cleanup.track_destination(built.file_number);
    context
        .fs
        .atomic_install(&temporary, &output.final_path)
        .map_err(|source| io_error("install compacted SSTable", &output.final_path, source))?;
    context
        .fs
        .sync_directory(context.directory)
        .map_err(|source| {
            io_error(
                "sync compacted SSTable directory",
                context.directory,
                source,
            )
        })?;
    FileMeta::new(
        built.file_number,
        built.file_size,
        built.smallest,
        built.largest,
    )
}

struct CompactionCleanup {
    directory: std::path::PathBuf,
    fs: Arc<dyn DurableFs>,
    paths: Vec<std::path::PathBuf>,
    armed: bool,
}

impl CompactionCleanup {
    fn new(directory: &Path, fs: Arc<dyn DurableFs>) -> Self {
        Self {
            directory: directory.to_path_buf(),
            fs,
            paths: Vec::new(),
            armed: true,
        }
    }

    fn track_temporary(&mut self, number: u64) {
        self.paths
            .push(self.directory.join(format!("{number:06}.sst.tmp")));
    }

    fn track_destination(&mut self, number: u64) {
        self.paths
            .push(self.directory.join(format!("{number:06}.sst")));
    }
}

impl Drop for CompactionCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for path in self.paths.iter().rev() {
            match self.fs.remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {}
            }
        }
        if !self.paths.is_empty() {
            let _ = self.fs.sync_directory(&self.directory);
        }
    }
}

fn reader_block_limit(options: &Options) -> usize {
    options
        .block_bytes
        .max(crate::DEFAULT_MAX_UNCOMPRESSED_DATA_BLOCK_BYTES)
        .saturating_mul(4)
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}
