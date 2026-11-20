//! A partition's log: an ordered sequence of segments, in its own directory.
//!
//! - [`PartitionLog`]: the writer side, the only one that appends and repairs.
//! - [`PartitionReader`]: the reader side, read-only, bounded by the
//!   read limit published by the writer.
//!
//! This module takes no inter-process lock: the topic layer
//! guarantees that only one writer exists per partition.

mod reader;
mod retention;
mod writer;

use std::fs;
use std::path::Path;
use std::time::Duration;

pub(crate) use reader::PartitionReader;
pub use retention::Retention;
pub use writer::PartitionLog;

use crate::error::{Error, Result, corrupt};
use crate::record::{MAX_RECORD_SIZE, Record};
use crate::segment::{self, HEADER_SIZE, ReadRequest, SegmentRead};

/// When to force data to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// `fsync` after each append, before making the messages readable.
    /// Survives a power loss. This is the default.
    Always,
    /// Periodic `fsync`, by a producer thread. A [`PartitionLog`]
    /// used on its own does nothing with it: the caller calls [`PartitionLog::sync`].
    Interval(Duration),
    /// Left to the OS. Survives a process crash, not a power loss.
    Never,
}

/// Configuration of a partition log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    /// Size beyond which we roll over to the next segment.
    pub segment_max_bytes: u64,
    /// Number of bytes written between two index entries.
    pub index_interval_bytes: u64,
    /// When appended data is forced to disk.
    pub fsync: FsyncPolicy,
    /// Size and age limits of the partition. None by default.
    pub retention: Retention,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: 64 * 1024 * 1024,
            index_interval_bytes: 4096,
            fsync: FsyncPolicy::Always,
            retention: Retention::default(),
        }
    }
}

/// Index positions are `u32`s: a segment cannot exceed 4 GiB,
/// including headroom for a final record of maximum size.
const MAX_SEGMENT_BYTES: u64 = u32::MAX as u64 - MAX_RECORD_SIZE as u64;

impl LogConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.segment_max_bytes <= HEADER_SIZE || self.segment_max_bytes > MAX_SEGMENT_BYTES {
            return Err(Error::InvalidConfig(format!(
                "segment_max_bytes must be between {} and {MAX_SEGMENT_BYTES}",
                HEADER_SIZE + 1
            )));
        }
        if self.fsync == FsyncPolicy::Interval(Duration::ZERO) {
            return Err(Error::InvalidConfig(
                "the fsync interval must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Base offsets of the segments present in `dir`, sorted.
pub(crate) fn list_segment_bases(dir: &Path) -> Result<Vec<u64>> {
    let mut bases = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let Some(stem) = name.to_str().and_then(|n| n.strip_suffix(".log")) else {
            continue;
        };
        if stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit()) {
            let base = stem
                .parse()
                .map_err(|_| corrupt(dir.join(&name), "invalid segment name"))?;
            bases.push(base);
        }
    }
    bases.sort_unstable();
    Ok(bases)
}

/// Deletes indexes whose `.log` no longer exists: leftovers of a segment
/// deletion interrupted by a crash.
pub(crate) fn remove_orphan_indexes(dir: &Path, bases: &[u64]) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let base = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix(".idx"))
            .and_then(|stem| stem.parse::<u64>().ok());
        if base.is_some_and(|b| !bases.contains(&b)) {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Why a read across segments stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadStop {
    /// The byte budget has been reached.
    BudgetReached,
    /// All known segments have been read up to `end`.
    SegmentsExhausted,
}

/// Reads across segments, in order, starting from `from` and up to
/// `end` (exclusive). `bases` gives the base offset of each segment, sorted;
/// `read_segment(i, …)` reads segment `i`. `used` accumulates the bytes taken.
pub(crate) fn read_across(
    bases: &[u64],
    from: u64,
    end: u64,
    max_bytes: usize,
    used: &mut usize,
    out: &mut Vec<Record>,
    mut read_segment: impl FnMut(usize, &ReadRequest, &mut Vec<Record>) -> Result<SegmentRead>,
) -> Result<ReadStop> {
    let first = bases.partition_point(|&b| b <= from).saturating_sub(1);
    for i in first..bases.len() {
        let req = ReadRequest {
            from,
            end: bases.get(i + 1).map_or(end, |&next| next.min(end)),
            budget: max_bytes.saturating_sub(*used),
            take_one: out.is_empty(),
        };
        match read_segment(i, &req, out)? {
            SegmentRead::Exhausted { taken } => *used += taken,
            SegmentRead::BudgetReached => return Ok(ReadStop::BudgetReached),
        }
    }
    Ok(ReadStop::SegmentsExhausted)
}

pub(crate) fn segment_log_path(dir: &Path, base_offset: u64) -> std::path::PathBuf {
    segment::log_path(dir, base_offset)
}
