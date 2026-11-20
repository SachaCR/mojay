//! The reader side of a partition: read-only, lock-free, bounded by the
//! read limit published by the writer.

use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::{ReadStop, list_segment_bases, read_across};
use crate::error::{Error, Result};
use crate::record::Record;
use crate::segment::SegmentReader;

/// Maximum delay before noticing that a segment has been deleted by
/// retention, as long as the reader is used (see
/// `TopicReader::release_deleted_segments`). Until then, it stays readable
/// (the open file survives its deletion) but still takes up disk space.
const DISK_CHECK_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub(crate) struct PartitionReader {
    dir: PathBuf,
    /// Sorted by base offset. Empty as long as the writer has never opened the partition.
    segments: Vec<SegmentReader>,
    last_disk_check: Instant,
}

impl PartitionReader {
    pub(crate) fn open(dir: &Path) -> Result<Self> {
        let mut reader = Self {
            dir: dir.to_path_buf(),
            segments: Vec::new(),
            last_disk_check: Instant::now(),
        };
        reader.sync_with_disk()?;
        Ok(reader)
    }

    /// First available offset.
    pub(crate) fn start_offset(&self) -> u64 {
        self.segments.first().map_or(0, SegmentReader::base_offset)
    }

    /// Reads records starting from `offset`, without exceeding `max_bytes`
    /// or `read_limit`. Always returns at least one record if one
    /// exists before the limit.
    pub(crate) fn read(
        &mut self,
        offset: u64,
        max_bytes: usize,
        read_limit: u64,
    ) -> Result<Vec<Record>> {
        self.sync_with_disk_if_due()?;
        let start = self.start_offset();
        if offset < start || offset > read_limit {
            return Err(Error::OffsetOutOfRange {
                offset,
                start,
                end: read_limit,
            });
        }

        let mut out: Vec<Record> = Vec::new();
        let mut used = 0;
        loop {
            let from = out.last().map_or(offset, |r| r.offset + 1);
            if from >= read_limit {
                break;
            }
            let bases: Vec<u64> = self
                .segments
                .iter()
                .map(SegmentReader::base_offset)
                .collect();
            let segments = &mut self.segments;
            let stop = read_across(
                &bases,
                from,
                read_limit,
                max_bytes,
                &mut used,
                &mut out,
                |i, req, out| segments[i].read(req, out),
            )?;
            // The limit has not been reached: the writer may have opened new segments.
            if stop == ReadStop::BudgetReached || !self.sync_with_disk()? {
                break;
            }
        }
        Ok(out)
    }

    /// Brings the segment list in line with the disk if the last check is
    /// older than [`DISK_CHECK_INTERVAL`]. Cheap otherwise: a clock reading.
    pub(crate) fn sync_with_disk_if_due(&mut self) -> Result<()> {
        if self.last_disk_check.elapsed() >= DISK_CHECK_INTERVAL {
            self.sync_with_disk()?;
        }
        Ok(())
    }

    /// Base offsets of the segments this reader has open.
    #[cfg(test)]
    pub(crate) fn open_segment_bases(&self) -> Vec<u64> {
        self.segments
            .iter()
            .map(SegmentReader::base_offset)
            .collect()
    }

    /// Brings the segment list in line with the disk: forgets those that
    /// retention deleted (their file is then actually freed) and
    /// opens those the writer created. Returns `true` if there were new ones.
    fn sync_with_disk(&mut self) -> Result<bool> {
        self.last_disk_check = Instant::now();
        let bases = list_segment_bases(&self.dir)?;
        // Retention only deletes the oldest ones: everything before
        // the first present segment is gone.
        if let Some(&first) = bases.first() {
            self.segments.retain(|s| s.base_offset() >= first);
        }

        let last_known = self.segments.last().map(SegmentReader::base_offset);
        let mut found = false;
        for base in bases {
            if last_known.is_some_and(|last| base <= last) {
                continue;
            }
            match SegmentReader::open(&self.dir, base) {
                Ok(segment) => {
                    self.segments.push(segment);
                    found = true;
                }
                // Deleted between listing the directory and opening it.
                Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(found)
    }
}
