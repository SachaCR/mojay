//! The writer side of a partition: only the last segment (the active segment)
//! receives writes.

use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::retention::{self, SealedSegment};
use super::{
    FsyncPolicy, LogConfig, list_segment_bases, read_across, remove_orphan_indexes,
    segment_log_path,
};
use crate::durable::sync_dir;
use crate::error::{Error, Result, corrupt};
use crate::partition_state::{SharedState, WriterState};
use crate::record::{self, Message, Record};
use crate::segment::Segment;

/// The append-only log of a partition, stored in its own directory.
#[derive(Debug)]
pub struct PartitionLog {
    dir: PathBuf,
    config: LogConfig,
    /// Sorted by base offset; the last one is the active segment. Never empty.
    segments: Vec<Segment>,
}

impl PartitionLog {
    /// Opens the log in directory `dir`, creating it if needed.
    ///
    /// The end of the active segment is repaired if a write was interrupted.
    /// Returns [`Error::Corrupt`] if a sealed segment is unreadable.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self> {
        config.validate()?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let bases = list_segment_bases(&dir)?;
        let segments = if bases.is_empty() {
            let first = Segment::create(&dir, 0, config.index_interval_bytes)?;
            first.sync()?;
            sync_dir(&dir)?;
            vec![first]
        } else {
            let last = bases.len() - 1;
            bases
                .iter()
                .enumerate()
                .map(|(i, &base)| Segment::open(&dir, base, config.index_interval_bytes, i == last))
                .collect::<Result<_>>()?
        };
        check_continuity(&dir, &segments)?;
        // From the open segments, including a freshly created first segment.
        let bases: Vec<u64> = segments.iter().map(Segment::base_offset).collect();
        remove_orphan_indexes(&dir, &bases)?;
        let mut log = Self {
            dir,
            config,
            segments,
        };
        log.enforce_retention()?;
        Ok(log)
    }

    /// First offset still available.
    pub fn start_offset(&self) -> u64 {
        self.segments[0].base_offset()
    }

    /// Offset that the next appended message will receive.
    pub fn next_offset(&self) -> u64 {
        self.active().next_offset()
    }

    /// What to publish in the shared partition state after a change, for the
    /// other writers and for readers.
    pub(crate) fn writer_state(&self) -> WriterState {
        WriterState {
            next_offset: self.next_offset(),
            end_position: self.active().size(),
            active_base: self.active().base_offset(),
            start_offset: self.start_offset(),
        }
    }

    /// Brings this view of the partition up to date with what other writers
    /// did since our last change, as described by `shared`.
    ///
    /// Must be called under the partition's write lock, with the state read
    /// there. Usually a few comparisons and one `fstat`; the partition is
    /// reopened from disk (with the usual tail repair) when another writer
    /// started a new segment, or died in the middle of a change.
    pub(crate) fn catch_up(&mut self, shared: &SharedState) -> Result<()> {
        let expected = shared.state;
        if shared.dirty || expected.active_base != self.active().base_offset() {
            return self.reload();
        }
        // Same active segment: its length must be the published one, or
        // something happened that the state does not describe.
        if self.active().file_len()? != expected.end_position {
            return self.reload();
        }
        // Segments deleted by another writer's retention.
        self.segments
            .retain(|s| s.base_offset() >= expected.start_offset);
        if (self.active().size(), self.next_offset())
            != (expected.end_position, expected.next_offset)
        {
            self.active_mut()
                .catch_up(expected.end_position, expected.next_offset)?;
        }
        Ok(())
    }

    /// Reopens the partition from disk, repairing an interrupted write.
    fn reload(&mut self) -> Result<()> {
        *self = Self::open(&self.dir, self.config.clone())?;
        Ok(())
    }

    /// Appends a message and returns its offset.
    pub fn append(&mut self, message: &Message) -> Result<u64> {
        self.append_batch(std::slice::from_ref(message))
            .map(|range| range.start)
    }

    /// Appends messages in order and returns the range of assigned offsets.
    ///
    /// All messages are validated before the first write: an invalid
    /// message leaves none of them written. With [`FsyncPolicy::Always`], the whole
    /// batch is on disk on return.
    pub fn append_batch(&mut self, messages: &[Message]) -> Result<Range<u64>> {
        let range = self.append_unsynced(messages)?;
        if self.config.fsync == FsyncPolicy::Always {
            self.active().sync()?;
        }
        Ok(range)
    }

    /// Like [`append_batch`](Self::append_batch), but without the final
    /// `fsync` of the `Always` policy: the caller flushes later, possibly
    /// together with other writers' messages (group commit). A segment sealed
    /// along the way is still flushed before the next one is created.
    pub(crate) fn append_unsynced(&mut self, messages: &[Message]) -> Result<Range<u64>> {
        let sizes = messages
            .iter()
            .map(record::validate)
            .collect::<Result<Vec<_>>>()?;
        let first_offset = self.next_offset();
        let timestamp = record::now_millis();

        let mut pending = PendingWrite::default();
        let mut rolled = false;
        for (message, size) in messages.iter().zip(sizes) {
            if self.needs_roll(&pending, size) {
                self.write_pending(&mut pending)?;
                self.roll()?;
                rolled = true;
            }
            let offset = self.active().next_offset() + pending.sizes.len() as u64;
            let ts = message.timestamp.unwrap_or(timestamp);
            record::encode(message, offset, ts, &mut pending.bytes);
            pending.sizes.push(size);
        }
        self.write_pending(&mut pending)?;

        // A new sealed segment may have aged; size, on the other hand, is
        // checked on each append, from the sizes known in memory.
        if rolled || self.exceeds_max_bytes() {
            self.enforce_retention()?;
        }
        Ok(first_offset..self.next_offset())
    }

    /// Deletes the oldest sealed segments that exceed the configured
    /// retention, and returns how many were deleted.
    ///
    /// Called on open and on each new segment. The age limit
    /// advances with time, without any write: calling this method
    /// regularly also applies it during quiet periods.
    pub fn enforce_retention(&mut self) -> Result<usize> {
        let retention = self.config.retention;
        if retention.is_unlimited() {
            return Ok(0);
        }
        let (active, sealed) = self
            .segments
            .split_last()
            .expect("a log always has an active segment");
        let sealed = sealed
            .iter()
            .map(|s| {
                Ok(SealedSegment {
                    size: s.size(),
                    last_write: match retention.max_age {
                        Some(_) => Some(s.last_write()?),
                        None => None,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let count =
            retention::segments_to_delete(&sealed, active.size(), &retention, SystemTime::now());
        if count == 0 {
            return Ok(0);
        }
        // Oldest first: on failure, what remains is always
        // a continuous sequence of segments.
        for segment in self.segments.drain(..count) {
            segment.delete()?;
        }
        sync_dir(&self.dir)?;
        Ok(count)
    }

    /// Reads records starting from `offset`, without exceeding `max_bytes`
    /// (encoded size). Always returns at least one record if one
    /// exists, even if larger than `max_bytes`, so as never to block a reader.
    ///
    /// Reading at [`next_offset`](Self::next_offset) returns an empty list.
    /// Returns [`Error::OffsetOutOfRange`] outside `start_offset..=next_offset`.
    pub fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<Record>> {
        let (start, end) = (self.start_offset(), self.next_offset());
        if offset < start || offset > end {
            return Err(Error::OffsetOutOfRange { offset, start, end });
        }
        let bases: Vec<u64> = self.segments.iter().map(Segment::base_offset).collect();
        let mut out = Vec::new();
        let mut used = 0;
        read_across(
            &bases,
            offset,
            end,
            max_bytes,
            &mut used,
            &mut out,
            |i, req, out| self.segments[i].read(req, out),
        )?;
        Ok(out)
    }

    fn exceeds_max_bytes(&self) -> bool {
        self.config.retention.max_bytes.is_some_and(|max| {
            // Only one segment: nothing to delete, the active segment stays.
            self.segments.len() > 1 && self.segments.iter().map(Segment::size).sum::<u64>() > max
        })
    }

    /// Forces everything appended so far to disk.
    pub fn sync(&self) -> Result<()> {
        self.active().sync()
    }

    fn active(&self) -> &Segment {
        self.segments
            .last()
            .expect("a log always has an active segment")
    }

    fn active_mut(&mut self) -> &mut Segment {
        self.segments
            .last_mut()
            .expect("a log always has an active segment")
    }

    /// Must a new segment be opened before appending a record of `size` bytes?
    fn needs_roll(&self, pending: &PendingWrite, size: usize) -> bool {
        let active = self.active();
        let has_records = !active.is_empty() || !pending.sizes.is_empty();
        let would_exceed =
            active.size() + (pending.bytes.len() + size) as u64 > self.config.segment_max_bytes;
        // The index's relative offsets are u32s.
        let count = active.next_offset() - active.base_offset() + pending.sizes.len() as u64;
        has_records && (would_exceed || count >= u64::from(u32::MAX))
    }

    fn write_pending(&mut self, pending: &mut PendingWrite) -> Result<()> {
        if !pending.sizes.is_empty() {
            self.active_mut().append(&pending.bytes, &pending.sizes)?;
            pending.bytes.clear();
            pending.sizes.clear();
        }
        Ok(())
    }

    /// Seals the active segment and opens a new one.
    fn roll(&mut self) -> Result<()> {
        // A sealed segment is never repaired again: it must be complete on disk.
        self.active().sync()?;
        let base = self.next_offset();
        let segment = Segment::create(&self.dir, base, self.config.index_interval_bytes)?;
        segment.sync()?;
        sync_dir(&self.dir)?;
        self.segments.push(segment);
        Ok(())
    }
}

/// Encoded records, not yet written to the active segment.
#[derive(Default)]
struct PendingWrite {
    bytes: Vec<u8>,
    sizes: Vec<usize>,
}

/// Each segment must end exactly where the next one begins:
/// otherwise offsets are missing or overlap.
fn check_continuity(dir: &Path, segments: &[Segment]) -> Result<()> {
    for pair in segments.windows(2) {
        let (current, next) = (&pair[0], &pair[1]);
        if current.next_offset() != next.base_offset() {
            return Err(corrupt(
                segment_log_path(dir, current.base_offset()),
                format!(
                    "segment ends at offset {} but the next one starts at {}",
                    current.next_offset(),
                    next.base_offset()
                ),
            ));
        }
    }
    Ok(())
}
