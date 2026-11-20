//! Partition state shared between processes.
//!
//! A topic's `partitions.state` file holds one fixed-size slot per partition:
//!
//! | Field          | Meaning                                                        |
//! |----------------|----------------------------------------------------------------|
//! | `read_limit`   | First offset readers may not read yet. Only ever grows.        |
//! | `end_position` | Number of valid bytes in the active segment, header included.  |
//! | `active_base`  | Base offset of the active segment.                             |
//! | `start_offset` | First available offset (base of the oldest segment).           |
//! | `dirty`        | Non-zero while a writer is modifying the partition's files.    |
//! | `next_offset`  | Offset the next written message will get.                      |
//!
//! Readers only use `read_limit`. Writers publish the other fields while
//! holding the partition's write lock: whatever they read there was published
//! by the previous lock holder, so their view of the files can catch up with
//! appends made by other processes. `dirty` is set before touching any file
//! and cleared when publishing: if a writer dies in between, the next one
//! sees it and recovers the partition from disk.
//!
//! `next_offset` and `read_limit` are separate for group commit: messages are
//! written (and `next_offset` published) under the write lock, and only become
//! readable once flushed, which happens after the lock is released and can
//! cover several writers' messages at once. `read_limit` therefore trails
//! `next_offset` and only moves forward (`advance_read_limit`).
//!
//! The file is memory-mapped (shared `mmap`) by every process: publishing or
//! reading a field is a simple atomic operation in memory, with no system call.
//!
//! This is the only module where `unsafe` is allowed (see AGENT.md).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapOptions, MmapRaw};

use crate::error::{Error, Result, corrupt};
use crate::segment::HEADER_SIZE;

/// Fields per slot. Eight `u64`s: six used, the rest reserved.
const FIELDS: usize = 8;
const SLOT_SIZE: usize = FIELDS * std::mem::size_of::<u64>();

const READ_LIMIT: usize = 0;
const END_POSITION: usize = 1;
const ACTIVE_BASE: usize = 2;
const START_OFFSET: usize = 3;
const DIRTY: usize = 4;
const NEXT_OFFSET: usize = 5;

/// What a writer publishes about a partition after each change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriterState {
    pub(crate) next_offset: u64,
    pub(crate) end_position: u64,
    pub(crate) active_base: u64,
    pub(crate) start_offset: u64,
}

/// A slot as a writer reads it, under the partition's write lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SharedState {
    pub(crate) state: WriterState,
    /// A writer started modifying the files and never published.
    pub(crate) dirty: bool,
}

/// Creates the file for `partitions` empty partitions: nothing readable, an
/// active segment at base 0 holding only its header.
pub(crate) fn create(path: &Path, partitions: u32) -> Result<()> {
    let mut slot = [0u64; FIELDS];
    slot[END_POSITION] = HEADER_SIZE;
    let bytes: Vec<u8> = (0..partitions)
        .flat_map(|_| slot.iter().flat_map(|field| field.to_ne_bytes()))
        .collect();
    let mut file = File::create_new(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

/// The mapped file, shared by readers and writers. Always read-write, even
/// for a reader: an atomic reference requires writable memory (see `field`).
#[derive(Debug)]
pub(crate) struct PartitionStates {
    map: MmapRaw,
    partitions: u32,
}

impl PartitionStates {
    pub(crate) fn open(path: &Path, partitions: u32) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let expected = partitions as usize * SLOT_SIZE;
        let len = file.metadata()?.len();
        if partitions == 0 || len != expected as u64 {
            return Err(corrupt(
                path,
                format!("{len} bytes, expected {expected} for {partitions} partitions"),
            ));
        }
        let map = MmapOptions::new().len(expected).map_raw(&file)?;
        Ok(Self { map, partitions })
    }

    /// Read limit of `partition`. The *Acquire* ordering guarantees that
    /// data written before publication is visible after this read.
    pub(crate) fn read_limit(&self, partition: u32) -> Result<u64> {
        Ok(self.field(partition, READ_LIMIT)?.load(Ordering::Acquire))
    }

    /// Offset the next written message will get. Readable without the write
    /// lock: every message below it is fully written (not necessarily flushed).
    pub(crate) fn next_offset(&self, partition: u32) -> Result<u64> {
        Ok(self.field(partition, NEXT_OFFSET)?.load(Ordering::Acquire))
    }

    /// Base of the active segment. Read without the write lock, *after*
    /// `next_offset`: it is then at least as recent as that offset.
    pub(crate) fn active_base(&self, partition: u32) -> Result<u64> {
        Ok(self.field(partition, ACTIVE_BASE)?.load(Ordering::Acquire))
    }

    /// The writers' fields. Only meaningful under the partition's write lock.
    pub(crate) fn load(&self, partition: u32) -> Result<SharedState> {
        let get =
            |index| -> Result<u64> { Ok(self.field(partition, index)?.load(Ordering::Acquire)) };
        Ok(SharedState {
            state: WriterState {
                next_offset: get(NEXT_OFFSET)?,
                end_position: get(END_POSITION)?,
                active_base: get(ACTIVE_BASE)?,
                start_offset: get(START_OFFSET)?,
            },
            dirty: get(DIRTY)? != 0,
        })
    }

    /// Records that the lock holder is about to modify the partition's files.
    pub(crate) fn mark_dirty(&self, partition: u32) -> Result<()> {
        self.field(partition, DIRTY)?.store(1, Ordering::Release);
        Ok(())
    }

    /// Publishes the writers' state and clears `dirty`, under the write lock.
    /// `next_offset` is stored last: whoever sees it also sees the active
    /// segment it belongs to. The read limit is not touched.
    pub(crate) fn publish(&self, partition: u32, state: &WriterState) -> Result<()> {
        self.field(partition, END_POSITION)?
            .store(state.end_position, Ordering::Release);
        self.field(partition, ACTIVE_BASE)?
            .store(state.active_base, Ordering::Release);
        self.field(partition, START_OFFSET)?
            .store(state.start_offset, Ordering::Release);
        self.field(partition, DIRTY)?.store(0, Ordering::Release);
        self.field(partition, NEXT_OFFSET)?
            .store(state.next_offset, Ordering::Release);
        Ok(())
    }

    /// Makes every offset below `limit` readable, unless a later limit was
    /// already published: the read limit never moves back. *Release*: a
    /// reader that sees the new limit also sees every log write made before.
    pub(crate) fn advance_read_limit(&self, partition: u32, limit: u64) -> Result<()> {
        self.field(partition, READ_LIMIT)?
            .fetch_max(limit, Ordering::Release);
        Ok(())
    }

    /// Sets the read limit, possibly lower: only after repairing a partition
    /// on open, when messages beyond it turned out to be lost.
    pub(crate) fn set_read_limit(&self, partition: u32, limit: u64) -> Result<()> {
        self.field(partition, READ_LIMIT)?
            .store(limit, Ordering::Release);
        Ok(())
    }

    fn field(&self, partition: u32, index: usize) -> Result<&AtomicU64> {
        if partition >= self.partitions {
            return Err(Error::UnknownPartition {
                partition,
                partitions: self.partitions,
            });
        }
        debug_assert!(index < FIELDS);
        let ptr = self.map.as_mut_ptr().cast::<u64>();
        let offset = partition as usize * FIELDS + index;
        // SAFETY:
        // - Alignment: a mapping starts on a page boundary, and each field is
        //   at a multiple of 8 bytes from the start.
        // - Bounds: `partition < partitions` and `index < FIELDS`, and the
        //   mapping is exactly `partitions * SLOT_SIZE` bytes (checked when
        //   opening), so `offset` is inside it.
        // - Lifetime: the reference borrows `self`, so the mapping lives at
        //   least as long as it does.
        // - Writable memory: the mapping is always read-write.
        // - Concurrent access: in every process, these bytes are only read and
        //   written through `AtomicU64`s (this module), never through
        //   `read`/`write` once the file has been created.
        Ok(unsafe { AtomicU64::from_ptr(ptr.add(offset)) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn states(partitions: u32) -> (tempfile::TempDir, PartitionStates) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("partitions.state");
        create(&path, partitions).unwrap();
        let states = PartitionStates::open(&path, partitions).unwrap();
        (dir, states)
    }

    #[test]
    fn new_partitions_are_empty_and_clean() {
        let (_dir, states) = states(2);
        let slot = states.load(1).unwrap();
        assert!(!slot.dirty);
        assert_eq!(
            slot.state,
            WriterState {
                next_offset: 0,
                end_position: HEADER_SIZE,
                active_base: 0,
                start_offset: 0,
            }
        );
    }

    #[test]
    fn publish_is_seen_by_another_mapping_and_clears_dirty() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("partitions.state");
        create(&path, 3).unwrap();
        let writer = PartitionStates::open(&path, 3).unwrap();
        let reader = PartitionStates::open(&path, 3).unwrap();

        writer.mark_dirty(2).unwrap();
        assert!(reader.load(2).unwrap().dirty);
        let state = WriterState {
            next_offset: 42,
            end_position: 4096,
            active_base: 30,
            start_offset: 10,
        };
        writer.publish(2, &state).unwrap();
        assert_eq!(
            reader.load(2).unwrap(),
            SharedState {
                state,
                dirty: false
            }
        );
        assert_eq!(reader.next_offset(2).unwrap(), 42);
        assert_eq!(
            reader.read_limit(2).unwrap(),
            0,
            "publishing does not make anything readable"
        );
        assert_eq!(reader.next_offset(0).unwrap(), 0, "other slots untouched");
    }

    #[test]
    fn read_limit_only_moves_forward_unless_set_after_a_repair() {
        let (_dir, states) = states(1);
        states.advance_read_limit(0, 10).unwrap();
        states.advance_read_limit(0, 7).unwrap();
        assert_eq!(
            states.read_limit(0).unwrap(),
            10,
            "an older flush never moves it back"
        );
        states.advance_read_limit(0, 12).unwrap();
        assert_eq!(states.read_limit(0).unwrap(), 12);
        states.set_read_limit(0, 9).unwrap();
        assert_eq!(states.read_limit(0).unwrap(), 9);
    }

    #[test]
    fn unknown_partition_is_an_error_not_a_crash() {
        let (_dir, states) = states(2);
        assert!(matches!(
            states.read_limit(2),
            Err(Error::UnknownPartition { partition: 2, .. })
        ));
    }

    #[test]
    fn file_of_the_wrong_size_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("partitions.state");
        create(&path, 2).unwrap();
        assert!(matches!(
            PartitionStates::open(&path, 3),
            Err(Error::Corrupt { .. })
        ));
    }
}
