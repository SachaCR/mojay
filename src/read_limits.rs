//! Read limits shared between processes.
//!
//! A topic's `read_limits` file holds one `u64` per partition: the
//! first offset that readers are not yet allowed to read. The writer
//! publishes it after each append; readers never read beyond it.
//!
//! The file is memory-mapped (shared `mmap`) by every process:
//! publishing or reading a limit is a simple atomic operation in memory,
//! with no system call.
//!
//! This is the only module where `unsafe` is allowed (see AGENT.md).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapOptions, MmapRaw};

use crate::error::{Error, Result, corrupt};

const SLOT_SIZE: usize = std::mem::size_of::<u64>();

/// Creates the file with `partitions` limits, all set to zero.
pub(crate) fn create(path: &Path, partitions: u32) -> Result<()> {
    let mut file = File::create_new(path)?;
    file.write_all(&vec![0u8; partitions as usize * SLOT_SIZE])?;
    file.sync_all()?;
    Ok(())
}

/// The mapped file. Always read-write, even for a reader:
/// an atomic reference requires writable memory (see `slot`).
#[derive(Debug)]
struct Mapping {
    map: MmapRaw,
    partitions: u32,
}

impl Mapping {
    fn open(path: &Path, partitions: u32) -> Result<Self> {
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

    fn slot(&self, partition: u32) -> Result<&AtomicU64> {
        if partition >= self.partitions {
            return Err(Error::UnknownPartition {
                partition,
                partitions: self.partitions,
            });
        }
        let ptr = self.map.as_mut_ptr().cast::<u64>();
        // SAFETY:
        // - Alignment: a mapping starts on a page boundary, and each
        //   slot is at a multiple of 8 bytes from the start.
        // - Bounds: `partition < partitions`, and the mapping is exactly
        //   `partitions * 8` bytes (checked when opening).
        // - Lifetime: the reference borrows `self`, so the mapping lives at
        //   least as long as it does.
        // - Writable memory: the mapping is always read-write.
        // - Concurrent access: in every process, these bytes are only read
        //   and written through `AtomicU64`s (this module), never through
        //   `read`/`write` once the file has been created.
        Ok(unsafe { AtomicU64::from_ptr(ptr.add(partition as usize)) })
    }
}

/// Reader view: reads the limits published by the writer.
#[derive(Debug)]
pub(crate) struct ReadLimits(Mapping);

impl ReadLimits {
    pub(crate) fn open(path: &Path, partitions: u32) -> Result<Self> {
        Mapping::open(path, partitions).map(Self)
    }

    /// Read limit of `partition`. The *Acquire* ordering guarantees that
    /// data written before publication is visible after this read.
    pub(crate) fn get(&self, partition: u32) -> Result<u64> {
        Ok(self.0.slot(partition)?.load(Ordering::Acquire))
    }
}

/// Writer view: publishes the limits. Only one may exist per topic, which
/// the writer lock guarantees.
#[derive(Debug)]
pub(crate) struct ReadLimitsWriter(Mapping);

impl ReadLimitsWriter {
    pub(crate) fn open(path: &Path, partitions: u32) -> Result<Self> {
        Mapping::open(path, partitions).map(Self)
    }

    /// Makes every offset below `limit` readable. The *Release* ordering
    /// guarantees that log writes made before are visible to any
    /// reader that sees the new limit.
    pub(crate) fn publish(&self, partition: u32, limit: u64) -> Result<()> {
        self.0.slot(partition)?.store(limit, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_publishes_and_reader_sees() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("read_limits");
        create(&path, 3).unwrap();
        let writer = ReadLimitsWriter::open(&path, 3).unwrap();
        let reader = ReadLimits::open(&path, 3).unwrap();
        assert_eq!(reader.get(2).unwrap(), 0);
        writer.publish(2, 42).unwrap();
        assert_eq!(reader.get(2).unwrap(), 42);
        assert_eq!(reader.get(0).unwrap(), 0);
    }

    #[test]
    fn unknown_partition_is_an_error_not_a_crash() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("read_limits");
        create(&path, 2).unwrap();
        let reader = ReadLimits::open(&path, 2).unwrap();
        assert!(matches!(
            reader.get(2),
            Err(Error::UnknownPartition { partition: 2, .. })
        ));
    }

    #[test]
    fn file_of_the_wrong_size_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("read_limits");
        create(&path, 2).unwrap();
        assert!(matches!(
            ReadLimits::open(&path, 3),
            Err(Error::Corrupt { .. })
        ));
    }
}
