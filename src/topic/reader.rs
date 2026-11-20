//! Reading a topic, lock-free, from any process.

use std::path::Path;

use super::{STATE_FILE, load_config, partition_dir};
use crate::error::{Error, Result};
use crate::log::PartitionReader;
use crate::partition_state::PartitionStates;
use crate::record::Record;

/// A reader of a topic. It takes no lock and modifies nothing:
/// you can open as many as you like while a [`Producer`](crate::Producer)
/// writes in another process.
///
/// It never reads beyond the read limit published by the writer:
/// everything it returns is complete.
#[derive(Debug)]
pub struct TopicReader {
    topic: String,
    partitions: Vec<PartitionReader>,
    states: PartitionStates,
}

impl TopicReader {
    pub(crate) fn open(topic_dir: &Path, topic: &str) -> Result<Self> {
        let config = load_config(topic_dir)?;
        let partitions = (0..config.partitions)
            .map(|p| PartitionReader::open(&partition_dir(topic_dir, p)))
            .collect::<Result<_>>()?;
        let states = PartitionStates::open(&topic_dir.join(STATE_FILE), config.partitions)?;
        Ok(Self {
            topic: topic.to_string(),
            partitions,
            states,
        })
    }

    /// The name of the topic this reader reads.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The number of partitions of the topic, fixed at its creation: valid
    /// partitions are `0..partitions()`.
    pub fn partitions(&self) -> u32 {
        self.partitions.len() as u32
    }

    /// First available offset in `partition`: retention may have deleted
    /// the ones before it.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownPartition`] if the topic has no such partition.
    pub fn start_offset(&self, partition: u32) -> Result<u64> {
        Ok(self.partition(partition)?.start_offset())
    }

    /// First offset not yet readable in `partition`: you can read
    /// `start_offset..read_limit`.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownPartition`] if the topic has no such partition.
    pub fn read_limit(&self, partition: u32) -> Result<u64> {
        self.states.read_limit(partition)
    }

    /// Reads messages from `partition` starting at `offset`, without exceeding
    /// `max_bytes` (encoded size) or the read limit. Always returns
    /// at least one message if there is a readable one, even if larger than `max_bytes`.
    ///
    /// Reading at the read limit returns an empty list: nothing new yet.
    ///
    /// Each call also releases, in every partition, the segments that
    /// retention deleted more than 500 ms ago. A reader that is no longer
    /// called keeps their files open, and their disk space taken, until it
    /// is called again or dropped.
    ///
    /// # Errors
    ///
    /// - [`Error::UnknownPartition`] if the topic has no such partition.
    /// - [`Error::OffsetOutOfRange`] outside `start_offset..=read_limit`.
    /// - [`Error::Corrupt`] if a record before the read limit is unreadable.
    /// - [`Error::Io`] on a file system error.
    pub fn read(&mut self, partition: u32, offset: u64, max_bytes: usize) -> Result<Vec<Record>> {
        self.release_deleted_segments()?;
        let read_limit = self.states.read_limit(partition)?;
        let partitions = self.partitions();
        self.partitions
            .get_mut(partition as usize)
            .ok_or(Error::UnknownPartition {
                partition,
                partitions,
            })?
            .read(offset, max_bytes, read_limit)
    }

    /// Closes the files of the segments that retention deleted, in every
    /// partition last checked more than 500 ms ago: an open file keeps its
    /// disk space taken. Covers the partitions that are never read too.
    pub(crate) fn release_deleted_segments(&mut self) -> Result<()> {
        for partition in &mut self.partitions {
            partition.sync_with_disk_if_due()?;
        }
        Ok(())
    }

    /// Base offsets of the segments this reader has open in `partition`.
    #[cfg(test)]
    pub(crate) fn open_segment_bases(&self, partition: u32) -> Vec<u64> {
        self.partitions[partition as usize].open_segment_bases()
    }

    fn partition(&self, partition: u32) -> Result<&PartitionReader> {
        self.partitions
            .get(partition as usize)
            .ok_or(Error::UnknownPartition {
                partition,
                partitions: self.partitions(),
            })
    }
}
