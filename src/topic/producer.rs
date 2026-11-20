//! A topic's writer: only one per topic, across all processes.

use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::background_sync::{self, BackgroundSync, SharedLogs};
use super::{READ_LIMITS_FILE, WRITER_LOCK_FILE, load_config, partition_dir};
use crate::error::{Error, Result};
use crate::lock::FileLock;
use crate::log::{FsyncPolicy, PartitionLog, Retention};
use crate::partitioner::Partitioner;
use crate::read_limits::ReadLimitsWriter;
use crate::record::Message;

/// Minimum delay between two checks of segment age while
/// sending. The size limit, on the other hand, is applied on each new segment.
const RETENTION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// A topic's writer. It holds the topic lock for as long as it lives: a second
/// producer, in this process or another one, gets [`Error::Busy`].
///
/// Each message becomes readable as soon as it is appended, after the `fsync` if the
/// policy is [`FsyncPolicy::Always`].
#[derive(Debug)]
pub struct Producer {
    topic: String,
    partitioner: Partitioner,
    logs: SharedLogs,
    read_limits: ReadLimitsWriter,
    background_sync: Option<BackgroundSync>,
    retention: Retention,
    last_retention_check: Instant,
    /// Declared last: the lock is only released once everything else is closed.
    _lock: FileLock,
}

impl Producer {
    pub(crate) fn open(topic_dir: &Path, topic: &str) -> Result<Self> {
        let lock = FileLock::try_exclusive(&topic_dir.join(WRITER_LOCK_FILE))?
            .ok_or_else(|| Error::Busy(topic.to_string()))?;
        let config = load_config(topic_dir)?;

        // Opening a log repairs the end of a write interrupted by a crash.
        let logs = (0..config.partitions)
            .map(|p| PartitionLog::open(partition_dir(topic_dir, p), config.log.clone()))
            .collect::<Result<Vec<_>>>()?;
        let read_limits =
            ReadLimitsWriter::open(&topic_dir.join(READ_LIMITS_FILE), config.partitions)?;
        // The repair may have removed messages: the limit follows the actual log.
        for (partition, log) in (0..).zip(&logs) {
            read_limits.publish(partition, log.next_offset())?;
        }

        let logs = Arc::new(Mutex::new(logs));
        let background_sync = match config.log.fsync {
            FsyncPolicy::Interval(every) => Some(BackgroundSync::start(Arc::clone(&logs), every)?),
            FsyncPolicy::Always | FsyncPolicy::Never => None,
        };
        Ok(Self {
            topic: topic.to_string(),
            partitioner: Partitioner::new(config.partitions),
            logs,
            read_limits,
            background_sync,
            retention: config.log.retention,
            last_retention_check: Instant::now(),
            _lock: lock,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partitions(&self) -> u32 {
        background_sync::lock(&self.logs).len() as u32
    }

    /// Appends a message and returns its partition and offset.
    ///
    /// The partition depends on the key (murmur2, like Kafka): two messages with
    /// the same key go to the same partition, in order. Without a key,
    /// messages are distributed round-robin.
    pub fn send(&mut self, message: &Message) -> Result<(u32, u64)> {
        let partition = self.partitioner.partition_for(message.key.as_deref());
        let offset = self.send_to(partition, message)?;
        Ok((partition, offset))
    }

    /// Appends a message to a given partition and returns its offset.
    pub fn send_to(&mut self, partition: u32, message: &Message) -> Result<u64> {
        self.send_batch_to(partition, std::slice::from_ref(message))
            .map(|range| range.start)
    }

    /// Appends messages, in order, to a given partition, and returns
    /// the range of assigned offsets. Faster than sending one by one: a
    /// single `write` and a single `fsync` for the batch.
    pub fn send_batch_to(&mut self, partition: u32, messages: &[Message]) -> Result<Range<u64>> {
        self.report_background_failure()?;
        let mut logs = background_sync::lock(&self.logs);
        let partitions = logs.len() as u32;
        let log = logs
            .get_mut(partition as usize)
            .ok_or(Error::UnknownPartition {
                partition,
                partitions,
            })?;
        let range = log.append_batch(messages)?;
        // Published after the write (and the fsync with `Always`): a reader never
        // sees a message that is not fully written.
        self.read_limits.publish(partition, log.next_offset())?;
        drop(logs);

        if self.retention.max_age.is_some()
            && self.last_retention_check.elapsed() >= RETENTION_CHECK_INTERVAL
        {
            self.enforce_retention()?;
        }
        Ok(range)
    }

    /// Applies retention to all partitions now, and returns the
    /// number of deleted segments.
    ///
    /// It is already applied on open, on each new segment, and
    /// (for the age limit) at most once per second while sending.
    /// A producer that sends nothing can call it regularly.
    pub fn enforce_retention(&mut self) -> Result<usize> {
        self.last_retention_check = Instant::now();
        let mut logs = background_sync::lock(&self.logs);
        logs.iter_mut()
            .map(PartitionLog::enforce_retention)
            .sum::<Result<usize>>()
    }

    /// Forces all appended messages to disk, whatever
    /// the `fsync` policy.
    pub fn flush(&mut self) -> Result<()> {
        self.report_background_failure()?;
        background_sync::sync_all(&self.logs)
    }

    /// An error from the background `fsync` is returned by the next call.
    fn report_background_failure(&self) -> Result<()> {
        match self
            .background_sync
            .as_ref()
            .and_then(BackgroundSync::take_failure)
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        // Stopping the thread does a final fsync. An error at this point can no
        // longer be returned to anyone: the data stays in the OS
        // cache and survives the end of the process.
        if let Some(sync) = self.background_sync.take() {
            let _ = sync.stop();
        }
    }
}
