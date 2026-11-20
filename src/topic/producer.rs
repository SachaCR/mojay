//! A topic's writer. Any number of producers, in any process, can write to the
//! same topic and the same partition: they take turns through each
//! partition's write lock, held only while their messages are written, and
//! share flushes to disk (group commit).

use std::fs::File;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::background_sync::{self, BackgroundSync, SharedLogs};
use super::{FLUSH_LOCK_FILE, STATE_FILE, WRITE_LOCK_FILE, load_config, partition_dir};
use crate::error::{Error, Result};
use crate::lock::WriteLock;
use crate::log::{FsyncPolicy, PartitionLog, Retention, segment_log_path};
use crate::partition_state::PartitionStates;
use crate::partitioner::Partitioner;
use crate::record::Message;

/// Minimum delay between two checks of segment age while
/// sending. The size limit, on the other hand, is applied on each new segment.
const RETENTION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// A partition's files and locks, as seen by one producer.
#[derive(Debug)]
struct PartitionHandles {
    dir: PathBuf,
    /// Held while messages are written (microseconds).
    write_lock: WriteLock,
    /// Held while flushing to disk with `Always` (milliseconds).
    flush_lock: WriteLock,
}

/// A topic's writer.
///
/// Several producers can write to the same topic, in this process or in
/// others. For each batch, a producer:
///
/// 1. takes the partition's write lock, catches up with what other writers
///    appended, writes its messages, publishes the next offset, and releases
///    the lock;
/// 2. with [`FsyncPolicy::Always`], makes the messages durable through group
///    commit: under the partition's flush lock, either another writer's flush
///    already covered them, or one `fsync` covers everything written so far,
///    by every writer;
/// 3. advances the read limit: the messages become readable.
///
/// Offsets in a partition are totally ordered, and each producer's own
/// messages keep their call order. With `Always`, a send returns, and its
/// messages become readable, only once they are on disk.
#[derive(Debug)]
pub struct Producer {
    topic: String,
    partitioner: Partitioner,
    logs: SharedLogs,
    states: PartitionStates,
    partitions: Vec<PartitionHandles>,
    fsync: FsyncPolicy,
    background_sync: Option<BackgroundSync>,
    retention: Retention,
    last_retention_check: Instant,
}

impl Producer {
    pub(crate) fn open(topic_dir: &Path, topic: &str) -> Result<Self> {
        let config = load_config(topic_dir)?;
        let states = PartitionStates::open(&topic_dir.join(STATE_FILE), config.partitions)?;
        let mut partitions = Vec::new();
        let mut logs = Vec::new();
        for partition in 0..config.partitions {
            let dir = partition_dir(topic_dir, partition);
            let handles = PartitionHandles {
                write_lock: WriteLock::open(&dir.join(WRITE_LOCK_FILE))?,
                flush_lock: WriteLock::open(&dir.join(FLUSH_LOCK_FILE))?,
                dir,
            };
            // Opening a log repairs the end of a write interrupted by a crash,
            // which is only safe while no other writer is appending.
            let guard = handles.write_lock.acquire()?;
            states.mark_dirty(partition)?;
            let log = PartitionLog::open(&handles.dir, config.log.clone())?;
            // Messages written by a writer that died before flushing them must
            // be durable before becoming readable.
            if config.log.fsync == FsyncPolicy::Always {
                log.sync()?;
            }
            // The repair may have removed messages: the state follows the actual log.
            states.publish(partition, &log.writer_state())?;
            states.set_read_limit(partition, log.next_offset())?;
            drop(guard);
            partitions.push(handles);
            logs.push(log);
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
            states,
            partitions,
            fsync: config.log.fsync,
            background_sync,
            retention: config.log.retention,
            last_retention_check: Instant::now(),
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partitions(&self) -> u32 {
        self.partitions.len() as u32
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
    /// single lock, `write` and `fsync` for the batch.
    pub fn send_batch_to(&mut self, partition: u32, messages: &[Message]) -> Result<Range<u64>> {
        self.report_background_failure()?;
        let range = self.with_partition(partition, |log| log.append_unsynced(messages))?;
        self.make_readable(partition, range.end)?;

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
        let mut deleted = 0;
        for partition in 0..self.partitions() {
            deleted += self.with_partition(partition, PartitionLog::enforce_retention)?;
        }
        Ok(deleted)
    }

    /// Forces all messages appended by this producer to disk, whatever
    /// the `fsync` policy.
    pub fn flush(&mut self) -> Result<()> {
        self.report_background_failure()?;
        background_sync::sync_all(&self.logs)
    }

    fn handles(&self, partition: u32) -> Result<&PartitionHandles> {
        self.partitions
            .get(partition as usize)
            .ok_or(Error::UnknownPartition {
                partition,
                partitions: self.partitions(),
            })
    }

    /// Runs `change` on a partition's log, under its write lock:
    ///
    /// 1. take the lock (waiting for another writer to finish writing);
    /// 2. catch up with what other writers did since our last change;
    /// 3. mark the partition dirty, so that a crash from here on is detected;
    /// 4. apply the change, then publish the new state (next offset last).
    ///
    /// Nothing becomes readable here: see [`make_readable`](Self::make_readable).
    fn with_partition<T>(
        &self,
        partition: u32,
        change: impl FnOnce(&mut PartitionLog) -> Result<T>,
    ) -> Result<T> {
        let _guard = self.handles(partition)?.write_lock.acquire()?;
        let mut logs = background_sync::lock(&self.logs);
        let log = &mut logs[partition as usize];

        log.catch_up(&self.states.load(partition)?)?;
        self.states.mark_dirty(partition)?;
        match change(log) {
            Ok(value) => {
                self.states.publish(partition, &log.writer_state())?;
                Ok(value)
            }
            // Rejected before anything was written: the state is still exact.
            Err(error @ (Error::RecordTooLarge { .. } | Error::InvalidRecord(_))) => {
                self.states.publish(partition, &log.writer_state())?;
                Err(error)
            }
            // Anything else may have left a partial write: the partition stays
            // dirty, and the next writer (maybe us) reopens it from disk.
            Err(error) => Err(error),
        }
    }

    /// Makes every offset below `end` readable, once durable if the policy
    /// asks for it. Called after the write lock is released.
    ///
    /// With `Always`, this is the group commit: under the flush lock, either a
    /// flush that started after our write already made `end` readable (we
    /// waited for it, and are done without flushing), or we flush the active
    /// segment ourselves. One `fsync` makes durable everything written to the
    /// file before it, by any writer: it covers every offset below the
    /// `next_offset` read just before it, and the read limit moves there.
    fn make_readable(&self, partition: u32, end: u64) -> Result<()> {
        if self.fsync != FsyncPolicy::Always {
            return self.states.advance_read_limit(partition, end);
        }
        let handles = self.handles(partition)?;
        let _guard = handles.flush_lock.acquire()?;
        if self.states.read_limit(partition)? >= end {
            return Ok(());
        }
        // `next_offset` first, then the segment: the segment is at least as
        // recent as that offset. Earlier segments were flushed when sealed.
        let covered = self.states.next_offset(partition)?;
        let active_base = self.states.active_base(partition)?;
        match File::open(segment_log_path(&handles.dir, active_base)) {
            Ok(file) => file.sync_data()?,
            // Sealed (and flushed by the writer that sealed it), then removed
            // by retention since we read its base: nothing left to flush.
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.states.advance_read_limit(partition, covered)
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
