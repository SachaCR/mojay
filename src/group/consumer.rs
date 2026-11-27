//! A member of a consumer group: it owns partitions, reads them, and
//! commits its position in each one. It shares the partitions with the
//! other live members, with no coordinator.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use super::config::{ConsumerConfig, StartFrom};
use super::membership::Membership;
use super::{GroupPaths, offsets, quota};
use crate::error::{Error, Result};
use crate::lock::{FileLock, WriteLock};
use crate::record::{Header, Record};
use crate::topic::TopicReader;

/// Delay between two rebalances: this is also the reaction time to
/// a member joining or leaving.
const REBALANCE_INTERVAL: Duration = Duration::from_millis(200);
/// Wait between two checks when there is nothing to read: it doubles
/// from `MIN_WAIT` to `MAX_WAIT` as long as nothing arrives.
const MIN_WAIT: Duration = Duration::from_millis(1);
const MAX_WAIT: Duration = Duration::from_millis(50);
/// Estimated overhead of a record, on top of its key, value and headers.
const RECORD_OVERHEAD: usize = 32;

/// A message read by a consumer, with its origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    /// The topic the message was read from.
    pub topic: String,
    /// Its partition in that topic.
    pub partition: u32,
    /// Its offset in that partition.
    pub offset: u64,
    /// Milliseconds since the epoch: given by the producer, or the local
    /// clock at append time.
    pub timestamp: i64,
    /// The message key, as sent.
    pub key: Option<Vec<u8>>,
    /// The message value, as sent.
    pub value: Option<Vec<u8>>,
    /// The message headers, in order.
    pub headers: Vec<Header>,
}

impl ConsumerRecord {
    fn new(topic: &str, partition: u32, record: Record) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
            offset: record.offset,
            timestamp: record.timestamp,
            key: record.key,
            value: record.value,
            headers: record.headers,
        }
    }

    fn approximate_size(&self) -> usize {
        let len = |b: &Option<Vec<u8>>| b.as_ref().map_or(0, Vec::len);
        let headers: usize = self
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.len())
            .sum();
        RECORD_OVERHEAD + len(&self.key) + len(&self.value) + headers
    }
}

/// A change in the partitions owned by a consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceEvent {
    /// The consumer now owns this partition.
    Assigned {
        /// The topic of the partition.
        topic: String,
        /// The partition number in that topic.
        partition: u32,
    },
    /// The consumer handed this partition over to another group member.
    Revoked {
        /// The topic of the partition.
        topic: String,
        /// The partition number in that topic.
        partition: u32,
    },
}

/// A topic the consumer is subscribed to.
#[derive(Debug)]
struct Subscription {
    name: String,
    reader: TopicReader,
}

/// A partition owned by this consumer: it holds its lock.
#[derive(Debug)]
struct OwnedPartition {
    /// Index into `Consumer::topics`.
    topic: usize,
    partition: u32,
    /// Next offset to return.
    position: u64,
    /// Last committed offset, as it is on disk.
    committed: Option<u64>,
    _lock: FileLock,
}

/// A member of a consumer group.
///
/// The live members of a group share its partitions: each one
/// owns those whose lock it holds, and only it reads them for the group.
/// On each `poll` (at most every 200 ms), a member counts the live members,
/// gives up what it has in excess and takes free partitions if it is short.
///
/// *At-least-once* delivery: after a crash, messages returned since the
/// last commit are read again. A member that stops calling `poll` keeps its
/// partitions for as long as it lives (see [`poll`](Self::poll)).
///
/// Members of the same group must subscribe to the same topics: otherwise the
/// assignment stays correct (never two readers per partition), but is no
/// longer balanced.
#[derive(Debug)]
pub struct Consumer {
    group: String,
    paths: GroupPaths,
    config: ConsumerConfig,
    topics: Vec<Subscription>,
    /// Sorted by (topic, partition).
    owned: Vec<OwnedPartition>,
    /// Partition to start the next `poll` with, so that all
    /// partitions are served in turn.
    next_to_serve: usize,
    events: Vec<RebalanceEvent>,
    last_rebalance: Option<Instant>,
    last_commit: Instant,
    /// Taken while writing a commit: members of the group share the
    /// offsets file of each topic.
    offsets_lock: WriteLock,
    /// Declared after `owned`: partitions are released before leaving the group.
    membership: Membership,
}

impl Consumer {
    pub(crate) fn open(
        groups_dir: &Path,
        group: &str,
        readers: Vec<TopicReader>,
        config: ConsumerConfig,
    ) -> Result<Self> {
        if readers.is_empty() {
            return Err(Error::InvalidConfig(
                "a consumer must subscribe to at least one topic".into(),
            ));
        }
        let mut topics: Vec<Subscription> = readers
            .into_iter()
            .map(|reader| Subscription {
                name: reader.topic().to_string(),
                reader,
            })
            .collect();
        // The same order on every member: it determines the partition order.
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        if topics.windows(2).any(|w| w[0].name == w[1].name) {
            return Err(Error::InvalidConfig(
                "duplicate topic in subscription".into(),
            ));
        }

        let paths = GroupPaths::open(groups_dir, group)?;
        let offsets_lock = WriteLock::open(&paths.offsets_lock())?;
        let membership = Membership::join(&paths.dir)?;
        let mut consumer = Self {
            group: group.to_string(),
            paths,
            config,
            topics,
            owned: Vec::new(),
            next_to_serve: 0,
            events: Vec::new(),
            last_rebalance: None,
            last_commit: Instant::now(),
            offsets_lock,
            membership,
        };
        consumer.rebalance()?;
        Ok(consumer)
    }

    /// The name of the group this consumer is a member of.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The partitions owned by this consumer, sorted.
    pub fn assignment(&self) -> Vec<(String, u32)> {
        self.owned
            .iter()
            .map(|o| (self.topics[o.topic].name.clone(), o.partition))
            .collect()
    }

    /// The next offset this consumer will return for a partition it owns.
    pub fn position(&self, topic: &str, partition: u32) -> Option<u64> {
        self.owned
            .iter()
            .find(|o| o.partition == partition && self.topics[o.topic].name == topic)
            .map(|o| o.position)
    }

    /// The partitions received or handed over since the last call, in order.
    ///
    /// With manual commit, a handed-over partition resumes at its new
    /// owner from the last commit: committing before each `poll` avoids these re-reads.
    pub fn take_rebalance_events(&mut self) -> Vec<RebalanceEvent> {
        std::mem::take(&mut self.events)
    }

    /// Waits at most `timeout` for messages, and returns them as soon as there are any.
    ///
    /// Returns an empty list if nothing arrived in time. The partitions of
    /// each `poll` are served in turn, so that none is starved.
    ///
    /// `poll` is also when this member takes part in the group: there is no
    /// background thread. At most every 200 ms, while it runs, the member
    /// gives up its surplus partitions to newcomers and takes free ones.
    /// A member that stops calling `poll` keeps its partitions, and a new
    /// member gets none of them until it polls again, closes or dies. Call
    /// `poll` regularly, even with a short timeout, or drop the consumer
    /// when it stops reading.
    ///
    /// # Errors
    ///
    /// - [`Error::Corrupt`] if a partition's data is unreadable.
    /// - [`Error::Io`] on a file system error, including an automatic
    ///   commit that fails.
    pub fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        self.auto_commit_if_due()?;
        let deadline = Instant::now() + timeout;
        let mut wait = MIN_WAIT;
        loop {
            if self.rebalance_is_due() {
                self.rebalance()?;
            }
            // Even with nothing to read: an idle consumer must not keep
            // segments deleted by retention open.
            for topic in &mut self.topics {
                topic.reader.release_deleted_segments()?;
            }
            let records = self.fetch()?;
            if !records.is_empty() {
                return Ok(records);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(records);
            }
            thread::sleep(wait.min(left));
            wait = (wait * 2).min(MAX_WAIT);
        }
    }

    /// Commits the position of each owned partition: all messages
    /// already returned by `poll` are considered processed.
    ///
    /// The offsets are written atomically and durably, with one write per
    /// subscribed topic whatever the number of partitions. Only partitions
    /// still owned are committed: a handed-over partition belongs to its
    /// new owner.
    ///
    /// # Errors
    ///
    /// - [`Error::Corrupt`] if the group's offsets file is damaged.
    /// - [`Error::Io`] on a file system error.
    pub fn commit(&mut self) -> Result<()> {
        let pending: Vec<usize> = (0..self.owned.len())
            .filter(|&i| self.owned[i].committed != Some(self.owned[i].position))
            .collect();
        self.commit_owned(&pending)?;
        self.last_commit = Instant::now();
        Ok(())
    }

    /// Commits the position of `self.owned[i]` for each `i` of `indices`:
    /// one rewrite of `offsets/<topic>` per topic involved, under the
    /// group's offsets lock so that other members' entries are kept.
    fn commit_owned(&mut self, indices: &[usize]) -> Result<()> {
        if indices.is_empty() {
            return Ok(());
        }
        let _guard = self.offsets_lock.acquire()?;
        for (topic, subscription) in self.topics.iter().enumerate() {
            let changes: Vec<(u32, u64)> = indices
                .iter()
                .map(|&i| &self.owned[i])
                .filter(|owned| owned.topic == topic)
                .map(|owned| (owned.partition, owned.position))
                .collect();
            if !changes.is_empty() {
                let path = self.paths.committed_offsets(&subscription.name);
                offsets::update(&path, subscription.reader.partitions(), &changes)?;
            }
        }
        for &i in indices {
            self.owned[i].committed = Some(self.owned[i].position);
        }
        Ok(())
    }

    fn auto_commit_if_due(&mut self) -> Result<()> {
        match self.config.auto_commit {
            Some(every) if self.last_commit.elapsed() >= every => self.commit(),
            _ => Ok(()),
        }
    }

    fn rebalance_is_due(&self) -> bool {
        self.last_rebalance
            .is_none_or(|at| at.elapsed() >= REBALANCE_INTERVAL)
    }

    /// Adjusts the owned partitions to this member's share: gives up the
    /// surplus, then takes free partitions if some are missing.
    fn rebalance(&mut self) -> Result<()> {
        self.last_rebalance = Some(Instant::now());
        let members = self.membership.living_members()?;
        let rank = members
            .iter()
            .position(|m| m == self.membership.id())
            .unwrap_or(members.len());
        let all = self.all_partitions();
        let quota = quota::quota(all.len(), members.len(), rank);

        while self.owned.len() > quota {
            self.release_last()?;
        }
        if self.owned.len() < quota {
            let start = quota::claim_start(all.len(), members.len(), rank);
            for k in 0..all.len() {
                if self.owned.len() >= quota {
                    break;
                }
                let (topic, partition) = all[(start + k) % all.len()];
                self.try_claim(topic, partition)?;
            }
            self.owned.sort_by_key(|o| (o.topic, o.partition));
        }
        Ok(())
    }

    /// All partitions of the subscribed topics, in the same order on every
    /// member (topics sorted by name).
    fn all_partitions(&self) -> Vec<(usize, u32)> {
        self.topics
            .iter()
            .enumerate()
            .flat_map(|(t, sub)| (0..sub.reader.partitions()).map(move |p| (t, p)))
            .collect()
    }

    /// Gives up the last owned partition. With automatic commit, its
    /// position is committed *before* releasing the lock: the next member
    /// resumes exactly there, with no duplicates.
    fn release_last(&mut self) -> Result<()> {
        let last = self.owned.len() - 1;
        if self.config.auto_commit.is_some() {
            self.commit_owned(&[last])?;
        }
        if let Some(owned) = self.owned.pop() {
            self.events.push(RebalanceEvent::Revoked {
                topic: self.topics[owned.topic].name.clone(),
                partition: owned.partition,
            });
            // The lock is released here, by dropping `owned`.
        }
        Ok(())
    }

    /// Takes the partition if it is free, and resumes at its committed offset.
    /// The offset is read *after* taking the lock: the previous member has
    /// finished writing its own before releasing.
    fn try_claim(&mut self, topic: usize, partition: u32) -> Result<()> {
        if self
            .owned
            .iter()
            .any(|o| o.topic == topic && o.partition == partition)
        {
            return Ok(());
        }
        let name = &self.topics[topic].name;
        let Some(lock) = FileLock::try_exclusive(&self.paths.partition_lock(name, partition))?
        else {
            return Ok(());
        };
        let partitions = self.topics[topic].reader.partitions();
        let committed = offsets::read(&self.paths.committed_offsets(name), partitions)?
            .get(partition as usize)
            .copied()
            .flatten();
        let position = self.start_position(topic, partition, committed)?;
        self.events.push(RebalanceEvent::Assigned {
            topic: name.clone(),
            partition,
        });
        self.owned.push(OwnedPartition {
            topic,
            partition,
            position,
            committed,
            _lock: lock,
        });
        Ok(())
    }

    /// Where to resume a partition, based on the committed offset if it is usable.
    fn start_position(&self, topic: usize, partition: u32, committed: Option<u64>) -> Result<u64> {
        let reader = &self.topics[topic].reader;
        let start = reader.start_offset(partition)?;
        let limit = reader.read_limit(partition)?;
        Ok(match committed {
            // Deleted by retention: apply the start policy.
            Some(c) if c < start => self.policy_position(start, limit),
            // Beyond the limit (messages lost with a non-guaranteeing fsync
            // policy): resume at the limit, without skipping any new message.
            Some(c) => c.min(limit),
            None => self.policy_position(start, limit),
        })
    }

    fn policy_position(&self, start: u64, limit: u64) -> u64 {
        match self.config.start_from {
            StartFrom::Earliest => start,
            StartFrom::Latest => limit,
        }
    }

    /// Reads what is available in the owned partitions, in turn,
    /// within the `max_poll_bytes` limit.
    fn fetch(&mut self) -> Result<Vec<ConsumerRecord>> {
        let count = self.owned.len();
        let mut out = Vec::new();
        let mut budget = self.config.max_poll_bytes;
        for k in 0..count {
            if !out.is_empty() && budget == 0 {
                break;
            }
            let i = (self.next_to_serve + k) % count;
            let (topic, partition) = (self.owned[i].topic, self.owned[i].partition);
            let records = match self.read_owned(i, budget) {
                Err(Error::OffsetOutOfRange { .. }) => {
                    // Retention deleted the position: apply the start policy.
                    self.owned[i].position = self.start_position(topic, partition, None)?;
                    continue;
                }
                other => other?,
            };
            for record in records {
                let record = ConsumerRecord::new(&self.topics[topic].name, partition, record);
                budget = budget.saturating_sub(record.approximate_size());
                self.owned[i].position = record.offset + 1;
                out.push(record);
            }
        }
        if count > 0 {
            self.next_to_serve = (self.next_to_serve + 1) % count;
        }
        Ok(out)
    }

    /// Takes back records that `poll` returned but that never reached the
    /// caller (an async `poll` cancelled while it ran): each owned partition
    /// resumes at the first of these records, as if `poll` had not returned
    /// them. They are read again, and no commit can skip them. Records of a
    /// partition no longer owned are ignored: its new owner resumes from the
    /// last commit.
    #[cfg(any(feature = "async", test))]
    pub(crate) fn rewind(&mut self, records: &[ConsumerRecord]) {
        for record in records {
            let owned = self.owned.iter_mut().find(|o| {
                o.partition == record.partition && self.topics[o.topic].name == record.topic
            });
            if let Some(owned) = owned {
                owned.position = owned.position.min(record.offset);
            }
        }
    }

    fn read_owned(&mut self, i: usize, budget: usize) -> Result<Vec<Record>> {
        let owned = &self.owned[i];
        let reader = &mut self.topics[owned.topic].reader;
        if owned.position >= reader.read_limit(owned.partition)? {
            return Ok(Vec::new());
        }
        reader.read(owned.partition, owned.position, budget)
    }
}

impl Drop for Consumer {
    /// With automatic commit, commits before releasing the partitions:
    /// the next member resumes exactly where this one stopped.
    /// The partitions are then released, then the member leaves the group.
    fn drop(&mut self) {
        if self.config.auto_commit.is_some() {
            // An error here can no longer be returned: at worst, messages
            // since the last commit will be read again (at-least-once).
            let _ = self.commit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Db, FsyncPolicy, LogConfig, Message, Retention, TopicConfig};

    /// Small segments whose sealed ones expire after `AGE`.
    const AGE: Duration = Duration::from_millis(300);
    const COUNT: u64 = 30;

    /// A topic whose first partition holds `COUNT` messages over several
    /// segments, all younger than `AGE`.
    ///
    /// Retention also runs on each new segment: if writing took longer than
    /// `AGE`, the first segments would be deleted before any test reads
    /// them. Without `fsync` (not what these tests check), writing takes a
    /// few milliseconds even under load; it used to take about 500 ms with
    /// `Always` while the other tests ran, and a consumer then waited forever
    /// for the deleted messages.
    fn topic_with_segments(partitions: u32) -> (tempfile::TempDir, Db) {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Db::open(dir.path()).unwrap();
        let log = LogConfig {
            segment_max_bytes: 256,
            fsync: FsyncPolicy::Never,
            retention: Retention {
                max_bytes: None,
                max_age: Some(AGE),
            },
            ..LogConfig::default()
        };
        db.create_topic("t", TopicConfig { partitions, log })
            .unwrap();
        let mut producer = db.producer("t").unwrap();
        for i in 0..COUNT {
            producer
                .send_to(0, &Message::new("k", format!("value-{i}")))
                .unwrap();
        }
        assert_eq!(
            db.reader("t").unwrap().start_offset(0).unwrap(),
            0,
            "writing took longer than AGE: retention already deleted messages"
        );
        (dir, db)
    }

    /// Lets the sealed segments expire, then has a producer delete them
    /// (opening it is enough: it applies retention).
    fn expire_sealed_segments(db: &Db) {
        thread::sleep(AGE + Duration::from_millis(100));
        let mut producer = db.producer("t").unwrap();
        producer.enforce_retention().unwrap();
        assert!(db.reader("t").unwrap().start_offset(0).unwrap() > 0);
    }

    #[test]
    fn rewind_returns_to_the_first_undelivered_record_of_each_partition() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Db::open(dir.path()).unwrap();
        let topic = TopicConfig {
            partitions: 2,
            ..TopicConfig::default()
        };
        db.create_topic("t", topic).unwrap();
        let mut producer = db.producer("t").unwrap();
        for i in 0..COUNT {
            producer
                .send_to(0, &Message::new("k", format!("value-{i}")))
                .unwrap();
        }
        producer.send_to(1, &Message::new("k", "on-1")).unwrap();
        let config = ConsumerConfig {
            max_poll_bytes: usize::MAX,
            ..ConsumerConfig::default()
        };
        let mut consumer = db.consumer("g", &["t"], config).unwrap();
        let mut first = Vec::new();
        while first.len() < COUNT as usize + 1 {
            first.extend(consumer.poll(Duration::from_secs(1)).unwrap());
        }
        assert_eq!(consumer.position("t", 0), Some(COUNT));
        assert_eq!(consumer.position("t", 1), Some(1));

        // Only the second half of partition 0 and the partition 1 record were
        // not delivered; a record of an unknown topic is ignored.
        let mut undelivered: Vec<ConsumerRecord> = first
            .iter()
            .filter(|r| r.partition == 1 || r.offset >= COUNT / 2)
            .cloned()
            .collect();
        let mut foreign = undelivered[0].clone();
        foreign.topic = "other".into();
        undelivered.push(foreign);
        consumer.rewind(&undelivered);
        assert_eq!(consumer.position("t", 0), Some(COUNT / 2));
        assert_eq!(consumer.position("t", 1), Some(0));

        let mut again = Vec::new();
        while again.len() < undelivered.len() - 1 {
            again.extend(consumer.poll(Duration::from_secs(1)).unwrap());
        }
        let key = |r: &ConsumerRecord| (r.partition, r.offset);
        let mut expected: Vec<_> = undelivered[..undelivered.len() - 1]
            .iter()
            .map(key)
            .collect();
        let mut got: Vec<_> = again.iter().map(key).collect();
        expected.sort_unstable();
        got.sort_unstable();
        assert_eq!(got, expected);
    }

    #[test]
    fn idle_consumer_releases_segments_deleted_by_retention() {
        let (_dir, db) = topic_with_segments(1);
        let mut consumer = db.consumer("g", &["t"], ConsumerConfig::default()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut read = 0;
        while read < COUNT {
            assert!(Instant::now() < deadline, "only {read} of {COUNT} read");
            read += consumer.poll(Duration::from_secs(1)).unwrap().len() as u64;
        }
        let reader = &consumer.topics[0].reader;
        assert!(reader.open_segment_bases(0).len() > 1);

        expire_sealed_segments(&db);
        // Nothing new to read: the consumer stays at the read limit.
        assert!(
            consumer
                .poll(Duration::from_millis(600))
                .unwrap()
                .is_empty()
        );
        let reader = &consumer.topics[0].reader;
        assert_eq!(
            reader.open_segment_bases(0),
            vec![reader.start_offset(0).unwrap()],
            "only the active segment is still open"
        );
    }

    #[test]
    fn reading_one_partition_releases_deleted_segments_of_the_others() {
        let (_dir, db) = topic_with_segments(2);
        let mut reader = db.reader("t").unwrap();
        assert!(reader.open_segment_bases(0).len() > 1);

        expire_sealed_segments(&db);
        thread::sleep(Duration::from_millis(500));
        assert!(reader.read(1, 0, usize::MAX).unwrap().is_empty());
        assert_eq!(
            reader.open_segment_bases(0),
            vec![reader.start_offset(0).unwrap()]
        );
    }
}
