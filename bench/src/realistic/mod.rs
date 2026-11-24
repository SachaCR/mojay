//! The realistic suite: JSON events with keys and headers (see `payload`),
//! on 8 partitions, with several processes and more data than one segment.
//! Each scenario runs in its own temporary data folder, deleted at the end.

mod ingest;
mod pipeline;
mod storage;

use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mojay::{Db, FsyncPolicy, Header, LogConfig, Message, TopicConfig};
use tempfile::TempDir;

use crate::measurement::Scenario;
use crate::payload::{self, Workload};

pub const TOPIC: &str = "events";
const PARTITIONS: u32 = 8;
/// Carries the time a message was due to be sent, in nanoseconds since the
/// epoch: wall-clock time, so that it means the same in every process.
const SENT_AT_HEADER: &str = "bench-sent-at";
const RATE_SEED: u64 = 2;

/// Every scenario of the suite, in run order.
pub fn all() -> Vec<Scenario> {
    let mut scenarios = ingest::scenarios();
    scenarios.extend(pipeline::scenarios());
    scenarios.extend(storage::scenarios());
    scenarios
}

fn policies() -> [(&'static str, FsyncPolicy); 3] {
    [
        ("always", FsyncPolicy::Always),
        (
            "interval",
            FsyncPolicy::Interval(Duration::from_millis(100)),
        ),
        ("never", FsyncPolicy::Never),
    ]
}

/// A fresh data folder with the `events` topic, 8 partitions.
fn new_topic(log: LogConfig) -> (TempDir, Db) {
    let dir = TempDir::new().expect("temporary folder");
    let db = Db::open(dir.path()).expect("data folder");
    let config = TopicConfig {
        partitions: PARTITIONS,
        log,
    };
    db.create_topic(TOPIC, config).expect("topic");
    (dir, db)
}

fn log_config(fsync: FsyncPolicy) -> LogConfig {
    LogConfig {
        fsync,
        ..LogConfig::default()
    }
}

/// Size of the topic's segments on disk.
fn disk_bytes(db: &Db) -> u64 {
    let description = db.describe_topic(TOPIC).expect("describe");
    description.partitions.iter().map(|p| p.size_bytes()).sum()
}

fn epoch_nanos() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(since_epoch.as_nanos()).unwrap_or(u64::MAX)
}

/// Sends `rate` mixed messages per second for `duration`, each stamped with
/// the time it was due. Returns the number of messages and payload bytes.
fn produce_at_rate(db: &Db, rate: u64, duration: Duration) -> (u64, u64) {
    let mut producer = db.producer(TOPIC).expect("producer");
    let pool = payload::pool(Workload::Mixed, RATE_SEED);
    let interval_nanos = 1_000_000_000 / rate;
    let total = (rate as f64 * duration.as_secs_f64()) as u64;
    let origin_epoch = epoch_nanos();
    let origin = Instant::now();
    let mut bytes = 0;
    for i in 0..total {
        let due = Duration::from_nanos(interval_nanos * i);
        if let Some(wait) = due.checked_sub(origin.elapsed()) {
            thread::sleep(wait);
        }
        let message = &pool[i as usize % pool.len()];
        let due_epoch = origin_epoch + interval_nanos * i;
        producer.send(&stamped(message, due_epoch)).expect("send");
        bytes += payload::message_bytes(message);
    }
    (total, bytes)
}

/// A copy of `message` stamped with the time it was due to be sent.
fn stamped(message: &Message, due_epoch_nanos: u64) -> Message {
    message
        .clone()
        .header(SENT_AT_HEADER, due_epoch_nanos.to_le_bytes().to_vec())
}

/// Time since the message was due to be sent, if it carries the header.
pub fn delivery_latency(headers: &[Header]) -> Option<Duration> {
    let header = headers.iter().find(|h| h.key == SENT_AT_HEADER)?;
    let sent_at = u64::from_le_bytes(header.value.as_slice().try_into().ok()?);
    Some(Duration::from_nanos(epoch_nanos().saturating_sub(sent_at)))
}
