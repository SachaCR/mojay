//! The benchmark scenarios. Each one runs in its own temporary data folder,
//! deleted when the scenario ends, and keeps memory bounded: latencies go into
//! a fixed-size histogram, reads are done in 1 MiB batches.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use mojay::{ConsumerConfig, Db, FsyncPolicy, LogConfig, Message, TopicConfig};
use tempfile::TempDir;

use crate::histogram::Histogram;

/// Messages written before the read scenarios: 300,000 × 100 B ≈ 30 MiB on disk.
const PREFILL_MESSAGES: u64 = 300_000;
const READ_BATCH_BYTES: usize = 1024 * 1024;

/// The result of one scenario.
pub struct Measurement {
    pub name: String,
    pub messages: u64,
    /// Payload bytes (message values), not counting the on-disk overhead.
    pub bytes: u64,
    pub elapsed: Duration,
    /// What the latency measures ("per send", "per batch", "delivery"), and its histogram.
    pub latency: Option<(&'static str, Histogram)>,
}

pub struct Scenario {
    pub name: String,
    pub run: Box<dyn Fn(Duration) -> Measurement>,
}

/// Every scenario, in run order.
pub fn all() -> Vec<Scenario> {
    let mut scenarios = Vec::new();
    for (policy_name, policy) in policies() {
        for (size, batch) in [(100, 1), (1024, 1), (100, 100)] {
            let name = format!("produce {policy_name}, {size} B, batch {batch}");
            scenarios.push(Scenario {
                name: name.clone(),
                run: Box::new(move |duration| produce(&name, policy, size, batch, duration)),
            });
        }
    }
    for (policy_name, policy) in [
        ("always", FsyncPolicy::Always),
        ("never", FsyncPolicy::Never),
    ] {
        for writers in [1, 2, 4] {
            let name = format!("{writers} writer(s) on 1 partition, {policy_name}, 100 B");
            scenarios.push(Scenario {
                name: name.clone(),
                run: Box::new(move |duration| concurrent_writers(&name, policy, writers, duration)),
            });
        }
    }
    scenarios.push(Scenario {
        name: "read raw (TopicReader), 100 B".into(),
        run: Box::new(|_| read_raw("read raw (TopicReader), 100 B")),
    });
    scenarios.push(Scenario {
        name: "read with a consumer group, 100 B".into(),
        run: Box::new(|_| read_consumer("read with a consumer group, 100 B")),
    });
    for (policy_name, policy) in [
        ("always", FsyncPolicy::Always),
        ("never", FsyncPolicy::Never),
    ] {
        let name = format!("producer -> consumer latency, {policy_name}, 1,000 msg/s");
        scenarios.push(Scenario {
            name: name.clone(),
            run: Box::new(move |duration| end_to_end(&name, policy, duration)),
        });
    }
    scenarios
}

fn policies() -> [(&'static str, FsyncPolicy); 3] {
    [
        ("always", FsyncPolicy::Always),
        (
            "interval 100ms",
            FsyncPolicy::Interval(Duration::from_millis(100)),
        ),
        ("never", FsyncPolicy::Never),
    ]
}

fn new_topic(policy: FsyncPolicy) -> (TempDir, Db) {
    let dir = TempDir::new().expect("temporary folder");
    let db = Db::open(dir.path()).expect("data folder");
    let config = TopicConfig {
        partitions: 1,
        log: LogConfig {
            fsync: policy,
            ..LogConfig::default()
        },
    };
    db.create_topic("bench", config).expect("topic");
    (dir, db)
}

/// One producer sends `batch` messages of `size` bytes at a time, for `duration`.
fn produce(
    name: &str,
    policy: FsyncPolicy,
    size: usize,
    batch: usize,
    duration: Duration,
) -> Measurement {
    let (_dir, db) = new_topic(policy);
    let mut producer = db.producer("bench").expect("producer");
    let messages = vec![Message::with_value(vec![b'x'; size]); batch];
    let mut latency = Histogram::default();
    let mut sent = 0u64;
    let started = Instant::now();
    while started.elapsed() < duration {
        let before = Instant::now();
        producer.send_batch_to(0, &messages).expect("send");
        latency.record(before.elapsed());
        sent += batch as u64;
    }
    let elapsed = started.elapsed();
    Measurement {
        name: name.into(),
        messages: sent,
        bytes: sent * size as u64,
        elapsed,
        latency: Some((if batch == 1 { "per send" } else { "per batch" }, latency)),
    }
}

/// `writers` producers (one thread each) send to the same partition at the same time.
fn concurrent_writers(
    name: &str,
    policy: FsyncPolicy,
    writers: usize,
    duration: Duration,
) -> Measurement {
    let (_dir, db) = new_topic(policy);
    let start = Arc::new(Barrier::new(writers + 1));
    let handles: Vec<_> = (0..writers)
        .map(|_| {
            let db = db.clone();
            let start = Arc::clone(&start);
            thread::spawn(move || {
                let mut producer = db.producer("bench").expect("producer");
                let message = Message::with_value(vec![b'x'; 100]);
                let mut latency = Histogram::default();
                let mut sent = 0u64;
                start.wait();
                let started = Instant::now();
                while started.elapsed() < duration {
                    let before = Instant::now();
                    producer.send_to(0, &message).expect("send");
                    latency.record(before.elapsed());
                    sent += 1;
                }
                (sent, latency)
            })
        })
        .collect();
    start.wait();
    let started = Instant::now();
    let mut total = 0;
    let mut latency = Histogram::default();
    for handle in handles {
        let (sent, histogram) = handle.join().expect("writer thread");
        total += sent;
        latency.merge(&histogram);
    }
    Measurement {
        name: name.into(),
        messages: total,
        bytes: total * 100,
        elapsed: started.elapsed(),
        latency: Some(("per send", latency)),
    }
}

/// Writes `PREFILL_MESSAGES` messages of 100 B, in batches, without fsync.
fn prefilled_topic() -> (TempDir, Db) {
    let (dir, db) = new_topic(FsyncPolicy::Never);
    let mut producer = db.producer("bench").expect("producer");
    let batch = vec![Message::with_value(vec![b'x'; 100]); 1000];
    for _ in 0..PREFILL_MESSAGES / 1000 {
        producer.send_batch_to(0, &batch).expect("prefill");
    }
    (dir, db)
}

/// Reads the whole partition with a `TopicReader`, 1 MiB at a time.
fn read_raw(name: &str) -> Measurement {
    let (_dir, db) = prefilled_topic();
    let mut reader = db.reader("bench").expect("reader");
    let started = Instant::now();
    let mut next = 0;
    let mut bytes = 0;
    while next < PREFILL_MESSAGES {
        let records = reader.read(0, next, READ_BATCH_BYTES).expect("read");
        for record in &records {
            bytes += record.value.as_ref().map_or(0, Vec::len) as u64;
        }
        next += records.len() as u64;
    }
    Measurement {
        name: name.into(),
        messages: next,
        bytes,
        elapsed: started.elapsed(),
        latency: None,
    }
}

/// Reads the whole partition with a consumer, then commits.
fn read_consumer(name: &str) -> Measurement {
    let (_dir, db) = prefilled_topic();
    let mut consumer = db
        .consumer("bench", &["bench"], ConsumerConfig::default())
        .expect("consumer");
    let started = Instant::now();
    let mut read = 0;
    let mut bytes = 0;
    while read < PREFILL_MESSAGES {
        for record in consumer.poll(Duration::from_millis(100)).expect("poll") {
            bytes += record.value.as_ref().map_or(0, Vec::len) as u64;
            read += 1;
        }
    }
    consumer.commit().expect("commit");
    Measurement {
        name: name.into(),
        messages: read,
        bytes,
        elapsed: started.elapsed(),
        latency: None,
    }
}

/// A producer sends 1,000 messages per second, each carrying its send time; a
/// consumer measures how long each takes to reach it.
fn end_to_end(name: &str, policy: FsyncPolicy, duration: Duration) -> Measurement {
    let (_dir, db) = new_topic(policy);
    let mut consumer = db
        .consumer("bench", &["bench"], ConsumerConfig::default())
        .expect("consumer");
    let origin = Instant::now();
    let total = (duration.as_millis() as u64).max(1);

    let producer_db = db.clone();
    let producer = thread::spawn(move || {
        let mut producer = producer_db.producer("bench").expect("producer");
        for i in 0..total {
            // Paced at one message per millisecond.
            let due = Duration::from_millis(i);
            if let Some(wait) = due.checked_sub(origin.elapsed()) {
                thread::sleep(wait);
            }
            let sent_at = origin.elapsed().as_nanos() as u64;
            producer
                .send_to(0, &Message::with_value(sent_at.to_le_bytes().to_vec()))
                .expect("send");
        }
    });

    let mut latency = Histogram::default();
    let mut received = 0;
    let started = Instant::now();
    while received < total {
        for record in consumer.poll(Duration::from_millis(100)).expect("poll") {
            let value = record.value.expect("value");
            let sent_at = u64::from_le_bytes(value[..8].try_into().expect("8 bytes"));
            let now = origin.elapsed().as_nanos() as u64;
            latency.record(Duration::from_nanos(now.saturating_sub(sent_at)));
            received += 1;
        }
    }
    producer.join().expect("producer thread");
    Measurement {
        name: name.into(),
        messages: received,
        bytes: received * 8,
        elapsed: started.elapsed(),
        latency: Some(("delivery", latency)),
    }
}
