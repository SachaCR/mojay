//! Consumer tests: reading, committing, resuming, handover between members.

#[allow(dead_code)]
mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use common::{config, message, new_db, spawn_child, wait_until};
use mojay::{
    Consumer, ConsumerConfig, ConsumerRecord, Db, Error, FsyncPolicy, StartFrom, TopicConfig,
};

const POLL: Duration = Duration::from_millis(100);

fn manual() -> ConsumerConfig {
    ConsumerConfig::default()
}

/// Call `poll` until `count` messages are received, or fail after 10 s.
fn poll_exactly(consumer: &mut Consumer, count: usize) -> Vec<ConsumerRecord> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut out = Vec::new();
    while out.len() < count {
        assert!(
            Instant::now() < deadline,
            "only {} messages out of {count}",
            out.len()
        );
        out.extend(consumer.poll(POLL).unwrap());
    }
    assert_eq!(out.len(), count, "too many messages");
    out
}

fn assert_nothing_more(consumer: &mut Consumer) {
    assert!(consumer.poll(Duration::from_millis(50)).unwrap().is_empty());
}

/// Write messages `range` to partition 0 of `topic`.
fn produce(db: &Db, topic: &str, range: std::ops::Range<u64>) {
    let mut producer = db.producer(topic).unwrap();
    for i in range {
        producer.send_to(0, &message(i)).unwrap();
    }
}

fn offsets(records: &[ConsumerRecord]) -> Vec<u64> {
    records.iter().map(|r| r.offset).collect()
}

/// The committed offset, read directly from disk.
fn committed_on_disk(data: &Path, group: &str, topic: &str, partition: u32) -> Option<u64> {
    let path = data.join(format!("groups/{group}/offsets/{topic}-{partition}"));
    let bytes = fs::read(path).ok()?;
    Some(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
}

// ---------------------------------------------------------------------------
// Child process
// ---------------------------------------------------------------------------

/// Entry point for child processes. Does nothing in a normal test run.
#[test]
fn child_process() {
    let Some((scenario, db)) = common::child_scenario() else {
        return;
    };
    match scenario.as_str() {
        // Read at least 40 messages, commit, then wait to be killed.
        "consume_commit_then_hang" => {
            // Small polls: the commit lands mid-stream, not at the end.
            let small = ConsumerConfig {
                max_poll_bytes: 500,
                ..ConsumerConfig::default()
            };
            let mut consumer = db.consumer("g", &["stream"], small).unwrap();
            let mut read = 0;
            while read < 40 {
                read += consumer.poll(POLL).unwrap().len();
            }
            consumer.commit().unwrap();
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        }
        other => panic!("unknown scenario: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Reading and committing
// ---------------------------------------------------------------------------

#[test]
fn consumer_reads_every_partition_in_order() {
    let (_dir, db) = new_db();
    db.create_topic("orders", config(3, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    let mut sent: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for i in 0..30 {
        let (partition, offset) = producer.send(&message(i)).unwrap();
        sent.entry(partition).or_default().push(offset);
    }

    let mut consumer = db.consumer("billing", &["orders"], manual()).unwrap();
    assert_eq!(consumer.assignment().len(), 3);
    let mut received: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for record in poll_exactly(&mut consumer, 30) {
        assert_eq!(record.topic, "orders");
        received
            .entry(record.partition)
            .or_default()
            .push(record.offset);
    }
    assert_eq!(
        received, sent,
        "each partition must be read in full, in order"
    );
    assert_nothing_more(&mut consumer);
}

#[test]
fn committed_consumer_resumes_where_it_stopped() {
    let (_dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    produce(&db, "stream", 0..20);

    let mut consumer = db.consumer("billing", &["stream"], manual()).unwrap();
    assert_eq!(
        offsets(&poll_exactly(&mut consumer, 20)),
        (0..20).collect::<Vec<_>>()
    );
    consumer.commit().unwrap();
    drop(consumer);

    produce(&db, "stream", 20..25);
    let mut consumer = db.consumer("billing", &["stream"], manual()).unwrap();
    assert_eq!(consumer.position("stream", 0), Some(20));
    assert_eq!(
        offsets(&poll_exactly(&mut consumer, 5)),
        (20..25).collect::<Vec<_>>()
    );
}

#[test]
fn uncommitted_messages_are_read_again() {
    let (_dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    produce(&db, "stream", 0..10);

    let mut consumer = db.consumer("billing", &["stream"], manual()).unwrap();
    poll_exactly(&mut consumer, 10);
    drop(consumer); // without committing

    let mut consumer = db.consumer("billing", &["stream"], manual()).unwrap();
    assert_eq!(
        offsets(&poll_exactly(&mut consumer, 10)),
        (0..10).collect::<Vec<_>>()
    );
}

#[test]
fn auto_commit_commits_on_poll_and_on_close() {
    let (dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    produce(&db, "stream", 0..10);
    let auto = ConsumerConfig {
        auto_commit: Some(Duration::ZERO),
        ..ConsumerConfig::default()
    };

    let mut consumer = db.consumer("billing", &["stream"], auto.clone()).unwrap();
    poll_exactly(&mut consumer, 10);
    assert_nothing_more(&mut consumer); // this poll commits the 10 returned messages
    assert_eq!(
        committed_on_disk(dir.path(), "billing", "stream", 0),
        Some(10)
    );

    produce(&db, "stream", 10..13);
    poll_exactly(&mut consumer, 3);
    drop(consumer); // closing commits the last 3

    let mut consumer = db.consumer("billing", &["stream"], auto).unwrap();
    assert_eq!(consumer.position("stream", 0), Some(13));
    assert_nothing_more(&mut consumer);
}

#[test]
fn start_from_latest_skips_existing_messages() {
    let (_dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    produce(&db, "stream", 0..10);

    let latest = ConsumerConfig {
        start_from: StartFrom::Latest,
        ..ConsumerConfig::default()
    };
    let mut consumer = db.consumer("alerts", &["stream"], latest).unwrap();
    assert_nothing_more(&mut consumer);
    produce(&db, "stream", 10..12);
    assert_eq!(offsets(&poll_exactly(&mut consumer, 2)), vec![10, 11]);
}

#[test]
fn groups_are_independent() {
    let (_dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    produce(&db, "stream", 0..10);

    let mut billing = db.consumer("billing", &["stream"], manual()).unwrap();
    let mut audit = db.consumer("audit", &["stream"], manual()).unwrap();
    poll_exactly(&mut billing, 10);
    billing.commit().unwrap();
    assert_eq!(
        offsets(&poll_exactly(&mut audit, 10)),
        (0..10).collect::<Vec<_>>()
    );
}

#[test]
fn consumer_reads_several_topics() {
    let (_dir, db) = new_db();
    db.create_topic("orders", TopicConfig::default()).unwrap();
    db.create_topic("refunds", TopicConfig::default()).unwrap();
    produce(&db, "orders", 0..4);
    produce(&db, "refunds", 0..3);

    let mut consumer = db
        .consumer("billing", &["orders", "refunds"], manual())
        .unwrap();
    let records = poll_exactly(&mut consumer, 7);
    let count = |topic: &str| records.iter().filter(|r| r.topic == topic).count();
    assert_eq!((count("orders"), count("refunds")), (4, 3));
}

#[test]
fn poll_returns_as_soon_as_a_message_arrives() {
    let (_dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    let mut consumer = db.consumer("billing", &["stream"], manual()).unwrap();

    let started = Instant::now();
    assert!(consumer.poll(Duration::from_millis(80)).unwrap().is_empty());
    assert!(started.elapsed() >= Duration::from_millis(80));

    let writer = {
        let db = db.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            produce(&db, "stream", 0..1);
        })
    };
    let started = Instant::now();
    let records = consumer.poll(Duration::from_secs(10)).unwrap();
    assert_eq!(offsets(&records), vec![0]);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "poll too slow to react"
    );
    writer.join().unwrap();
}

#[test]
fn max_poll_bytes_splits_large_backlogs_without_losing_any() {
    let (_dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    produce(&db, "stream", 0..200);
    let small = ConsumerConfig {
        max_poll_bytes: 1000,
        ..ConsumerConfig::default()
    };
    let mut consumer = db.consumer("billing", &["stream"], small).unwrap();
    let first = consumer.poll(POLL).unwrap();
    assert!(!first.is_empty() && first.len() < 200);
    let mut all = first;
    all.extend(poll_exactly(&mut consumer, 200 - all.len()));
    assert_eq!(offsets(&all), (0..200).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Handover between members
// ---------------------------------------------------------------------------

/// With a single partition, the second member gets nothing while the first
/// is alive, then takes it over exactly at the commit when the first leaves.
#[test]
fn departing_member_hands_over_from_its_commit() {
    let (_dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    produce(&db, "stream", 0..10);

    let mut first = db.consumer("billing", &["stream"], manual()).unwrap();
    let mut second = db.consumer("billing", &["stream"], manual()).unwrap();
    assert_eq!(first.assignment().len(), 1);
    assert!(
        second.assignment().is_empty(),
        "the partition is already taken"
    );
    assert_nothing_more(&mut second);

    poll_exactly(&mut first, 10);
    first.commit().unwrap();
    drop(first);

    produce(&db, "stream", 10..12);
    let records = poll_exactly(&mut second, 2);
    assert_eq!(second.assignment().len(), 1);
    assert_eq!(
        offsets(&records),
        vec![10, 11],
        "resumes exactly at the commit, without duplicates"
    );
}

/// A member killed in another process releases its partitions; the next one
/// resumes from its last commit.
#[test]
fn killed_member_hands_over_from_its_last_commit() {
    let (dir, db) = new_db();
    db.create_topic("stream", TopicConfig::default()).unwrap();
    produce(&db, "stream", 0..100);

    let mut child = spawn_child("consume_commit_then_hang", dir.path());
    wait_until(Duration::from_secs(30), || {
        committed_on_disk(dir.path(), "g", "stream", 0).is_some()
    });
    let committed = committed_on_disk(dir.path(), "g", "stream", 0).unwrap();
    assert!(
        (40..100).contains(&committed),
        "commit at {committed}: there must be messages left to resume"
    );

    // The child holds the partition: this member receives nothing.
    let mut consumer = db.consumer("g", &["stream"], manual()).unwrap();
    assert!(consumer.assignment().is_empty());
    assert_nothing_more(&mut consumer);

    child.kill().unwrap();
    child.wait().unwrap();

    let records = poll_exactly(&mut consumer, (100 - committed) as usize);
    assert_eq!(offsets(&records), (committed..100).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[test]
fn invalid_subscriptions_are_rejected() {
    let (_dir, db) = new_db();
    db.create_topic("orders", TopicConfig::default()).unwrap();
    assert!(matches!(
        db.consumer("billing", &[], manual()),
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        db.consumer("billing", &["orders", "orders"], manual()),
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        db.consumer("billing", &["missing"], manual()),
        Err(Error::UnknownTopic(_))
    ));
    assert!(matches!(
        db.consumer("../evil", &["orders"], manual()),
        Err(Error::InvalidName { .. })
    ));
}

#[test]
fn damaged_committed_offset_is_reported() {
    let (dir, db) = new_db();
    db.create_topic("orders", TopicConfig::default()).unwrap();
    drop(db.consumer("billing", &["orders"], manual()).unwrap());
    fs::write(dir.path().join("groups/billing/offsets/orders-0"), b"oops").unwrap();
    assert!(matches!(
        db.consumer("billing", &["orders"], manual()),
        Err(Error::Corrupt { .. })
    ));
}
