//! Topic tests: creation, several writers, readers, read limits.
//!
//! Multi-process tests rerun this same test binary in a child process
//! (see `child_process`), which plays a given scenario.

#[allow(dead_code)]
mod common;

use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;
use std::thread;
use std::time::Duration;

use common::{ALL, config, message, new_db, spawn_child, wait_until};
use mojay::{Db, Error, FsyncPolicy, Message, Record, Retention, TopicConfig};
use tempfile::TempDir;

const STREAM_LEN: u64 = 3000;

fn assert_is_message(rec: &Record, i: u64) {
    assert_eq!(rec.offset, i);
    assert_eq!(rec.value, message(i).value, "content of offset {i}");
}

/// A message from writer `writer`, the `seq`-th it sends.
fn tagged(writer: &str, seq: u64) -> Message {
    Message::new(writer, format!("{writer}:{seq}:{}", "x".repeat(48)))
}

/// Reads all of partition 0, checks that offsets are contiguous from the
/// start, and returns each record as (writer, seq).
fn read_tagged(db: &Db, topic: &str) -> Vec<(String, u64)> {
    let mut reader = db.reader(topic).unwrap();
    let limit = reader.read_limit(0).unwrap();
    let mut next = reader.start_offset(0).unwrap();
    let mut out = Vec::new();
    while next < limit {
        for rec in reader.read(0, next, 64 * 1024).unwrap() {
            assert_eq!(rec.offset, next, "offsets are contiguous, with no gap");
            let value = String::from_utf8(rec.value.unwrap()).unwrap();
            let mut fields = value.split(':');
            let writer = fields.next().unwrap().to_string();
            let seq = fields.next().unwrap().parse().unwrap();
            out.push((writer, seq));
            next += 1;
        }
    }
    out
}

/// Checks that each writer's messages appear once each, in send order, from 0.
fn assert_each_writer_in_order(records: &[(String, u64)], writers: &[&str]) {
    for writer in writers {
        let seqs: Vec<u64> = records
            .iter()
            .filter(|(w, _)| w == writer)
            .map(|(_, seq)| *seq)
            .collect();
        let expected: Vec<u64> = (0..seqs.len() as u64).collect();
        assert_eq!(seqs, expected, "messages of {writer}: once each, in order");
    }
}

/// Overwrites one `u64` field of partition 0 in `partitions.state`, as a
/// crashed or out-of-date writer would have left it.
fn poke_state(data: &std::path::Path, topic: &str, field: u64, value: u64) {
    let path = data.join(format!("topics/{topic}/partitions.state"));
    let file = OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&value.to_ne_bytes(), field * 8).unwrap();
}

const READ_LIMIT_FIELD: u64 = 0;
const DIRTY_FIELD: u64 = 4;

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
        "write_stream" => {
            let mut producer = db.producer("stream").unwrap();
            for i in 0..STREAM_LEN {
                producer.send_to(0, &message(i)).unwrap();
            }
            println!("RESULT:done");
        }
        "write_until_killed" => {
            let mut producer = db.producer("stream").unwrap();
            // Write forever: the parent kills this process mid-write.
            for i in 0..u64::MAX {
                producer.send_to(0, &message(i)).unwrap();
            }
        }
        // `write_tagged:<writer>:<count>`: sends `count` tagged messages to partition 0.
        tagged_scenario if tagged_scenario.starts_with("write_tagged:") => {
            let mut fields = tagged_scenario.split(':').skip(1);
            let writer = fields.next().unwrap();
            let count: u64 = fields.next().unwrap().parse().unwrap();
            let mut producer = db.producer("shared").unwrap();
            for seq in 0..count {
                producer.send_to(0, &tagged(writer, seq)).unwrap();
            }
            println!("RESULT:done");
        }
        // `write_tagged_forever:<writer>`: sends until killed.
        tagged_scenario if tagged_scenario.starts_with("write_tagged_forever:") => {
            let writer = tagged_scenario.split(':').nth(1).unwrap();
            let mut producer = db.producer("shared").unwrap();
            for seq in 0..u64::MAX {
                producer.send_to(0, &tagged(writer, seq)).unwrap();
            }
        }
        other => panic!("unknown scenario: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Creation and names
// ---------------------------------------------------------------------------

#[test]
fn topics_are_created_listed_and_unique() {
    let (_dir, db) = new_db();
    db.create_topic("orders", TopicConfig::default()).unwrap();
    db.create_topic("audit.v2", TopicConfig::default()).unwrap();
    assert_eq!(db.list_topics().unwrap(), vec!["audit.v2", "orders"]);
    assert!(matches!(
        db.create_topic("orders", TopicConfig::default()),
        Err(Error::TopicExists(_))
    ));
}

#[test]
fn unknown_topics_and_bad_names_are_rejected() {
    let (_dir, db) = new_db();
    assert!(matches!(
        db.producer("missing"),
        Err(Error::UnknownTopic(_))
    ));
    assert!(matches!(db.reader("missing"), Err(Error::UnknownTopic(_))));
    assert!(matches!(
        db.create_topic("../evil", TopicConfig::default()),
        Err(Error::InvalidName { .. })
    ));
    let zero = TopicConfig {
        partitions: 0,
        ..TopicConfig::default()
    };
    assert!(matches!(
        db.create_topic("empty", zero),
        Err(Error::InvalidConfig(_))
    ));
}

#[test]
fn data_folder_from_an_unknown_format_is_refused() {
    let dir = TempDir::new().unwrap();
    Db::open(dir.path()).unwrap();
    fs::write(dir.path().join("mojay.toml"), "format_version = 99\n").unwrap();
    assert!(matches!(Db::open(dir.path()), Err(Error::Corrupt { .. })));
}

#[test]
fn topic_from_the_single_writer_format_is_refused() {
    let (dir, db) = new_db();
    db.create_topic("old", TopicConfig::default()).unwrap();
    let meta = dir.path().join("topics/old/meta.toml");
    let text = fs::read_to_string(&meta).unwrap();
    fs::write(
        &meta,
        text.replace("format_version = 2", "format_version = 1"),
    )
    .unwrap();

    for result in [db.producer("old").err(), db.reader("old").err()] {
        let error = result.expect("a version 1 topic must be refused");
        assert!(matches!(error, Error::Corrupt { .. }));
        assert!(error.to_string().contains("older Mojay"), "{error}");
    }
}

// ---------------------------------------------------------------------------
// Producing and reading
// ---------------------------------------------------------------------------

#[test]
fn messages_with_the_same_key_stay_together_and_in_order() {
    let (_dir, db) = new_db();
    db.create_topic("orders", config(4, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    let mut placed = Vec::new();
    for i in 0..20 {
        let key = format!("client-{}", i % 3);
        let msg = Message::new(key.clone(), i.to_string());
        placed.push((key, producer.send(&msg).unwrap()));
    }

    let mut reader = db.reader("orders").unwrap();
    for (key, (partition, offset)) in &placed {
        let same_key: Vec<_> = placed.iter().filter(|(k, _)| k == key).collect();
        assert!(same_key.iter().all(|(_, (p, _))| p == partition));
        let rec = &reader.read(*partition, *offset, ALL).unwrap()[0];
        assert_eq!(rec.key.as_deref(), Some(key.as_bytes()));
    }
}

#[test]
fn reader_follows_new_messages_and_segments_without_reopening() {
    let (_dir, db) = new_db();
    db.create_topic("stream", config(1, 2048, FsyncPolicy::Never))
        .unwrap();
    let mut reader = db.reader("stream").unwrap();
    assert_eq!(reader.read_limit(0).unwrap(), 0);
    assert!(reader.read(0, 0, ALL).unwrap().is_empty());

    let mut producer = db.producer("stream").unwrap();
    let mut next = 0;
    for batch in 0..10 {
        for i in batch * 30..(batch + 1) * 30 {
            producer.send_to(0, &message(i)).unwrap();
        }
        // The reader, opened beforehand, sees the new messages and segments.
        while next < reader.read_limit(0).unwrap() {
            for rec in reader.read(0, next, 4096).unwrap() {
                assert_is_message(&rec, next);
                next += 1;
            }
        }
        assert_eq!(next, (batch + 1) * 30);
    }
}

#[test]
fn unknown_partition_is_reported() {
    let (_dir, db) = new_db();
    db.create_topic("orders", config(2, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    assert!(matches!(
        producer.send_to(2, &message(0)),
        Err(Error::UnknownPartition {
            partition: 2,
            partitions: 2
        })
    ));
    let mut reader = db.reader("orders").unwrap();
    assert!(matches!(
        reader.read(5, 0, ALL),
        Err(Error::UnknownPartition { .. })
    ));
}

// ---------------------------------------------------------------------------
// Read limit
// ---------------------------------------------------------------------------

#[test]
fn reader_never_reads_past_the_read_limit() {
    let (dir, db) = new_db();
    db.create_topic("orders", config(1, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    for i in 0..10 {
        producer.send_to(0, &message(i)).unwrap();
    }
    drop(producer);

    // Simulate a writer that has published only the first 4 messages.
    poke_state(dir.path(), "orders", READ_LIMIT_FIELD, 4);

    let mut reader = db.reader("orders").unwrap();
    assert_eq!(reader.read_limit(0).unwrap(), 4);
    let records = reader.read(0, 0, ALL).unwrap();
    assert_eq!(records.len(), 4);
    assert!(matches!(
        reader.read(0, 5, ALL),
        Err(Error::OffsetOutOfRange { end: 4, .. })
    ));
}

#[test]
fn read_limit_follows_the_log_after_recovery() {
    let (dir, db) = new_db();
    db.create_topic("orders", config(1, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    for i in 0..5 {
        producer.send_to(0, &message(i)).unwrap();
    }
    drop(producer);

    // Interrupted write: extra trailing bytes in the active segment.
    let segment = dir.path().join("topics/orders/p0/00000000000000000000.log");
    let mut bytes = fs::read(&segment).unwrap();
    bytes.extend_from_slice(&[0xAB; 20]);
    fs::write(&segment, bytes).unwrap();

    let _producer = db.producer("orders").unwrap();
    let mut reader = db.reader("orders").unwrap();
    assert_eq!(reader.read_limit(0).unwrap(), 5);
    assert_eq!(reader.read(0, 0, ALL).unwrap().len(), 5);
}

// ---------------------------------------------------------------------------
// Several writers
// ---------------------------------------------------------------------------

#[test]
fn two_producers_in_the_same_process_take_turns_on_a_partition() {
    let (_dir, db) = new_db();
    // Small segments: the producers keep finding segments the other one started.
    db.create_topic("shared", config(1, 600, FsyncPolicy::Always))
        .unwrap();
    let mut a = db.producer("shared").unwrap();
    let mut b = db.producer("shared").unwrap();
    let mut offsets = Vec::new();
    for seq in 0..60 {
        offsets.push(a.send_to(0, &tagged("a", seq)).unwrap());
        offsets.push(b.send_to(0, &tagged("b", seq)).unwrap());
    }
    assert_eq!(
        offsets,
        (0..120).collect::<Vec<_>>(),
        "no offset given twice"
    );

    let records = read_tagged(&db, "shared");
    assert_eq!(records.len(), 120);
    assert_each_writer_in_order(&records, &["a", "b"]);
    assert!(
        db.describe_topic("shared").unwrap().partitions[0]
            .segments
            .len()
            > 5,
        "the test must cover segments started by the other producer"
    );
}

/// Several processes write to the same partition at the same time: every
/// message gets its own offset, and each process's messages keep their order.
#[test]
fn producers_in_several_processes_share_a_partition() {
    let (dir, db) = new_db();
    db.create_topic("shared", config(1, 4096, FsyncPolicy::Never))
        .unwrap();
    let writers = ["w0", "w1", "w2", "w3"];
    let children: Vec<_> = writers
        .iter()
        .map(|w| spawn_child(&format!("write_tagged:{w}:1000"), dir.path()))
        .collect();
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let records = read_tagged(&db, "shared");
    assert_eq!(records.len(), 4000);
    assert_each_writer_in_order(&records, &writers);
}

/// A writer killed in the middle of the stream never blocks or corrupts the
/// others: they keep writing, the read limit only grows, and the log stays
/// contiguous and readable.
#[test]
fn killed_writer_does_not_stop_the_others() {
    let (dir, db) = new_db();
    db.create_topic("shared", config(1, 4096, FsyncPolicy::Always))
        .unwrap();
    let mut child = spawn_child("write_tagged_forever:child", dir.path());
    let reader = db.reader("shared").unwrap();
    wait_until(Duration::from_secs(30), || {
        reader.read_limit(0).unwrap() >= 100
    });

    let mut producer = db.producer("shared").unwrap();
    let mut last_limit = 0;
    let mut send_and_check = |seq: u64| {
        producer.send_to(0, &tagged("parent", seq)).unwrap();
        let limit = reader.read_limit(0).unwrap();
        assert!(limit > last_limit, "the read limit only grows");
        last_limit = limit;
    };
    // Writing next to the child, then after it was killed mid-stream.
    (0..150).for_each(&mut send_and_check);
    child.kill().unwrap();
    child.wait().unwrap();
    (150..400).for_each(&mut send_and_check);

    let records = read_tagged(&db, "shared");
    assert_each_writer_in_order(&records, &["parent", "child"]);
    assert_eq!(records.iter().filter(|(w, _)| w == "parent").count(), 400);
}

/// A writer that died after marking the partition dirty, with a half-written
/// record on disk: the next writer repairs the partition before appending.
#[test]
fn next_writer_repairs_a_partition_left_dirty() {
    let (dir, db) = new_db();
    db.create_topic("shared", config(1, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("shared").unwrap();
    for seq in 0..5 {
        producer.send_to(0, &tagged("a", seq)).unwrap();
    }

    // What a writer killed mid-append leaves behind.
    poke_state(dir.path(), "shared", DIRTY_FIELD, 1);
    let segment = dir.path().join("topics/shared/p0/00000000000000000000.log");
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    std::io::Write::write_all(&mut file, &[0xAB; 37]).unwrap();

    assert_eq!(producer.send_to(0, &tagged("a", 5)).unwrap(), 5);
    let records = read_tagged(&db, "shared");
    assert_eq!(records.len(), 6);
    assert_each_writer_in_order(&records, &["a"]);
}

/// Retention applied by one writer deletes segments another writer still
/// had in its view: the other one simply forgets them.
#[test]
fn retention_by_one_writer_is_seen_by_the_others() {
    let (_dir, db) = new_db();
    let config = TopicConfig {
        partitions: 1,
        log: mojay::LogConfig {
            segment_max_bytes: 600,
            index_interval_bytes: 64,
            fsync: FsyncPolicy::Never,
            retention: Retention {
                max_bytes: Some(3000),
                max_age: None,
            },
        },
    };
    db.create_topic("shared", config).unwrap();
    let mut a = db.producer("shared").unwrap();
    let mut b = db.producer("shared").unwrap();
    for seq in 0..100 {
        a.send_to(0, &tagged("a", seq)).unwrap();
        b.send_to(0, &tagged("b", seq)).unwrap();
        a.enforce_retention().unwrap();
        b.enforce_retention().unwrap();
    }
    let start = db.reader("shared").unwrap().start_offset(0).unwrap();
    assert!(start > 0, "retention must have deleted segments");
    let read = read_tagged(&db, "shared");
    assert_eq!(read.len() as u64, 200 - start);
}

/// A reader follows a writer in another process, segment by segment, and
/// never sees an incomplete message.
#[test]
fn reader_follows_a_writer_in_another_process() {
    let (dir, db) = new_db();
    db.create_topic("stream", config(1, 4096, FsyncPolicy::Never))
        .unwrap();
    let mut reader = db.reader("stream").unwrap();
    let child = spawn_child("write_stream", dir.path());

    let mut next = 0;
    wait_until(Duration::from_secs(60), || {
        for rec in reader.read(0, next, 8 * 1024).unwrap() {
            assert_is_message(&rec, next);
            next += 1;
        }
        next == STREAM_LEN
    });
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
}

// ---------------------------------------------------------------------------
// Periodic fsync
// ---------------------------------------------------------------------------

#[test]
fn interval_policy_runs_in_background_and_stops_cleanly() {
    let (_dir, db) = new_db();
    let interval = FsyncPolicy::Interval(Duration::from_millis(5));
    db.create_topic("orders", config(2, 1 << 20, interval))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    for i in 0..50 {
        producer.send_to((i % 2) as u32, &message(i)).unwrap();
        if i % 10 == 0 {
            thread::sleep(Duration::from_millis(10));
        }
    }
    producer.flush().unwrap();
    drop(producer);

    let mut reader = db.reader("orders").unwrap();
    assert_eq!(reader.read(0, 0, ALL).unwrap().len(), 25);
    assert_eq!(reader.read(1, 0, ALL).unwrap().len(), 25);
}
