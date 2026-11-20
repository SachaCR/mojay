//! Tests for multi-member consumer groups: fair assignment, joins and
//! leaves, lossless handover, across threads and across processes.

#[allow(dead_code)]
mod common;

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{config, message, new_db, spawn_child, wait_until};
use mojay::{Consumer, ConsumerConfig, ConsumerRecord, Db, FsyncPolicy, RebalanceEvent};

const SHORT: Duration = Duration::from_millis(20);
const MESSAGES_PER_PARTITION: u64 = 100;
const PROCESS_TEST_MESSAGES: u64 = 2000;

/// (partition, offset): the identity of a message within a topic.
type Seen = BTreeSet<(u32, u64)>;

fn auto_commit() -> ConsumerConfig {
    ConsumerConfig {
        auto_commit: Some(Duration::ZERO),
        max_poll_bytes: 2000,
        ..ConsumerConfig::default()
    }
}

/// Create "stream" with `partitions` partitions of `per_partition` messages each.
fn topic_with_messages(db: &Db, partitions: u32, per_partition: u64) {
    db.create_topic("stream", config(partitions, 1 << 20, FsyncPolicy::Never))
        .unwrap();
    let mut producer = db.producer("stream").unwrap();
    for partition in 0..partitions {
        for i in 0..per_partition {
            producer.send_to(partition, &message(i)).unwrap();
        }
    }
}

fn every_message(partitions: u32, per_partition: u64) -> Seen {
    (0..partitions)
        .flat_map(|p| (0..per_partition).map(move |o| (p, o)))
        .collect()
}

/// Add the messages read to `seen`, failing on any duplicate.
fn record_without_duplicates(seen: &mut Seen, records: Vec<ConsumerRecord>) {
    for r in records {
        assert!(
            seen.insert((r.partition, r.offset)),
            "duplicate: partition {} offset {}",
            r.partition,
            r.offset
        );
    }
}

/// Run `poll` on every member until the assignment is `expected`,
/// keeping whatever they read along the way.
fn poll_until_assignment(consumers: &mut [&mut Consumer], expected: &[usize], seen: &mut Seen) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for consumer in consumers.iter_mut() {
            record_without_duplicates(seen, consumer.poll(SHORT).unwrap());
        }
        let current: Vec<_> = consumers.iter().map(|c| c.assignment().len()).collect();
        if current == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "assignment {current:?}, expected {expected:?}"
        );
    }
}

fn assert_disjoint(consumers: &[&Consumer]) {
    let mut all = BTreeSet::new();
    for consumer in consumers {
        for owned in consumer.assignment() {
            assert!(all.insert(owned.clone()), "{owned:?} owned twice");
        }
    }
}

fn members_dir(data: &Path) -> std::path::PathBuf {
    data.join("groups/g/members")
}

/// Number of member files; 0 while the group does not exist.
fn member_files(data: &Path) -> usize {
    fs::read_dir(members_dir(data)).map_or(0, |d| d.count())
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
        // Wait for a second member, read while printing each message, and
        // stop after one second without receiving anything.
        "member_until_idle" => {
            let mut consumer = db.consumer("g", &["stream"], auto_commit()).unwrap();
            let dir = Path::new(&std::env::var("MOJAY_TEST_CHILD_DIR").unwrap()).to_path_buf();
            wait_until(Duration::from_secs(30), || member_files(&dir) >= 2);
            let mut idle_since = Instant::now();
            let mut received_any = false;
            while !received_any || idle_since.elapsed() < Duration::from_secs(1) {
                let records = consumer.poll(Duration::from_millis(100)).unwrap();
                if !records.is_empty() {
                    received_any = true;
                    idle_since = Instant::now();
                }
                for r in records {
                    println!("REC {} {}", r.partition, r.offset);
                }
            }
        }
        // Stay a member and read forever, until killed.
        "member_until_killed" => {
            let mut consumer = db.consumer("g", &["stream"], auto_commit()).unwrap();
            loop {
                consumer.poll(Duration::from_millis(100)).unwrap();
            }
        }
        other => panic!("unknown scenario: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Assignment
// ---------------------------------------------------------------------------

#[test]
fn two_members_share_the_partitions_evenly() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 4, 0);
    let mut first = db.consumer("g", &["stream"], auto_commit()).unwrap();
    assert_eq!(first.assignment().len(), 4, "alone, it takes everything");

    let mut second = db.consumer("g", &["stream"], auto_commit()).unwrap();
    poll_until_assignment(&mut [&mut first, &mut second], &[2, 2], &mut Seen::new());
    assert_disjoint(&[&first, &second]);
}

#[test]
fn oldest_members_take_the_extra_partitions() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 4, 0);
    let mut a = db.consumer("g", &["stream"], auto_commit()).unwrap();
    let mut b = db.consumer("g", &["stream"], auto_commit()).unwrap();
    let mut c = db.consumer("g", &["stream"], auto_commit()).unwrap();
    poll_until_assignment(&mut [&mut a, &mut b, &mut c], &[2, 1, 1], &mut Seen::new());
    assert_disjoint(&[&a, &b, &c]);
}

#[test]
fn departed_member_gives_its_partitions_back() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 4, 0);
    let mut first = db.consumer("g", &["stream"], auto_commit()).unwrap();
    let mut second = db.consumer("g", &["stream"], auto_commit()).unwrap();
    poll_until_assignment(&mut [&mut first, &mut second], &[2, 2], &mut Seen::new());

    drop(second);
    poll_until_assignment(&mut [&mut first], &[4], &mut Seen::new());
}

#[test]
fn more_members_than_partitions_leaves_some_idle() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 2, 0);
    let mut members: Vec<Consumer> = (0..3)
        .map(|_| db.consumer("g", &["stream"], auto_commit()).unwrap())
        .collect();
    let mut refs: Vec<&mut Consumer> = members.iter_mut().collect();
    poll_until_assignment(&mut refs, &[1, 1, 0], &mut Seen::new());
}

#[test]
fn rebalance_events_report_assigned_and_revoked_partitions() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 2, 0);
    let mut first = db.consumer("g", &["stream"], auto_commit()).unwrap();
    let assigned = |p| RebalanceEvent::Assigned {
        topic: "stream".into(),
        partition: p,
    };
    assert_eq!(
        first.take_rebalance_events(),
        vec![assigned(0), assigned(1)]
    );
    assert!(
        first.take_rebalance_events().is_empty(),
        "returned only once"
    );

    let mut second = db.consumer("g", &["stream"], auto_commit()).unwrap();
    poll_until_assignment(&mut [&mut first, &mut second], &[1, 1], &mut Seen::new());
    assert_eq!(
        first.take_rebalance_events(),
        vec![RebalanceEvent::Revoked {
            topic: "stream".into(),
            partition: 1
        }]
    );
    assert_eq!(second.take_rebalance_events(), vec![assigned(1)]);
}

// ---------------------------------------------------------------------------
// Lossless handover
// ---------------------------------------------------------------------------

/// With auto-commit, every message is read exactly once,
/// even as members join and leave mid-read.
#[test]
fn every_message_is_read_once_while_members_come_and_go() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 4, MESSAGES_PER_PARTITION);
    let mut seen = Seen::new();

    let mut a = db.consumer("g", &["stream"], auto_commit()).unwrap();
    record_without_duplicates(&mut seen, a.poll(SHORT).unwrap());

    let mut b = db.consumer("g", &["stream"], auto_commit()).unwrap();
    poll_until_assignment(&mut [&mut a, &mut b], &[2, 2], &mut seen);

    let mut c = db.consumer("g", &["stream"], auto_commit()).unwrap();
    poll_until_assignment(&mut [&mut a, &mut b, &mut c], &[2, 1, 1], &mut seen);

    drop(a);
    poll_until_assignment(&mut [&mut b, &mut c], &[2, 2], &mut seen);

    let expected = every_message(4, MESSAGES_PER_PARTITION);
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < expected.len() {
        assert!(Instant::now() < deadline, "{} messages read", seen.len());
        record_without_duplicates(&mut seen, b.poll(SHORT).unwrap());
        record_without_duplicates(&mut seen, c.poll(SHORT).unwrap());
    }
    assert_eq!(seen, expected);
}

/// With manual commit, a partition that is given up is not committed on the
/// application's behalf: its new owner rereads it from the last commit.
#[test]
fn manual_commit_member_replays_revoked_partitions_from_last_commit() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 2, 10);
    let manual = ConsumerConfig::default();

    let mut first = db.consumer("g", &["stream"], manual.clone()).unwrap();
    let mut read = Seen::new();
    while read.len() < 20 {
        record_without_duplicates(&mut read, first.poll(SHORT).unwrap());
    }
    // No commit: a second member joins and receives a partition.
    let mut second = db.consumer("g", &["stream"], manual).unwrap();
    let mut replayed = Seen::new();
    poll_until_assignment(&mut [&mut first, &mut second], &[1, 1], &mut replayed);
    while replayed.len() < 10 {
        record_without_duplicates(&mut replayed, second.poll(SHORT).unwrap());
    }
    let (_, partition) = second.assignment()[0].clone();
    let expected: Seen = (0..10).map(|o| (partition, o)).collect();
    assert_eq!(replayed, expected, "fully re-read, nothing is lost");
}

// ---------------------------------------------------------------------------
// Across processes
// ---------------------------------------------------------------------------

/// Two members in two processes share the topic: each reads a share,
/// and together they read every message exactly once.
#[test]
fn two_processes_share_the_work_without_overlap() {
    let (dir, db) = new_db();
    topic_with_messages(&db, 4, PROCESS_TEST_MESSAGES / 4);

    let first = spawn_child("member_until_idle", dir.path());
    wait_until(Duration::from_secs(30), || member_files(dir.path()) >= 1);
    let second = spawn_child("member_until_idle", dir.path());

    let mut seen = Seen::new();
    for child in [first, second] {
        let output = child.wait_with_output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "a member failed:\n{stdout}");
        let mut own = 0;
        for line in stdout.lines() {
            // The test harness output may precede the first line.
            let Some(rec) = line.split("REC ").nth(1) else {
                continue;
            };
            let mut fields = rec.split_whitespace().map(|f| f.parse::<u64>().unwrap());
            let (p, o) = (fields.next().unwrap() as u32, fields.next().unwrap());
            assert!(seen.insert((p, o)), "duplicate across processes: {p} {o}");
            own += 1;
        }
        assert!(
            own > 0,
            "each member must have received a share of the work"
        );
    }
    assert_eq!(seen, every_message(4, PROCESS_TEST_MESSAGES / 4));
}

/// A member killed in another process is detected as dead: the
/// survivors take over its partitions.
#[test]
fn partitions_of_a_killed_member_are_taken_over() {
    let (dir, db) = new_db();
    topic_with_messages(&db, 4, 0);
    let mut consumer = db.consumer("g", &["stream"], auto_commit()).unwrap();
    let mut child = spawn_child("member_until_killed", dir.path());

    poll_until_assignment(&mut [&mut consumer], &[2], &mut Seen::new());
    child.kill().unwrap();
    child.wait().unwrap();
    poll_until_assignment(&mut [&mut consumer], &[4], &mut Seen::new());
    assert_eq!(
        member_files(dir.path()),
        1,
        "the dead member's file is cleaned up"
    );
}
