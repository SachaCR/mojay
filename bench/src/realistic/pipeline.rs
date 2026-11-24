//! Producer → consumers, as an application runs them: a producer at a fixed
//! rate in this process, consumer *processes* in groups. Delivery latency is
//! measured from the time each message was **due** to be sent, not the time
//! it was sent: if the producer falls behind, the delay counts (no
//! coordinated omission).

use std::collections::BTreeSet;
use std::thread;
use std::time::{Duration, Instant};

use mojay::{Db, FsyncPolicy};

use super::{TOPIC, disk_bytes, log_config, new_topic, produce_at_rate};
use crate::histogram::Histogram;
use crate::measurement::{Measurement, Scenario};
use crate::worker::Child;

/// Time for the consumers to share the partitions before the first message.
const GROUP_SETTLE: Duration = Duration::from_secs(1);
/// After the producer stops, how long the consumers get to catch up.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// One pipeline: a producer at `rate`, one consumer process per entry of `groups`.
#[derive(Clone, Copy)]
struct Pipeline {
    policy: FsyncPolicy,
    rate: u64,
    groups: &'static [&'static str],
    /// A consumer joins a third of the way through, the first one is killed
    /// two thirds of the way through.
    churn: bool,
}

const ALWAYS: FsyncPolicy = FsyncPolicy::Always;
const INTERVAL: FsyncPolicy = FsyncPolicy::Interval(Duration::from_millis(100));
const NEVER: FsyncPolicy = FsyncPolicy::Never;
const ONE: &[&str] = &["app"];
const WORK_QUEUE: &[&str] = &["workers", "workers", "workers", "workers"];
const FAN_OUT: &[&str] = &["billing", "search", "audit"];

pub fn scenarios() -> Vec<Scenario> {
    let pipeline = |policy, rate, groups, churn| Pipeline {
        policy,
        rate,
        groups,
        churn,
    };
    let list = [
        (
            "delivery, always, 100 msg/s",
            pipeline(ALWAYS, 100, ONE, false),
        ),
        (
            "delivery, interval, 1,000 msg/s",
            pipeline(INTERVAL, 1_000, ONE, false),
        ),
        (
            "delivery, never, 1,000 msg/s",
            pipeline(NEVER, 1_000, ONE, false),
        ),
        (
            "delivery, never, 10,000 msg/s",
            pipeline(NEVER, 10_000, ONE, false),
        ),
        (
            "delivery, never, 50,000 msg/s",
            pipeline(NEVER, 50_000, ONE, false),
        ),
        (
            "work queue (4 procs), always, 100 msg/s",
            pipeline(ALWAYS, 100, WORK_QUEUE, false),
        ),
        (
            "work queue (4 procs), never, 10,000 msg/s",
            pipeline(NEVER, 10_000, WORK_QUEUE, false),
        ),
        (
            "rebalance (join + crash), never, 10,000 msg/s",
            pipeline(NEVER, 10_000, &WORK_QUEUE[..2], true),
        ),
        (
            "fan-out (3 groups), never, 10,000 msg/s",
            pipeline(NEVER, 10_000, FAN_OUT, false),
        ),
    ];
    list.into_iter()
        .map(|(name, pipeline)| Scenario {
            name: name.into(),
            run: Box::new(move |s| vec![run(name, pipeline, s.duration)]),
        })
        .collect()
}

fn run(name: &str, pipeline: Pipeline, duration: Duration) -> Measurement {
    let (dir, db) = new_topic(log_config(pipeline.policy));
    let mut consumers: Vec<Child> = pipeline
        .groups
        .iter()
        .map(|group| Child::consumer(dir.path(), group))
        .collect();
    thread::sleep(GROUP_SETTLE);

    let producer_db = db.clone();
    let rate = pipeline.rate;
    let producer = thread::spawn(move || produce_at_rate(&producer_db, rate, duration));
    if pipeline.churn {
        churn(dir.path(), &mut consumers, pipeline.groups[0], duration);
    }
    let (sent, bytes) = producer.join().expect("producer thread");
    wait_until_drained(&db, pipeline.groups, sent);

    let mut latency = Histogram::default();
    for consumer in &mut consumers {
        consumer.send("stop");
    }
    for consumer in consumers {
        latency.merge(&consumer.result().latency);
    }
    Measurement {
        name: name.into(),
        messages: sent,
        bytes,
        elapsed: duration,
        disk_bytes: Some(disk_bytes(&db)),
        latency: Some(("delivery", latency)),
    }
}

/// A new member joins the group, then the oldest member crashes: the group
/// must hand its partitions over without losing messages.
fn churn(data: &std::path::Path, consumers: &mut Vec<Child>, group: &str, duration: Duration) {
    thread::sleep(duration / 3);
    consumers.push(Child::consumer(data, group));
    thread::sleep(duration / 3);
    consumers.remove(0).kill();
}

/// Waits until every group has committed all `sent` messages.
fn wait_until_drained(db: &Db, groups: &[&str], sent: u64) {
    let groups: BTreeSet<&str> = groups.iter().copied().collect();
    let deadline = Instant::now() + DRAIN_TIMEOUT;
    loop {
        let offsets = db.committed_offsets(TOPIC).expect("committed offsets");
        let drained = groups.iter().all(|group| {
            let committed: u64 = offsets
                .iter()
                .filter(|o| o.group == *group)
                .map(|o| o.offset)
                .sum();
            committed >= sent
        });
        if drained {
            return;
        }
        if Instant::now() > deadline {
            eprintln!("warning: consumers did not catch up within {DRAIN_TIMEOUT:?}");
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
}
