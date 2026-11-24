//! Ingestion: how fast events go in, from one producer or from several
//! processes at once.

use std::time::{Duration, Instant};

use mojay::FsyncPolicy;

use super::{TOPIC, disk_bytes, log_config, new_topic, policies};
use crate::histogram::Histogram;
use crate::measurement::{Measurement, Scenario};
use crate::payload::{self, Shape, Workload};
use crate::worker::Child;

/// Batches are written to one partition: `send_batch_to` takes a partition,
/// so a batch goes round-robin rather than by key.
const BATCH: usize = 100;
const SEED: u64 = 1;

pub fn scenarios() -> Vec<Scenario> {
    let mut scenarios = Vec::new();
    let workloads = [
        Workload::Only(Shape::Click),
        Workload::Only(Shape::Order),
        Workload::Only(Shape::Document),
        Workload::Mixed,
    ];
    for (policy_name, policy) in policies() {
        for workload in workloads {
            let name = format!("ingest {}, {policy_name}, keyed send", workload.name());
            scenarios.push(Scenario {
                name: name.clone(),
                run: Box::new(move |s| vec![keyed_sends(&name, policy, workload, s.duration)]),
            });
        }
        let name = format!("ingest mixed, {policy_name}, batch {BATCH}");
        scenarios.push(Scenario {
            name: name.clone(),
            run: Box::new(move |s| vec![batches(&name, policy, s.duration)]),
        });
    }
    for (policy_name, policy) in [
        ("always", FsyncPolicy::Always),
        ("never", FsyncPolicy::Never),
    ] {
        for processes in [1, 4] {
            let name = format!("fan-in mixed, {policy_name}, {processes} producer process(es)");
            scenarios.push(Scenario {
                name: name.clone(),
                run: Box::new(move |s| vec![fan_in(&name, policy, processes, s.duration)]),
            });
        }
    }
    scenarios
}

/// One producer, one keyed `send` at a time: the partitioner picks the partition.
fn keyed_sends(
    name: &str,
    policy: FsyncPolicy,
    workload: Workload,
    duration: Duration,
) -> Measurement {
    let (_dir, db) = new_topic(log_config(policy));
    let mut producer = db.producer(TOPIC).expect("producer");
    let pool = payload::pool(workload, SEED);
    let mut latency = Histogram::default();
    let (mut sent, mut bytes) = (0u64, 0u64);
    let started = Instant::now();
    while started.elapsed() < duration {
        let message = &pool[sent as usize % pool.len()];
        let before = Instant::now();
        producer.send(message).expect("send");
        latency.record(before.elapsed());
        sent += 1;
        bytes += payload::message_bytes(message);
    }
    let elapsed = started.elapsed();
    Measurement {
        name: name.into(),
        messages: sent,
        bytes,
        elapsed,
        disk_bytes: Some(disk_bytes(&db)),
        latency: Some(("per send", latency)),
    }
}

/// One producer, batches of `BATCH` mixed messages, partitions in turn.
fn batches(name: &str, policy: FsyncPolicy, duration: Duration) -> Measurement {
    let (_dir, db) = new_topic(log_config(policy));
    let mut producer = db.producer(TOPIC).expect("producer");
    let pool = payload::pool(Workload::Mixed, SEED);
    let (chunks, _) = pool.as_chunks::<BATCH>();
    let chunk_bytes: Vec<u64> = chunks
        .iter()
        .map(|chunk| chunk.iter().map(payload::message_bytes).sum())
        .collect();
    let mut latency = Histogram::default();
    let (mut batches, mut bytes) = (0usize, 0u64);
    let started = Instant::now();
    while started.elapsed() < duration {
        let index = batches % chunks.len();
        let partition = (batches % producer.partitions() as usize) as u32;
        let before = Instant::now();
        producer
            .send_batch_to(partition, &chunks[index])
            .expect("send");
        latency.record(before.elapsed());
        batches += 1;
        bytes += chunk_bytes[index];
    }
    let elapsed = started.elapsed();
    Measurement {
        name: name.into(),
        messages: (batches * BATCH) as u64,
        bytes,
        elapsed,
        disk_bytes: Some(disk_bytes(&db)),
        latency: Some(("per batch", latency)),
    }
}

/// `processes` producer processes send to the same topic at the same time.
fn fan_in(name: &str, policy: FsyncPolicy, processes: u64, duration: Duration) -> Measurement {
    let (dir, db) = new_topic(log_config(policy));
    let mut children: Vec<Child> = (0..processes)
        .map(|seed| Child::producer(dir.path(), duration, seed))
        .collect();
    for child in &mut children {
        child.send("go");
    }
    let mut latency = Histogram::default();
    let (mut messages, mut bytes) = (0, 0);
    for child in children {
        let result = child.result();
        messages += result.messages;
        bytes += result.bytes;
        latency.merge(&result.latency);
    }
    Measurement {
        name: name.into(),
        messages,
        bytes,
        // Each child sends for exactly `duration` after its `go`.
        elapsed: duration,
        disk_bytes: Some(disk_bytes(&db)),
        latency: Some(("per send", latency)),
    }
}
