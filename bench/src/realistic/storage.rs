//! What depends on the amount of data: reading a large backlog, running for
//! a long time with retention, and reopening after a crash.

use std::fs;
use std::io::BufRead;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use mojay::{ConsumerConfig, Db, FsyncPolicy, LogConfig, Retention};

use super::{TOPIC, delivery_latency, disk_bytes, log_config, new_topic, produce_at_rate};
use crate::histogram::Histogram;
use crate::measurement::{Measurement, Scenario, Settings};
use crate::payload::{self, Workload};

const PREFILL_BATCH: usize = 100;
const SEED: u64 = 3;
const MIB: u64 = 1024 * 1024;

/// Soak: rate, number of report windows, and a small segment size and
/// retention limit so that rotation and deletion happen many times.
const SOAK_RATE: u64 = 20_000;
const SOAK_WINDOWS: u32 = 6;
const SOAK_SEGMENT_BYTES: u64 = 8 * MIB;
const SOAK_RETENTION_BYTES: u64 = 32 * MIB;
const POLL_TIMEOUT: Duration = Duration::from_millis(100);
/// Recovery: large enough that the whole backlog stays in the active segments.
const RECOVERY_SEGMENT_BYTES: u64 = 2048 * MIB;
const INDEX_EXTENSION: &str = "idx";

pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "catch-up: read the backlog with a group".into(),
            run: Box::new(|s| vec![catch_up("catch-up: read the backlog with a group", s)]),
        },
        Scenario {
            name: "soak".into(),
            run: Box::new(soak),
        },
        Scenario {
            name: "reopen after a crash".into(),
            run: Box::new(reopen),
        },
    ]
}

/// Writes about `target_bytes` of mixed messages, in batches, partitions in
/// turn. Returns the number of messages and payload bytes.
fn prefill(db: &Db, target_bytes: u64) -> (u64, u64) {
    let mut producer = db.producer(TOPIC).expect("producer");
    let pool = payload::pool(Workload::Mixed, SEED);
    let (chunks, _) = pool.as_chunks::<PREFILL_BATCH>();
    let (mut batches, mut bytes) = (0usize, 0u64);
    while bytes < target_bytes {
        let chunk = &chunks[batches % chunks.len()];
        let partition = (batches % producer.partitions() as usize) as u32;
        producer.send_batch_to(partition, chunk).expect("prefill");
        bytes += chunk.iter().map(payload::message_bytes).sum::<u64>();
        batches += 1;
    }
    ((batches * PREFILL_BATCH) as u64, bytes)
}

/// A consumer that was away comes back to a large backlog and reads it all.
fn catch_up(name: &str, settings: &Settings) -> Measurement {
    let (_dir, db) = new_topic(log_config(FsyncPolicy::Never));
    let (written, bytes) = prefill(&db, settings.backlog_bytes);
    if settings.cold {
        wait_for_cold_cache();
    }
    let config = ConsumerConfig {
        auto_commit: Some(Duration::from_secs(1)),
        ..ConsumerConfig::default()
    };
    let mut consumer = db.consumer("catch-up", &[TOPIC], config).expect("consumer");
    let started = Instant::now();
    let mut read = 0;
    while read < written {
        read += consumer.poll(POLL_TIMEOUT).expect("poll").len() as u64;
    }
    let elapsed = started.elapsed();
    Measurement {
        name: format!("{name}, {} MiB", settings.backlog_bytes / MIB),
        messages: read,
        bytes,
        elapsed,
        disk_bytes: Some(disk_bytes(&db)),
        latency: None,
    }
}

/// Dropping the page cache needs root: the bench asks instead of doing it.
fn wait_for_cold_cache() {
    eprintln!(
        "Backlog written. Drop the page cache now (macOS: `sudo purge`, Linux: \
         `sync; echo 3 | sudo tee /proc/sys/vm/drop_caches`), then press Enter."
    );
    let _ = std::io::stdin().lock().lines().next();
}

/// A producer at a steady rate and a consumer, for `settings.soak`, with
/// rotation and retention: latency and disk size must stay flat.
fn soak(settings: &Settings) -> Vec<Measurement> {
    let log = LogConfig {
        fsync: FsyncPolicy::Interval(Duration::from_millis(100)),
        segment_max_bytes: SOAK_SEGMENT_BYTES,
        retention: Retention {
            max_bytes: Some(SOAK_RETENTION_BYTES),
            max_age: None,
        },
        ..LogConfig::default()
    };
    let (_dir, db) = new_topic(log);
    let config = ConsumerConfig {
        auto_commit: Some(Duration::from_secs(1)),
        ..ConsumerConfig::default()
    };
    let mut consumer = db.consumer("soak", &[TOPIC], config).expect("consumer");
    let producer_db = db.clone();
    let duration = settings.soak;
    let producer = thread::spawn(move || produce_at_rate(&producer_db, SOAK_RATE, duration));

    let window = duration / SOAK_WINDOWS;
    let windows = (1..=SOAK_WINDOWS)
        .map(|number| {
            let mut measurement = consume_for(&mut consumer, window);
            measurement.name = format!(
                "soak {number}/{SOAK_WINDOWS}, interval, {SOAK_RATE} msg/s, retention {} MiB/partition",
                SOAK_RETENTION_BYTES / MIB
            );
            measurement.disk_bytes = Some(disk_bytes(&db));
            measurement
        })
        .collect();
    producer.join().expect("producer thread");
    windows
}

/// Polls for `window`, measuring delivery latency.
fn consume_for(consumer: &mut mojay::Consumer, window: Duration) -> Measurement {
    let mut latency = Histogram::default();
    let (mut messages, mut bytes) = (0, 0);
    let started = Instant::now();
    while started.elapsed() < window {
        for record in consumer.poll(POLL_TIMEOUT).expect("poll") {
            if let Some(delay) = delivery_latency(&record.headers) {
                latency.record(delay);
            }
            messages += 1;
            bytes += record.value.map_or(0, |v| v.len() as u64);
        }
    }
    Measurement {
        name: String::new(),
        messages,
        bytes,
        elapsed: started.elapsed(),
        disk_bytes: None,
        latency: Some(("delivery", latency)),
    }
}

/// Time to open a producer on large active segments: with their index, then
/// without it (as after a crash that lost it), which forces a full scan.
fn reopen(settings: &Settings) -> Vec<Measurement> {
    let (dir, db) = new_topic(LogConfig {
        segment_max_bytes: RECOVERY_SEGMENT_BYTES,
        ..log_config(FsyncPolicy::Never)
    });
    let (messages, bytes) = prefill(&db, settings.backlog_bytes);
    let intact = time_to_open(&db);
    let removed = remove_indexes(dir.path());
    assert!(
        removed > 0,
        "no index file found under {}",
        dir.path().display()
    );
    let rebuilt = time_to_open(&db);
    let size = settings.backlog_bytes / MIB;
    [("index intact", intact), ("index deleted", rebuilt)]
        .into_iter()
        .map(|(case, elapsed)| {
            let mut latency = Histogram::default();
            latency.record(elapsed);
            Measurement {
                name: format!("reopen {size} MiB of active segments, {case}"),
                messages,
                bytes,
                elapsed,
                disk_bytes: Some(disk_bytes(&db)),
                latency: Some(("open", latency)),
            }
        })
        .collect()
}

fn time_to_open(db: &Db) -> Duration {
    let started = Instant::now();
    let producer = db.producer(TOPIC).expect("producer");
    let elapsed = started.elapsed();
    drop(producer);
    elapsed
}

/// Deletes every segment index under `dir`, and returns how many.
fn remove_indexes(dir: &Path) -> usize {
    let mut removed = 0;
    for entry in fs::read_dir(dir).expect("read folder") {
        let path = entry.expect("folder entry").path();
        if path.is_dir() {
            removed += remove_indexes(&path);
        } else if path.extension().is_some_and(|e| e == INDEX_EXTENSION) {
            fs::remove_file(&path).expect("remove index");
            removed += 1;
        }
    }
    removed
}
