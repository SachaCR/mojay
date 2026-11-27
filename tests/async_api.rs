//! Async API tests (`tokio` feature): `AsyncProducer` and `AsyncConsumer`.
//!
//! Each test runs on a single-thread tokio runtime: a call that blocked the
//! runtime would stop every other task, which the tests would see.

#![cfg(feature = "tokio")]

#[allow(dead_code)]
mod common;

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Waker};
use std::time::Duration;

use common::{config, message, new_db};
use mojay::{AsyncConsumer, AsyncProducer, ConsumerConfig, Db, Error, FsyncPolicy};

const COUNT: u64 = 50;
const POLL: Duration = Duration::from_millis(200);

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(future)
}

/// A topic with 2 partitions holding `count` messages.
fn topic_with_messages(db: &Db, count: u64) {
    db.create_topic("t", config(2, 1 << 20, FsyncPolicy::Never))
        .unwrap();
    let mut producer = db.producer("t").unwrap();
    for i in 0..count {
        producer.send(&message(i)).unwrap();
    }
}

/// Polls until `count` messages are read, and returns their (partition, offset), sorted.
async fn read(consumer: &mut AsyncConsumer, count: usize) -> Vec<(u32, u64)> {
    let mut read = Vec::new();
    while read.len() < count {
        let records = consumer.poll(POLL).await.unwrap();
        assert!(!records.is_empty(), "only {} of {count} read", read.len());
        read.extend(records.iter().map(|r| (r.partition, r.offset)));
    }
    read.sort_unstable();
    read
}

fn auto_commit() -> ConsumerConfig {
    ConsumerConfig {
        auto_commit: Some(Duration::from_secs(60)),
        ..ConsumerConfig::default()
    }
}

#[test]
fn produce_then_consume() {
    let (_dir, db) = new_db();
    db.create_topic("t", config(2, 1 << 20, FsyncPolicy::Never))
        .unwrap();
    block_on(async {
        let mut producer = AsyncProducer::open(&db, "t").await.unwrap();
        assert_eq!(producer.topic(), "t");
        assert_eq!(producer.partitions(), 2);
        let mut sent = Vec::new();
        for i in 0..COUNT {
            sent.push(producer.send(message(i)).await.unwrap());
        }
        let batch = vec![message(COUNT), message(COUNT + 1)];
        let range = producer.send_batch_to(1, batch).await.unwrap();
        sent.extend(range.map(|offset| (1, offset)));
        producer.flush().await.unwrap();
        producer.close().await.unwrap();

        let mut consumer = AsyncConsumer::open(&db, "g", &["t"], ConsumerConfig::default())
            .await
            .unwrap();
        assert_eq!(consumer.group(), "g");
        sent.sort_unstable();
        assert_eq!(read(&mut consumer, sent.len()).await, sent);
        assert_eq!(
            consumer.assignment().await.unwrap(),
            vec![("t".to_string(), 0), ("t".to_string(), 1)]
        );
        consumer.close().await.unwrap();
    });
}

#[test]
fn waiting_poll_does_not_block_the_runtime() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 0);
    block_on(async {
        let mut consumer = AsyncConsumer::open(&db, "g", &["t"], ConsumerConfig::default())
            .await
            .unwrap();
        let ticks = Arc::new(AtomicU32::new(0));
        let ticker = tokio::spawn({
            let ticks = Arc::clone(&ticks);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    ticks.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        // Nothing to read: `poll` waits for its whole timeout.
        assert!(
            consumer
                .poll(Duration::from_millis(300))
                .await
                .unwrap()
                .is_empty()
        );
        ticker.abort();
        // About 30 if the runtime stayed free, 0 if `poll` had blocked it.
        assert!(ticks.load(Ordering::Relaxed) >= 10);
    });
}

/// Starts a `poll` (its job is queued on the helper thread), lets the helper
/// thread work for `wait`, then cancels the `poll` by dropping it.
fn cancel_poll(consumer: &mut AsyncConsumer, wait: Duration) {
    let mut poll = pin!(consumer.poll(POLL));
    let mut context = Context::from_waker(Waker::noop());
    assert!(poll.as_mut().poll(&mut context).is_pending());
    std::thread::sleep(wait);
}

/// Before the helper thread reads (no wait), and after it sent the records
/// (it needs far less than 200 ms): both ways of cancelling are covered.
const CANCEL_MOMENTS: [Duration; 2] = [Duration::ZERO, Duration::from_millis(200)];

#[test]
fn cancelled_poll_loses_nothing() {
    for wait in CANCEL_MOMENTS {
        let (_dir, db) = new_db();
        topic_with_messages(&db, COUNT);
        block_on(async {
            let mut consumer = AsyncConsumer::open(&db, "g", &["t"], ConsumerConfig::default())
                .await
                .unwrap();
            cancel_poll(&mut consumer, wait);
            let read = read(&mut consumer, COUNT as usize).await;
            assert_eq!(read.len(), COUNT as usize, "cancelled after {wait:?}");

            // The commit covers exactly what was delivered: nothing is left.
            consumer.commit().await.unwrap();
            consumer.close().await.unwrap();
            let mut consumer = AsyncConsumer::open(&db, "g", &["t"], ConsumerConfig::default())
                .await
                .unwrap();
            assert!(consumer.poll(POLL).await.unwrap().is_empty());
        });
    }
}

#[test]
fn auto_commit_on_close_skips_no_cancelled_message() {
    for wait in CANCEL_MOMENTS {
        let (_dir, db) = new_db();
        topic_with_messages(&db, COUNT);
        block_on(async {
            let mut consumer = AsyncConsumer::open(&db, "g", &["t"], auto_commit())
                .await
                .unwrap();
            cancel_poll(&mut consumer, wait);
            // Commits automatically, then releases the partitions.
            consumer.close().await.unwrap();

            let mut consumer = AsyncConsumer::open(&db, "g", &["t"], auto_commit())
                .await
                .unwrap();
            let read = read(&mut consumer, COUNT as usize).await;
            assert_eq!(read.len(), COUNT as usize, "cancelled after {wait:?}");
        });
    }
}

#[test]
fn close_releases_the_partitions() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 0);
    block_on(async {
        let first = AsyncConsumer::open(&db, "g", &["t"], ConsumerConfig::default())
            .await
            .unwrap();
        assert_eq!(first.assignment().await.unwrap().len(), 2);
        first.close().await.unwrap();

        let second = AsyncConsumer::open(&db, "g", &["t"], ConsumerConfig::default())
            .await
            .unwrap();
        assert_eq!(second.assignment().await.unwrap().len(), 2);
    });
}

#[test]
fn open_errors_are_returned() {
    let (_dir, db) = new_db();
    block_on(async {
        let producer = AsyncProducer::open(&db, "missing").await;
        assert!(matches!(producer, Err(Error::UnknownTopic(name)) if name == "missing"));
        let consumer = AsyncConsumer::open(&db, "g", &["missing"], ConsumerConfig::default()).await;
        assert!(matches!(consumer, Err(Error::UnknownTopic(name)) if name == "missing"));
    });
}

#[test]
fn send_errors_are_returned() {
    let (_dir, db) = new_db();
    topic_with_messages(&db, 0);
    block_on(async {
        let mut producer = AsyncProducer::open(&db, "t").await.unwrap();
        let result = producer.send_to(7, message(0)).await;
        assert!(matches!(
            result,
            Err(Error::UnknownPartition {
                partition: 7,
                partitions: 2
            })
        ));
        // The producer is still usable.
        assert_eq!(producer.send_to(0, message(0)).await.unwrap(), 0);
    });
}
