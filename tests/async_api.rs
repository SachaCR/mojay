//! Async API tests (`async` feature): `AsyncProducer` and `AsyncConsumer`.
//!
//! Each test runs on a single-thread tokio runtime: a call that blocked the
//! runtime would stop every other task, which the tests would see.

#![cfg(feature = "async")]

#[allow(dead_code)]
mod common;

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

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
    produce(db, count);
}

fn produce(db: &Db, count: u64) {
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
    const WAIT: Duration = Duration::from_secs(1);
    let (_dir, db) = new_db();
    topic_with_messages(&db, 0);
    block_on(async {
        let mut consumer = AsyncConsumer::open(&db, "g", &["t"], ConsumerConfig::default())
            .await
            .unwrap();
        // The runtime has a single thread: this task can only run while
        // `poll` waits if `poll` hands the thread back to the runtime.
        let start = Instant::now();
        let ran_after = Arc::new(Mutex::new(None));
        tokio::spawn({
            let ran_after = Arc::clone(&ran_after);
            async move {
                *ran_after.lock().unwrap() = Some(start.elapsed());
            }
        });
        // Nothing to read: `poll` waits for its whole timeout.
        assert!(consumer.poll(WAIT).await.unwrap().is_empty());

        // Checked before this task yields again. A `poll` that held the
        // thread would leave `None`, or (if it held it then awaited) let the
        // task run only at the end. No timer is counted: CI machines have
        // slow, irregular timers.
        let ran_after = ran_after
            .lock()
            .unwrap()
            .expect("the other task never ran while poll waited");
        assert!(
            ran_after < WAIT / 2,
            "the other task ran only after {ran_after:?}"
        );
    });
}

/// Starts a long `poll` on an empty topic, so it is certainly still waiting
/// when first polled (its job is queued on the helper thread), writes
/// `COUNT` messages, and cancels the `poll` by dropping it:
/// - `wait` zero: dropped before the messages are written, so before the
///   helper thread can send them;
/// - otherwise: dropped `wait` after they are written, once the helper
///   thread has sent them (it needs far less).
fn cancel_poll_while_messages_arrive(consumer: &mut AsyncConsumer, db: &Db, wait: Duration) {
    let mut poll = Box::pin(consumer.poll(Duration::from_secs(10)));
    let mut context = Context::from_waker(Waker::noop());
    assert!(
        poll.as_mut().poll(&mut context).is_pending(),
        "the topic is empty: poll must wait"
    );
    if wait.is_zero() {
        drop(poll);
        produce(db, COUNT);
    } else {
        produce(db, COUNT);
        std::thread::sleep(wait);
        drop(poll);
    }
}

/// Both moments a `poll` can be cancelled: before the helper thread sends
/// the records, and after.
const CANCEL_MOMENTS: [Duration; 2] = [Duration::ZERO, Duration::from_millis(200)];

#[test]
fn cancelled_poll_loses_nothing() {
    for wait in CANCEL_MOMENTS {
        let (_dir, db) = new_db();
        topic_with_messages(&db, 0);
        block_on(async {
            let mut consumer = AsyncConsumer::open(&db, "g", &["t"], ConsumerConfig::default())
                .await
                .unwrap();
            cancel_poll_while_messages_arrive(&mut consumer, &db, wait);
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
        topic_with_messages(&db, 0);
        block_on(async {
            let mut consumer = AsyncConsumer::open(&db, "g", &["t"], auto_commit())
                .await
                .unwrap();
            cancel_poll_while_messages_arrive(&mut consumer, &db, wait);
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
