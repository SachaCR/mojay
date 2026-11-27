//! The async version of [`Consumer`].

use std::mem;
use std::time::Duration;

use super::worker::{Jobs, Worker};
use crate::error::Result;
use crate::{Consumer, ConsumerConfig, ConsumerRecord, Db, RebalanceEvent};

/// A member of a consumer group, for async code: the async version of
/// [`Consumer`], with the same behavior. Available with the `async` feature.
///
/// The consumer lives on its own helper thread, which runs the calls in
/// order: a waiting [`poll`](Self::poll) never blocks the async runtime. Only
/// tokio's channels are used, so any runtime works.
///
/// As with [`Consumer`], call `poll` regularly: it is when this member takes
/// part in the group. Dropping an `AsyncConsumer` closes it in the
/// background; [`close`](Self::close) waits until its partitions are released.
///
/// ```no_run
/// use std::time::Duration;
/// use mojay::{AsyncConsumer, AsyncProducer, ConsumerConfig, Db, Message};
///
/// async fn run(db: &Db) -> mojay::Result<()> {
///     let mut producer = AsyncProducer::open(db, "orders").await?;
///     producer.send(Message::new("client-42", "order")).await?;
///     producer.close().await?;
///
///     let mut consumer =
///         AsyncConsumer::open(db, "billing", &["orders"], ConsumerConfig::default()).await?;
///     for record in consumer.poll(Duration::from_millis(500)).await? {
///         println!("{} / {}", record.partition, record.offset);
///     }
///     consumer.commit().await?;
///     consumer.close().await
/// }
/// ```
#[derive(Debug)]
pub struct AsyncConsumer {
    group: String,
    worker: Worker<Consumer>,
}

impl AsyncConsumer {
    /// Opens a consumer in group `group`, subscribed to `topics`: the async
    /// version of [`Db::consumer`].
    ///
    /// # Errors
    ///
    /// The errors of [`Db::consumer`], and [`Error::WorkerStopped`] if the
    /// helper thread stopped while opening.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn open(
        db: &Db,
        group: &str,
        topics: &[&str],
        config: ConsumerConfig,
    ) -> Result<Self> {
        let db = db.clone();
        let group_name = group.to_string();
        let topics: Vec<String> = topics.iter().map(|topic| topic.to_string()).collect();
        let worker = Worker::spawn("mojay-consumer", move || {
            let topics: Vec<&str> = topics.iter().map(String::as_str).collect();
            db.consumer(&group_name, &topics, config)
        })
        .await?;
        Ok(Self {
            group: group.to_string(),
            worker,
        })
    }

    /// The name of the group this consumer is a member of.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Waits at most `timeout` for messages, and returns them as soon as
    /// there are any: see [`Consumer::poll`].
    ///
    /// **Cancel safe**: if the returned future is dropped before it completes
    /// (with `tokio::time::timeout` or `tokio::select!`, for example), the
    /// messages it may have read are not lost. The next `poll` returns them,
    /// and no commit skips them in the meantime.
    ///
    /// # Errors
    ///
    /// The errors of [`Consumer::poll`], and [`Error::WorkerStopped`] if the
    /// helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        let jobs = self.worker.jobs();
        let delivery = self
            .worker
            .call(move |consumer, reply| match consumer.poll(timeout) {
                Ok(records) => {
                    let delivery = Delivery { records, jobs };
                    // The caller stopped waiting: take the records back right
                    // away, before any other queued job (a commit, a close).
                    if let Err(Ok(mut delivery)) = reply.send(Ok(delivery)) {
                        consumer.rewind(&delivery.take());
                    }
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            })
            .await?;
        Ok(delivery.into_records())
    }

    /// Commits the position of each owned partition: see [`Consumer::commit`].
    ///
    /// # Errors
    ///
    /// The errors of [`Consumer::commit`], and [`Error::WorkerStopped`] if the
    /// helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn commit(&mut self) -> Result<()> {
        self.worker.run(Consumer::commit).await
    }

    /// The partitions owned by this consumer, sorted: see [`Consumer::assignment`].
    ///
    /// # Errors
    ///
    /// [`Error::WorkerStopped`] if the helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn assignment(&self) -> Result<Vec<(String, u32)>> {
        self.worker.run(|consumer| Ok(consumer.assignment())).await
    }

    /// The next offset this consumer will return for a partition it owns:
    /// see [`Consumer::position`].
    ///
    /// # Errors
    ///
    /// [`Error::WorkerStopped`] if the helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn position(&self, topic: &str, partition: u32) -> Result<Option<u64>> {
        let topic = topic.to_string();
        self.worker
            .run(move |consumer| Ok(consumer.position(&topic, partition)))
            .await
    }

    /// The partitions received or handed over since the last call, in order:
    /// see [`Consumer::take_rebalance_events`].
    ///
    /// # Errors
    ///
    /// [`Error::WorkerStopped`] if the helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn take_rebalance_events(&mut self) -> Result<Vec<RebalanceEvent>> {
        self.worker
            .run(|consumer| Ok(consumer.take_rebalance_events()))
            .await
    }

    /// Closes the consumer after the calls already made, and waits until it
    /// is closed: with automatic commit, positions are committed, then the
    /// partitions are released and the member leaves the group.
    ///
    /// # Errors
    ///
    /// [`Error::WorkerStopped`] if the helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn close(self) -> Result<()> {
        self.worker.close().await
    }
}

/// Records on their way from the helper thread to the caller of `poll`.
///
/// If they are dropped before the caller takes them (its future was dropped
/// after the records were sent), the consumer takes them back: a rewind job
/// is queued. With `&mut self` on `poll`, no other call can be queued before it.
struct Delivery {
    records: Vec<ConsumerRecord>,
    jobs: Jobs<Consumer>,
}

impl Delivery {
    fn take(&mut self) -> Vec<ConsumerRecord> {
        mem::take(&mut self.records)
    }

    fn into_records(mut self) -> Vec<ConsumerRecord> {
        self.take()
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if !self.records.is_empty() {
            let records = self.take();
            self.jobs.push(move |consumer| consumer.rewind(&records));
        }
    }
}
