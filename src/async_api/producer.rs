//! The async version of [`Producer`].

use std::ops::Range;

use super::worker::Worker;
use crate::error::Result;
use crate::{Db, Message, Producer};

/// A topic's writer, for async code: the async version of [`Producer`], with
/// the same behavior. Available with the `async` feature.
///
/// The producer lives on its own helper thread, which runs the calls in
/// order: locks and `fsync`s never block the async runtime. Only tokio's
/// channels are used, so any runtime works.
///
/// Messages are taken by value: they move to the helper thread.
///
/// **Not cancel safe**: if a `send` future is dropped before it completes,
/// the message may still be written. Dropping an `AsyncProducer` closes it
/// in the background; [`close`](Self::close) waits until it is closed.
#[derive(Debug)]
pub struct AsyncProducer {
    topic: String,
    partitions: u32,
    worker: Worker<Producer>,
}

impl AsyncProducer {
    /// Opens a writer on a topic: the async version of [`Db::producer`].
    ///
    /// # Errors
    ///
    /// The errors of [`Db::producer`], and [`Error::WorkerStopped`] if the
    /// helper thread stopped while opening.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn open(db: &Db, topic: &str) -> Result<Self> {
        let db = db.clone();
        let topic_name = topic.to_string();
        let worker = Worker::spawn("mojay-producer", move || db.producer(&topic_name)).await?;
        let partitions = worker.run(|producer| Ok(producer.partitions())).await?;
        Ok(Self {
            topic: topic.to_string(),
            partitions,
            worker,
        })
    }

    /// The name of the topic this producer writes to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The number of partitions of the topic: see [`Producer::partitions`].
    pub fn partitions(&self) -> u32 {
        self.partitions
    }

    /// Appends a message and returns its partition and offset: see [`Producer::send`].
    ///
    /// # Errors
    ///
    /// The errors of [`Producer::send`], and [`Error::WorkerStopped`] if the
    /// helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn send(&mut self, message: Message) -> Result<(u32, u64)> {
        self.worker
            .run(move |producer| producer.send(&message))
            .await
    }

    /// Appends a message to a given partition and returns its offset: see
    /// [`Producer::send_to`].
    ///
    /// # Errors
    ///
    /// The errors of [`Producer::send_to`], and [`Error::WorkerStopped`] if
    /// the helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn send_to(&mut self, partition: u32, message: Message) -> Result<u64> {
        self.worker
            .run(move |producer| producer.send_to(partition, &message))
            .await
    }

    /// Appends messages, in order, to a given partition, and returns the
    /// range of assigned offsets: see [`Producer::send_batch_to`].
    ///
    /// # Errors
    ///
    /// The errors of [`Producer::send_batch_to`], and
    /// [`Error::WorkerStopped`] if the helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn send_batch_to(
        &mut self,
        partition: u32,
        messages: Vec<Message>,
    ) -> Result<Range<u64>> {
        self.worker
            .run(move |producer| producer.send_batch_to(partition, &messages))
            .await
    }

    /// Applies retention to all partitions now, and returns the number of
    /// deleted segments: see [`Producer::enforce_retention`].
    ///
    /// # Errors
    ///
    /// The errors of [`Producer::enforce_retention`], and
    /// [`Error::WorkerStopped`] if the helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn enforce_retention(&mut self) -> Result<usize> {
        self.worker.run(Producer::enforce_retention).await
    }

    /// Forces all messages appended by this producer to disk: see
    /// [`Producer::flush`].
    ///
    /// # Errors
    ///
    /// The errors of [`Producer::flush`], and [`Error::WorkerStopped`] if the
    /// helper thread stopped.
    ///
    /// [`Error::WorkerStopped`]: crate::Error::WorkerStopped
    pub async fn flush(&mut self) -> Result<()> {
        self.worker.run(Producer::flush).await
    }

    /// Closes the producer after the calls already made, and waits until it
    /// is closed (the final `fsync` of the `Interval` policy included).
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
