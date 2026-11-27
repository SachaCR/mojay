use std::io;
use std::path::PathBuf;

/// Errors returned by Mojay.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A file system operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Invalid on-disk data that cannot be repaired
    /// automatically (unreadable segment header, damaged sealed segment...).
    #[error("corrupt data in {path}: {reason}")]
    Corrupt {
        /// The damaged file.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },

    /// The requested offset does not exist (yet, or any more) in the partition.
    #[error("offset {offset} out of range (available: {start}..{end})")]
    OffsetOutOfRange {
        /// The requested offset.
        offset: u64,
        /// First available offset.
        start: u64,
        /// Last offset that can be requested: reading there returns nothing yet.
        end: u64,
    },

    /// A message whose encoded size exceeds
    /// [`MAX_RECORD_SIZE`](crate::MAX_RECORD_SIZE). Nothing was written.
    #[error("record too large: {size} bytes (maximum {max})")]
    RecordTooLarge {
        /// Encoded size of the message.
        size: usize,
        /// The maximum allowed.
        max: usize,
    },

    /// A message that cannot be encoded (a header name too long, for example).
    /// Nothing was written.
    #[error("invalid record: {0}")]
    InvalidRecord(String),

    /// An invalid configuration or argument, with the reason.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A topic with this name already exists.
    #[error("topic \"{0}\" already exists")]
    TopicExists(String),

    /// No topic has this name.
    #[error("topic \"{0}\" does not exist")]
    UnknownTopic(String),

    /// A topic or group name that cannot be used as a directory name.
    #[error("invalid name \"{name}\": {reason}")]
    InvalidName {
        /// The rejected name.
        name: String,
        /// Why it is rejected.
        reason: String,
    },

    /// A partition number beyond those of the topic.
    #[error("partition {partition} does not exist (the topic has {partitions})")]
    UnknownPartition {
        /// The requested partition.
        partition: u32,
        /// The number of partitions of the topic: valid ones are `0..partitions`.
        partitions: u32,
    },

    /// The helper thread of an `AsyncProducer` or `AsyncConsumer` (`async`
    /// feature) stopped without answering: it panicked. The async producer
    /// or consumer can no longer be used. The variant exists without the
    /// feature too, so that enabling it never breaks a `match` elsewhere.
    #[error("the helper thread of the async producer or consumer stopped")]
    WorkerStopped,
}

/// The result type of Mojay operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Shorthand to build an [`Error::Corrupt`].
pub(crate) fn corrupt(path: impl Into<PathBuf>, reason: impl Into<String>) -> Error {
    Error::Corrupt {
        path: path.into(),
        reason: reason.into(),
    }
}
