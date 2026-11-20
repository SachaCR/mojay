use std::io;
use std::path::PathBuf;

/// Errors returned by Mojay.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Invalid on-disk data that cannot be repaired
    /// automatically (unreadable segment header, damaged sealed segment...).
    #[error("corrupt data in {path}: {reason}")]
    Corrupt { path: PathBuf, reason: String },

    /// The requested offset does not exist (yet, or any more) in the partition.
    #[error("offset {offset} out of range (available: {start}..{end})")]
    OffsetOutOfRange { offset: u64, start: u64, end: u64 },

    #[error("record too large: {size} bytes (maximum {max})")]
    RecordTooLarge { size: usize, max: usize },

    #[error("invalid record: {0}")]
    InvalidRecord(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Another writer (process or object) already holds the topic lock.
    #[error("topic \"{0}\" already has a writer")]
    Busy(String),

    #[error("topic \"{0}\" already exists")]
    TopicExists(String),

    #[error("topic \"{0}\" does not exist")]
    UnknownTopic(String),

    #[error("invalid name \"{name}\": {reason}")]
    InvalidName { name: String, reason: String },

    #[error("partition {partition} does not exist (the topic has {partitions})")]
    UnknownPartition { partition: u32, partitions: u32 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Shorthand to build an [`Error::Corrupt`].
pub(crate) fn corrupt(path: impl Into<PathBuf>, reason: impl Into<String>) -> Error {
    Error::Corrupt {
        path: path.into(),
        reason: reason.into(),
    }
}
