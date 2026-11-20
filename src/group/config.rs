//! Consumer configuration.

use std::time::Duration;

/// Where to start reading a partition that has no usable committed offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    /// At the oldest available message.
    Earliest,
    /// After the last readable message: only new messages will be read.
    Latest,
}

/// Consumer configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerConfig {
    /// Where to start when the group has committed nothing for a partition, or
    /// when the committed offset no longer exists (deleted by retention).
    pub start_from: StartFrom,
    /// `Some(interval)`: positions are committed automatically, at
    /// most once per interval, at the start of each `poll`, when a
    /// partition is handed over to another member, and on close. Any message
    /// returned by a `poll` is then considered processed at the next `poll`.
    ///
    /// `None`: manual commit with [`Consumer::commit`](crate::Consumer::commit).
    /// A partition handed over to another member is then not committed: the new owner
    /// resumes it at the last commit (possible duplicates, no loss).
    pub auto_commit: Option<Duration>,
    /// Approximate maximum size of the messages returned by a `poll`.
    pub max_poll_bytes: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            start_from: StartFrom::Earliest,
            auto_commit: None,
            max_poll_bytes: 1024 * 1024,
        }
    }
}
