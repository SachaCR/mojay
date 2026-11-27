//! The async API, with the `async` feature: [`AsyncProducer`] and
//! [`AsyncConsumer`].
//!
//! Each one owns a sync [`Producer`](crate::Producer) or
//! [`Consumer`](crate::Consumer) on a helper thread (see `worker`): the sync
//! code is reused as is, calls run in order, and no blocking call (file I/O,
//! lock, `fsync`, the wait of `poll`) ever runs on an async runtime thread.

mod consumer;
mod producer;
mod worker;

pub use consumer::AsyncConsumer;
pub use producer::AsyncProducer;
