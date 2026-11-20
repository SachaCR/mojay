//! Mojay: topics, partitions, offsets and consumer groups in a local
//! directory, with no server. The SQLite of Kafka.
//!
//! ```no_run
//! use std::time::Duration;
//! use mojay::{ConsumerConfig, Db, Message, TopicConfig};
//!
//! let db = Db::open("./data")?;
//! db.create_topic("orders", TopicConfig { partitions: 4, ..Default::default() })?;
//!
//! let mut producer = db.producer("orders")?;
//! producer.send(&Message::new("client-42", "order"))?;
//!
//! let mut consumer = db.consumer("billing", &["orders"], ConsumerConfig::default())?;
//! for record in consumer.poll(Duration::from_millis(500))? {
//!     println!("{} / {} : {:?}", record.partition, record.offset, record.value);
//! }
//! consumer.commit()?;
//! # Ok::<(), mojay::Error>(())
//! ```

#[cfg(not(unix))]
compile_error!("Mojay only targets macOS and Linux in V1.");

mod db;
mod durable;
mod error;
mod group;
mod index;
pub mod inspect;
mod lock;
mod log;
mod meta;
mod partition_state;
mod partitioner;
mod record;
mod segment;
mod topic;

pub use db::Db;
pub use error::{Error, Result};
pub use group::{
    CommittedOffset, Consumer, ConsumerConfig, ConsumerRecord, RebalanceEvent, StartFrom,
};
pub use log::{FsyncPolicy, LogConfig, PartitionLog, Retention};
pub use record::{Header, MAX_RECORD_SIZE, Message, Record};
pub use topic::{
    PartitionDescription, Producer, SegmentDescription, TopicConfig, TopicDescription, TopicReader,
};
