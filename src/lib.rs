//! Mojay : topics, partitions, offsets et consumer groups dans un dossier
//! local, sans serveur. L'équivalent de SQLite pour Kafka.
//!
//! ```no_run
//! use std::time::Duration;
//! use mojay::{ConsumerConfig, Db, Message, TopicConfig};
//!
//! let db = Db::open("./data")?;
//! db.create_topic("orders", TopicConfig { partitions: 4, ..Default::default() })?;
//!
//! let mut producer = db.producer("orders")?;
//! producer.send(&Message::new("client-42", "commande"))?;
//!
//! let mut consumer = db.consumer("billing", &["orders"], ConsumerConfig::default())?;
//! for record in consumer.poll(Duration::from_millis(500))? {
//!     println!("{} / {} : {:?}", record.partition, record.offset, record.value);
//! }
//! consumer.commit()?;
//! # Ok::<(), mojay::Error>(())
//! ```

#[cfg(not(unix))]
compile_error!("Mojay ne cible que macOS et Linux en V1.");

mod db;
mod durable;
mod error;
mod group;
mod index;
mod lock;
mod log;
mod meta;
mod partitioner;
mod read_limits;
mod record;
mod segment;
mod topic;

pub use db::Db;
pub use error::{Error, Result};
pub use group::{Consumer, ConsumerConfig, ConsumerRecord, RebalanceEvent, StartFrom};
pub use log::{FsyncPolicy, LogConfig, PartitionLog};
pub use record::{Header, MAX_RECORD_SIZE, Message, Record};
pub use topic::{Producer, TopicConfig, TopicReader};
