//! Mojay : topics, partitions, offsets et consumer groups dans un dossier
//! local, sans serveur. L'équivalent de SQLite pour Kafka.
//!
//! Cette version contient la première brique : le log d'une partition
//! ([`PartitionLog`]), avec segments, index et récupération après crash.

#[cfg(not(unix))]
compile_error!("Mojay ne cible que macOS et Linux en V1.");

mod error;
mod log;
mod record;
mod segment;

pub use error::{Error, Result};
pub use log::{FsyncPolicy, LogConfig, PartitionLog};
pub use record::{Header, MAX_RECORD_SIZE, Message, Record};
