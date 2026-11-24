//! Serverless consumer groups: the file system acts as the coordinator.
//!
//! ```text
//! groups/<group>/
//!   members/<id>.lock             # flock held = live member
//!   partitions/<topic>-<p>.lock   # flock held = partition owned by a member
//!   offsets.lock                  # flock held while a member writes a commit
//!   offsets/<topic>               # the group's committed offsets, all partitions
//! ```
//!
//! Every consumer belongs to a group; a standalone consumer is the only member
//! of the group named after it.
//!
//! The live members share the partitions (see [`Consumer`]):
//! the member of rank r among N live members owns ⌊P / N⌋ of them, plus one if
//! r < P mod N.

mod config;
mod consumer;
mod describe;
mod membership;
mod offsets;
mod quota;

use std::fs;
use std::path::{Path, PathBuf};

pub use config::{ConsumerConfig, StartFrom};
pub use consumer::{Consumer, ConsumerRecord, RebalanceEvent};
pub use describe::CommittedOffset;
pub(crate) use describe::committed_offsets;

use crate::error::Result;
use crate::topic::validate_name;

/// The paths of a group.
#[derive(Debug, Clone)]
struct GroupPaths {
    dir: PathBuf,
}

impl GroupPaths {
    /// The directory of group `name`, created if needed.
    fn open(groups_dir: &Path, name: &str) -> Result<Self> {
        validate_name(name)?;
        let dir = groups_dir.join(name);
        fs::create_dir_all(dir.join("partitions"))?;
        fs::create_dir_all(dir.join("offsets"))?;
        Ok(Self { dir })
    }

    fn partition_lock(&self, topic: &str, partition: u32) -> PathBuf {
        self.dir
            .join("partitions")
            .join(format!("{topic}-{partition}.lock"))
    }

    fn committed_offsets(&self, topic: &str) -> PathBuf {
        self.dir.join("offsets").join(topic)
    }

    fn offsets_lock(&self) -> PathBuf {
        self.dir.join("offsets.lock")
    }
}
