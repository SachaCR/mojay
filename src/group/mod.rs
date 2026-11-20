//! Consumer groups, sans serveur : le système de fichiers sert de coordinateur.
//!
//! ```text
//! groups/<group>/
//!   members/<id>.lock             # flock tenu = membre vivant
//!   partitions/<topic>-<p>.lock   # flock tenu = partition possédée par un membre
//!   offsets/<topic>-<p>           # offset commité du groupe pour cette partition
//! ```
//!
//! Tout consumer appartient à un groupe ; un consumer seul est l'unique membre
//! du groupe qui porte son nom.
//!
//! Les membres vivants se répartissent les partitions (voir [`Consumer`]) :
//! le membre de rang r parmi N vivants en possède ⌊P / N⌋, plus une si
//! r < P mod N.

mod config;
mod consumer;
mod membership;
mod offsets;
mod quota;

use std::fs;
use std::path::{Path, PathBuf};

pub use config::{ConsumerConfig, StartFrom};
pub use consumer::{Consumer, ConsumerRecord, RebalanceEvent};

use crate::error::Result;
use crate::topic::validate_name;

/// Les chemins d'un groupe.
#[derive(Debug, Clone)]
struct GroupPaths {
    dir: PathBuf,
}

impl GroupPaths {
    /// Le dossier du groupe `name`, créé au besoin.
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

    fn committed_offset(&self, topic: &str, partition: u32) -> PathBuf {
        self.dir
            .join("offsets")
            .join(format!("{topic}-{partition}"))
    }
}
