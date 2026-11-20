//! Consumer groups, sans serveur : le système de fichiers sert de coordinateur.
//!
//! ```text
//! groups/<group>/
//!   partitions/<topic>-<p>.lock   # flock tenu = partition possédée par un membre
//!   offsets/<topic>-<p>           # offset commité du groupe pour cette partition
//! ```
//!
//! Tout consumer appartient à un groupe ; un consumer seul est l'unique membre
//! du groupe qui porte son nom.
//!
//! Étape 3 de la feuille de route : un membre prend toutes les partitions
//! libres. Un second membre du même groupe attend, et reprend les partitions
//! dès qu'elles se libèrent (arrêt ou crash du premier). Le partage équitable
//! entre membres vivants arrive à l'étape 4.

mod consumer;
mod offsets;

use std::fs;
use std::path::{Path, PathBuf};

pub use consumer::{Consumer, ConsumerConfig, ConsumerRecord, StartFrom};

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
