//! Lecture d'un topic, sans verrou, depuis n'importe quel processus.

use std::path::Path;

use super::{READ_LIMITS_FILE, load_config, partition_dir};
use crate::error::{Error, Result};
use crate::log::PartitionReader;
use crate::read_limits::ReadLimits;
use crate::record::Record;

/// Un lecteur d'un topic. Il ne prend aucun verrou et ne modifie rien :
/// on peut en ouvrir autant qu'on veut, pendant qu'un [`Producer`](crate::Producer)
/// écrit dans un autre processus.
///
/// Il ne lit jamais au-delà de la limite de lecture publiée par le writer :
/// tout ce qu'il renvoie est complet.
#[derive(Debug)]
pub struct TopicReader {
    topic: String,
    partitions: Vec<PartitionReader>,
    read_limits: ReadLimits,
}

impl TopicReader {
    pub(crate) fn open(topic_dir: &Path, topic: &str) -> Result<Self> {
        let config = load_config(topic_dir)?;
        let partitions = (0..config.partitions)
            .map(|p| PartitionReader::open(&partition_dir(topic_dir, p)))
            .collect::<Result<_>>()?;
        let read_limits = ReadLimits::open(&topic_dir.join(READ_LIMITS_FILE), config.partitions)?;
        Ok(Self {
            topic: topic.to_string(),
            partitions,
            read_limits,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partitions(&self) -> u32 {
        self.partitions.len() as u32
    }

    /// Premier offset disponible dans `partition`.
    pub fn start_offset(&self, partition: u32) -> Result<u64> {
        Ok(self.partition(partition)?.start_offset())
    }

    /// Premier offset pas encore lisible dans `partition` : on peut lire
    /// `start_offset..read_limit`.
    pub fn read_limit(&self, partition: u32) -> Result<u64> {
        self.read_limits.get(partition)
    }

    /// Lit les messages de `partition` à partir de `offset`, sans dépasser
    /// `max_bytes` (taille encodée) ni la limite de lecture. Renvoie toujours
    /// au moins un message s'il y en a un de lisible, même plus gros que `max_bytes`.
    ///
    /// Lire à la limite de lecture renvoie une liste vide : rien de nouveau pour l'instant.
    pub fn read(&mut self, partition: u32, offset: u64, max_bytes: usize) -> Result<Vec<Record>> {
        let read_limit = self.read_limits.get(partition)?;
        let partitions = self.partitions();
        self.partitions
            .get_mut(partition as usize)
            .ok_or(Error::UnknownPartition {
                partition,
                partitions,
            })?
            .read(offset, max_bytes, read_limit)
    }

    fn partition(&self, partition: u32) -> Result<&PartitionReader> {
        self.partitions
            .get(partition as usize)
            .ok_or(Error::UnknownPartition {
                partition,
                partitions: self.partitions(),
            })
    }
}
