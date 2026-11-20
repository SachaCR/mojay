//! Le writer d'un topic : un seul par topic, tous processus confondus.

use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::background_sync::{self, BackgroundSync, SharedLogs};
use super::{READ_LIMITS_FILE, WRITER_LOCK_FILE, load_config, partition_dir};
use crate::error::{Error, Result};
use crate::lock::FileLock;
use crate::log::{FsyncPolicy, PartitionLog};
use crate::partitioner::Partitioner;
use crate::read_limits::ReadLimitsWriter;
use crate::record::Message;

/// Le writer d'un topic. Il tient le verrou du topic tant qu'il vit : un second
/// producteur, dans ce processus ou un autre, reçoit [`Error::Busy`].
///
/// Chaque message est rendu lisible dès son ajout, après le `fsync` si la
/// politique est [`FsyncPolicy::Always`].
#[derive(Debug)]
pub struct Producer {
    topic: String,
    partitioner: Partitioner,
    logs: SharedLogs,
    read_limits: ReadLimitsWriter,
    background_sync: Option<BackgroundSync>,
    /// Déclaré en dernier : le verrou n'est relâché qu'une fois tout le reste fermé.
    _lock: FileLock,
}

impl Producer {
    pub(crate) fn open(topic_dir: &Path, topic: &str) -> Result<Self> {
        let lock = FileLock::try_exclusive(&topic_dir.join(WRITER_LOCK_FILE))?
            .ok_or_else(|| Error::Busy(topic.to_string()))?;
        let config = load_config(topic_dir)?;

        // Ouvrir un log répare la fin d'une écriture interrompue par un crash.
        let logs = (0..config.partitions)
            .map(|p| PartitionLog::open(partition_dir(topic_dir, p), config.log.clone()))
            .collect::<Result<Vec<_>>>()?;
        let read_limits =
            ReadLimitsWriter::open(&topic_dir.join(READ_LIMITS_FILE), config.partitions)?;
        // La réparation a pu retirer des messages : la limite suit le log réel.
        for (partition, log) in (0..).zip(&logs) {
            read_limits.publish(partition, log.next_offset())?;
        }

        let logs = Arc::new(Mutex::new(logs));
        let background_sync = match config.log.fsync {
            FsyncPolicy::Interval(every) => Some(BackgroundSync::start(Arc::clone(&logs), every)?),
            FsyncPolicy::Always | FsyncPolicy::Never => None,
        };
        Ok(Self {
            topic: topic.to_string(),
            partitioner: Partitioner::new(config.partitions),
            logs,
            read_limits,
            background_sync,
            _lock: lock,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partitions(&self) -> u32 {
        background_sync::lock(&self.logs).len() as u32
    }

    /// Ajoute un message et renvoie sa partition et son offset.
    ///
    /// La partition dépend de la clé (murmur2, comme Kafka) : deux messages de
    /// même clé vont dans la même partition, dans l'ordre. Sans clé, les
    /// messages sont répartis en round-robin.
    pub fn send(&mut self, message: &Message) -> Result<(u32, u64)> {
        let partition = self.partitioner.partition_for(message.key.as_deref());
        let offset = self.send_to(partition, message)?;
        Ok((partition, offset))
    }

    /// Ajoute un message dans une partition donnée et renvoie son offset.
    pub fn send_to(&mut self, partition: u32, message: &Message) -> Result<u64> {
        self.send_batch_to(partition, std::slice::from_ref(message))
            .map(|range| range.start)
    }

    /// Ajoute des messages, dans l'ordre, dans une partition donnée, et renvoie
    /// la plage d'offsets attribués. Plus rapide que des envois un par un : un
    /// seul `write` et un seul `fsync` pour le lot.
    pub fn send_batch_to(&mut self, partition: u32, messages: &[Message]) -> Result<Range<u64>> {
        self.report_background_failure()?;
        let mut logs = background_sync::lock(&self.logs);
        let partitions = logs.len() as u32;
        let log = logs
            .get_mut(partition as usize)
            .ok_or(Error::UnknownPartition {
                partition,
                partitions,
            })?;
        let range = log.append_batch(messages)?;
        // Publié après l'écriture (et le fsync avec `Always`) : un lecteur ne
        // voit jamais un message qui n'est pas entièrement écrit.
        self.read_limits.publish(partition, log.next_offset())?;
        Ok(range)
    }

    /// Force l'écriture sur le disque de tous les messages ajoutés, quelle que
    /// soit la politique de `fsync`.
    pub fn flush(&mut self) -> Result<()> {
        self.report_background_failure()?;
        background_sync::sync_all(&self.logs)
    }

    /// Une erreur du `fsync` en arrière-plan est rendue par l'appel suivant.
    fn report_background_failure(&self) -> Result<()> {
        match self
            .background_sync
            .as_ref()
            .and_then(BackgroundSync::take_failure)
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        // L'arrêt du thread fait un dernier fsync. Une erreur à ce stade ne peut
        // plus être rendue à personne : les données restent dans le cache de
        // l'OS et survivent à la fin du processus.
        if let Some(sync) = self.background_sync.take() {
            let _ = sync.stop();
        }
    }
}
