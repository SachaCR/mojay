//! Un membre d'un consumer group : il possède des partitions, les lit, et
//! commite sa position dans chacune.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use super::{GroupPaths, offsets};
use crate::error::{Error, Result};
use crate::lock::FileLock;
use crate::record::{Header, Record};
use crate::topic::TopicReader;

/// Délai minimal entre deux tentatives de prise de partitions libres.
const CLAIM_INTERVAL: Duration = Duration::from_millis(200);
/// Attente entre deux vérifications quand il n'y a rien à lire : elle double
/// de `MIN_WAIT` à `MAX_WAIT` tant que rien n'arrive.
const MIN_WAIT: Duration = Duration::from_millis(1);
const MAX_WAIT: Duration = Duration::from_millis(50);
/// Surcoût estimé d'un enregistrement, en plus de sa clé, sa valeur et ses headers.
const RECORD_OVERHEAD: usize = 32;

/// Où commencer à lire une partition sans offset commité utilisable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    /// Au plus ancien message disponible.
    Earliest,
    /// Après le dernier message lisible : seuls les nouveaux messages seront lus.
    Latest,
}

/// Configuration d'un consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerConfig {
    /// Où commencer quand le groupe n'a rien commité pour une partition, ou
    /// quand l'offset commité n'existe plus (supprimé par la rétention).
    pub start_from: StartFrom,
    /// `Some(intervalle)` : les positions sont commitées automatiquement, au
    /// plus tôt à cet intervalle, au début de chaque `poll` et à la fermeture.
    /// Tout message rendu par un `poll` est alors considéré comme traité au
    /// `poll` suivant. `None` : commit manuel avec [`Consumer::commit`].
    pub auto_commit: Option<Duration>,
    /// Taille approximative maximale des messages rendus par un `poll`.
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

/// Un message lu par un consumer, avec sa provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

impl ConsumerRecord {
    fn new(topic: &str, partition: u32, record: Record) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
            offset: record.offset,
            timestamp: record.timestamp,
            key: record.key,
            value: record.value,
            headers: record.headers,
        }
    }

    fn approximate_size(&self) -> usize {
        let len = |b: &Option<Vec<u8>>| b.as_ref().map_or(0, Vec::len);
        let headers: usize = self
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.len())
            .sum();
        RECORD_OVERHEAD + len(&self.key) + len(&self.value) + headers
    }
}

/// Un topic auquel le consumer est abonné.
#[derive(Debug)]
struct Subscription {
    name: String,
    reader: TopicReader,
}

/// Une partition possédée par ce consumer : il en tient le verrou.
#[derive(Debug)]
struct OwnedPartition {
    /// Indice dans `Consumer::topics`.
    topic: usize,
    partition: u32,
    /// Prochain offset à rendre.
    position: u64,
    /// Dernier offset commité, tel qu'il est sur le disque.
    committed: Option<u64>,
    _lock: FileLock,
}

/// Un membre d'un consumer group.
///
/// Il possède les partitions dont il tient le verrou, et lui seul les lit pour
/// son groupe. Livraison *at-least-once* : après un crash, les messages rendus
/// depuis le dernier commit sont relus.
///
/// À l'étape 3, un membre prend toutes les partitions libres. Un second membre
/// du même groupe attend et prend le relais dès qu'elles se libèrent.
#[derive(Debug)]
pub struct Consumer {
    group: String,
    paths: GroupPaths,
    config: ConsumerConfig,
    topics: Vec<Subscription>,
    /// Triées par (topic, partition).
    owned: Vec<OwnedPartition>,
    /// Partition par laquelle commencer le prochain `poll`, pour servir
    /// toutes les partitions à tour de rôle.
    next_to_serve: usize,
    last_claim: Option<Instant>,
    last_commit: Instant,
}

impl Consumer {
    pub(crate) fn open(
        groups_dir: &Path,
        group: &str,
        readers: Vec<TopicReader>,
        config: ConsumerConfig,
    ) -> Result<Self> {
        if readers.is_empty() {
            return Err(Error::InvalidConfig(
                "un consumer doit s'abonner à au moins un topic".into(),
            ));
        }
        let mut names: Vec<&str> = readers.iter().map(TopicReader::topic).collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::InvalidConfig(
                "topic en double dans l'abonnement".into(),
            ));
        }

        let topics = readers
            .into_iter()
            .map(|reader| Subscription {
                name: reader.topic().to_string(),
                reader,
            })
            .collect();
        let mut consumer = Self {
            group: group.to_string(),
            paths: GroupPaths::open(groups_dir, group)?,
            config,
            topics,
            owned: Vec::new(),
            next_to_serve: 0,
            last_claim: None,
            last_commit: Instant::now(),
        };
        consumer.claim_free_partitions()?;
        Ok(consumer)
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    /// Les partitions possédées par ce consumer, triées.
    pub fn assignment(&self) -> Vec<(String, u32)> {
        self.owned
            .iter()
            .map(|o| (self.topics[o.topic].name.clone(), o.partition))
            .collect()
    }

    /// Le prochain offset que ce consumer rendra pour une partition qu'il possède.
    pub fn position(&self, topic: &str, partition: u32) -> Option<u64> {
        self.owned
            .iter()
            .find(|o| o.partition == partition && self.topics[o.topic].name == topic)
            .map(|o| o.position)
    }

    /// Attend au plus `timeout` des messages, et les rend dès qu'il y en a.
    ///
    /// Renvoie une liste vide si rien n'est arrivé à temps. Les partitions de
    /// chaque `poll` sont servies à tour de rôle, pour qu'aucune ne soit affamée.
    pub fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        self.auto_commit_if_due()?;
        let deadline = Instant::now() + timeout;
        let mut wait = MIN_WAIT;
        loop {
            if self.claim_is_due() {
                self.claim_free_partitions()?;
            }
            let records = self.fetch()?;
            if !records.is_empty() {
                return Ok(records);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(records);
            }
            thread::sleep(wait.min(left));
            wait = (wait * 2).min(MAX_WAIT);
        }
    }

    /// Commite la position de chaque partition possédée : tous les messages
    /// déjà rendus par `poll` sont considérés comme traités.
    ///
    /// Chaque offset est écrit de façon atomique et durable.
    pub fn commit(&mut self) -> Result<()> {
        for owned in &mut self.owned {
            if owned.committed != Some(owned.position) {
                let topic = &self.topics[owned.topic].name;
                offsets::write(
                    &self.paths.committed_offset(topic, owned.partition),
                    owned.position,
                )?;
                owned.committed = Some(owned.position);
            }
        }
        self.last_commit = Instant::now();
        Ok(())
    }

    fn auto_commit_if_due(&mut self) -> Result<()> {
        match self.config.auto_commit {
            Some(every) if self.last_commit.elapsed() >= every => self.commit(),
            _ => Ok(()),
        }
    }

    fn total_partitions(&self) -> usize {
        self.topics
            .iter()
            .map(|t| t.reader.partitions() as usize)
            .sum()
    }

    fn claim_is_due(&self) -> bool {
        self.owned.len() < self.total_partitions()
            && self
                .last_claim
                .is_none_or(|at| at.elapsed() >= CLAIM_INTERVAL)
    }

    /// Prend le verrou de chaque partition libre, puis reprend à son offset
    /// commité. L'offset est lu *après* la prise du verrou : le membre
    /// précédent a commité avant de relâcher, rien n'est donc relu en double.
    fn claim_free_partitions(&mut self) -> Result<()> {
        self.last_claim = Some(Instant::now());
        for topic in 0..self.topics.len() {
            for partition in 0..self.topics[topic].reader.partitions() {
                if self
                    .owned
                    .iter()
                    .any(|o| o.topic == topic && o.partition == partition)
                {
                    continue;
                }
                let name = &self.topics[topic].name;
                let Some(lock) =
                    FileLock::try_exclusive(&self.paths.partition_lock(name, partition))?
                else {
                    continue;
                };
                let committed = offsets::read(&self.paths.committed_offset(name, partition))?;
                let position = self.start_position(topic, partition, committed)?;
                self.owned.push(OwnedPartition {
                    topic,
                    partition,
                    position,
                    committed,
                    _lock: lock,
                });
            }
        }
        self.owned.sort_by_key(|o| (o.topic, o.partition));
        Ok(())
    }

    /// Où reprendre une partition, d'après l'offset commité s'il est utilisable.
    fn start_position(&self, topic: usize, partition: u32, committed: Option<u64>) -> Result<u64> {
        let reader = &self.topics[topic].reader;
        let start = reader.start_offset(partition)?;
        let limit = reader.read_limit(partition)?;
        Ok(match committed {
            // Supprimé par la rétention : on applique la politique de départ.
            Some(c) if c < start => self.policy_position(start, limit),
            // Au-delà de la limite (messages perdus avec une politique fsync
            // sans garantie) : on reprend à la limite, sans sauter de nouveau message.
            Some(c) => c.min(limit),
            None => self.policy_position(start, limit),
        })
    }

    fn policy_position(&self, start: u64, limit: u64) -> u64 {
        match self.config.start_from {
            StartFrom::Earliest => start,
            StartFrom::Latest => limit,
        }
    }

    /// Lit ce qui est disponible dans les partitions possédées, à tour de rôle,
    /// dans la limite de `max_poll_bytes`.
    fn fetch(&mut self) -> Result<Vec<ConsumerRecord>> {
        let count = self.owned.len();
        let mut out = Vec::new();
        let mut budget = self.config.max_poll_bytes;
        for k in 0..count {
            if !out.is_empty() && budget == 0 {
                break;
            }
            let i = (self.next_to_serve + k) % count;
            let (topic, partition) = (self.owned[i].topic, self.owned[i].partition);
            let records = match self.read_owned(i, budget) {
                Err(Error::OffsetOutOfRange { .. }) => {
                    // La rétention a supprimé la position : on applique la politique de départ.
                    self.owned[i].position = self.start_position(topic, partition, None)?;
                    continue;
                }
                other => other?,
            };
            for record in records {
                let record = ConsumerRecord::new(&self.topics[topic].name, partition, record);
                budget = budget.saturating_sub(record.approximate_size());
                self.owned[i].position = record.offset + 1;
                out.push(record);
            }
        }
        if count > 0 {
            self.next_to_serve = (self.next_to_serve + 1) % count;
        }
        Ok(out)
    }

    fn read_owned(&mut self, i: usize, budget: usize) -> Result<Vec<Record>> {
        let owned = &self.owned[i];
        let reader = &mut self.topics[owned.topic].reader;
        if owned.position >= reader.read_limit(owned.partition)? {
            return Ok(Vec::new());
        }
        reader.read(owned.partition, owned.position, budget)
    }
}

impl Drop for Consumer {
    /// Avec le commit automatique, commite avant de relâcher les partitions :
    /// le membre suivant reprend exactement là où celui-ci s'est arrêté.
    fn drop(&mut self) {
        if self.config.auto_commit.is_some() {
            // Une erreur ici ne peut plus être rendue : au pire, les messages
            // depuis le dernier commit seront relus (at-least-once).
            let _ = self.commit();
        }
    }
}
