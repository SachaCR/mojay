//! Un membre d'un consumer group : il possède des partitions, les lit, et
//! commite sa position dans chacune. Il se répartit les partitions avec les
//! autres membres vivants, sans coordinateur.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use super::config::{ConsumerConfig, StartFrom};
use super::membership::Membership;
use super::{GroupPaths, offsets, quota};
use crate::error::{Error, Result};
use crate::lock::FileLock;
use crate::record::{Header, Record};
use crate::topic::TopicReader;

/// Délai entre deux répartitions : c'est aussi le délai de réaction à
/// l'arrivée ou au départ d'un membre.
const REBALANCE_INTERVAL: Duration = Duration::from_millis(200);
/// Attente entre deux vérifications quand il n'y a rien à lire : elle double
/// de `MIN_WAIT` à `MAX_WAIT` tant que rien n'arrive.
const MIN_WAIT: Duration = Duration::from_millis(1);
const MAX_WAIT: Duration = Duration::from_millis(50);
/// Surcoût estimé d'un enregistrement, en plus de sa clé, sa valeur et ses headers.
const RECORD_OVERHEAD: usize = 32;

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

/// Un changement dans les partitions possédées par un consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceEvent {
    /// Le consumer possède désormais cette partition.
    Assigned { topic: String, partition: u32 },
    /// Le consumer a cédé cette partition à un autre membre du groupe.
    Revoked { topic: String, partition: u32 },
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
/// Les membres vivants d'un groupe se répartissent ses partitions : chacun
/// possède celles dont il tient le verrou, et lui seul les lit pour le groupe.
/// À chaque `poll` (au plus toutes les 200 ms), un membre compte les vivants,
/// cède ce qu'il a en trop et prend des partitions libres s'il lui en manque.
///
/// Livraison *at-least-once* : après un crash, les messages rendus depuis le
/// dernier commit sont relus. Un membre qui n'appelle plus `poll` garde ses
/// partitions tant qu'il vit.
///
/// Les membres d'un même groupe doivent s'abonner aux mêmes topics : sinon la
/// répartition reste correcte (jamais deux lecteurs par partition), mais n'est
/// plus équilibrée.
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
    events: Vec<RebalanceEvent>,
    last_rebalance: Option<Instant>,
    last_commit: Instant,
    /// Déclaré après `owned` : les partitions sont relâchées avant le départ du groupe.
    membership: Membership,
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
        let mut topics: Vec<Subscription> = readers
            .into_iter()
            .map(|reader| Subscription {
                name: reader.topic().to_string(),
                reader,
            })
            .collect();
        // Le même ordre chez tous les membres : il fixe l'ordre des partitions.
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        if topics.windows(2).any(|w| w[0].name == w[1].name) {
            return Err(Error::InvalidConfig(
                "topic en double dans l'abonnement".into(),
            ));
        }

        let paths = GroupPaths::open(groups_dir, group)?;
        let membership = Membership::join(&paths.dir)?;
        let mut consumer = Self {
            group: group.to_string(),
            paths,
            config,
            topics,
            owned: Vec::new(),
            next_to_serve: 0,
            events: Vec::new(),
            last_rebalance: None,
            last_commit: Instant::now(),
            membership,
        };
        consumer.rebalance()?;
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

    /// Les partitions reçues ou cédées depuis le dernier appel, dans l'ordre.
    ///
    /// En commit manuel, une partition cédée reprendra chez son nouveau
    /// propriétaire au dernier commit : commiter avant chaque `poll` évite ces relectures.
    pub fn take_rebalance_events(&mut self) -> Vec<RebalanceEvent> {
        std::mem::take(&mut self.events)
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
            if self.rebalance_is_due() {
                self.rebalance()?;
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
    /// Chaque offset est écrit de façon atomique et durable. Seules les
    /// partitions encore possédées sont commitées : une partition cédée
    /// appartient à son nouveau propriétaire.
    pub fn commit(&mut self) -> Result<()> {
        for i in 0..self.owned.len() {
            self.commit_owned(i)?;
        }
        self.last_commit = Instant::now();
        Ok(())
    }

    fn commit_owned(&mut self, i: usize) -> Result<()> {
        let owned = &mut self.owned[i];
        if owned.committed != Some(owned.position) {
            let path = self
                .paths
                .committed_offset(&self.topics[owned.topic].name, owned.partition);
            offsets::write(&path, owned.position)?;
            owned.committed = Some(owned.position);
        }
        Ok(())
    }

    fn auto_commit_if_due(&mut self) -> Result<()> {
        match self.config.auto_commit {
            Some(every) if self.last_commit.elapsed() >= every => self.commit(),
            _ => Ok(()),
        }
    }

    fn rebalance_is_due(&self) -> bool {
        self.last_rebalance
            .is_none_or(|at| at.elapsed() >= REBALANCE_INTERVAL)
    }

    /// Ajuste les partitions possédées à la part de ce membre : cède le
    /// surplus, puis prend des partitions libres s'il en manque.
    fn rebalance(&mut self) -> Result<()> {
        self.last_rebalance = Some(Instant::now());
        let members = self.membership.living_members()?;
        let rank = members
            .iter()
            .position(|m| m == self.membership.id())
            .unwrap_or(members.len());
        let all = self.all_partitions();
        let quota = quota::quota(all.len(), members.len(), rank);

        while self.owned.len() > quota {
            self.release_last()?;
        }
        if self.owned.len() < quota {
            let start = quota::claim_start(all.len(), members.len(), rank);
            for k in 0..all.len() {
                if self.owned.len() >= quota {
                    break;
                }
                let (topic, partition) = all[(start + k) % all.len()];
                self.try_claim(topic, partition)?;
            }
            self.owned.sort_by_key(|o| (o.topic, o.partition));
        }
        Ok(())
    }

    /// Toutes les partitions des topics suivis, dans le même ordre chez tous
    /// les membres (topics triés par nom).
    fn all_partitions(&self) -> Vec<(usize, u32)> {
        self.topics
            .iter()
            .enumerate()
            .flat_map(|(t, sub)| (0..sub.reader.partitions()).map(move |p| (t, p)))
            .collect()
    }

    /// Cède la dernière partition possédée. Avec le commit automatique, sa
    /// position est commitée *avant* de relâcher le verrou : le membre suivant
    /// reprend exactement là, sans doublon.
    fn release_last(&mut self) -> Result<()> {
        let last = self.owned.len() - 1;
        if self.config.auto_commit.is_some() {
            self.commit_owned(last)?;
        }
        if let Some(owned) = self.owned.pop() {
            self.events.push(RebalanceEvent::Revoked {
                topic: self.topics[owned.topic].name.clone(),
                partition: owned.partition,
            });
            // Le verrou est relâché ici, en détruisant `owned`.
        }
        Ok(())
    }

    /// Prend la partition si elle est libre, et reprend à son offset commité.
    /// L'offset est lu *après* la prise du verrou : le membre précédent a
    /// fini d'écrire le sien avant de relâcher.
    fn try_claim(&mut self, topic: usize, partition: u32) -> Result<()> {
        if self
            .owned
            .iter()
            .any(|o| o.topic == topic && o.partition == partition)
        {
            return Ok(());
        }
        let name = &self.topics[topic].name;
        let Some(lock) = FileLock::try_exclusive(&self.paths.partition_lock(name, partition))?
        else {
            return Ok(());
        };
        let committed = offsets::read(&self.paths.committed_offset(name, partition))?;
        let position = self.start_position(topic, partition, committed)?;
        self.events.push(RebalanceEvent::Assigned {
            topic: name.clone(),
            partition,
        });
        self.owned.push(OwnedPartition {
            topic,
            partition,
            position,
            committed,
            _lock: lock,
        });
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
    /// Les partitions sont ensuite relâchées, puis le membre quitte le groupe.
    fn drop(&mut self) {
        if self.config.auto_commit.is_some() {
            // Une erreur ici ne peut plus être rendue : au pire, les messages
            // depuis le dernier commit seront relus (at-least-once).
            let _ = self.commit();
        }
    }
}
