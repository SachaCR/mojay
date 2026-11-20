//! Le côté writer d'une partition : seul le dernier segment (le segment actif)
//! reçoit des écritures.

use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::retention::{self, SealedSegment};
use super::{
    FsyncPolicy, LogConfig, list_segment_bases, read_across, remove_orphan_indexes,
    segment_log_path,
};
use crate::durable::sync_dir;
use crate::error::{Error, Result, corrupt};
use crate::record::{self, Message, Record};
use crate::segment::Segment;

/// Le log append-only d'une partition, stocké dans son propre dossier.
#[derive(Debug)]
pub struct PartitionLog {
    dir: PathBuf,
    config: LogConfig,
    /// Triés par offset de base ; le dernier est le segment actif. Jamais vide.
    segments: Vec<Segment>,
}

impl PartitionLog {
    /// Ouvre le log du dossier `dir`, en le créant au besoin.
    ///
    /// La fin du segment actif est réparée si une écriture a été interrompue.
    /// Renvoie [`Error::Corrupt`] si un segment scellé est illisible.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self> {
        config.validate()?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let bases = list_segment_bases(&dir)?;
        let segments = if bases.is_empty() {
            let first = Segment::create(&dir, 0, config.index_interval_bytes)?;
            first.sync()?;
            sync_dir(&dir)?;
            vec![first]
        } else {
            let last = bases.len() - 1;
            bases
                .iter()
                .enumerate()
                .map(|(i, &base)| Segment::open(&dir, base, config.index_interval_bytes, i == last))
                .collect::<Result<_>>()?
        };
        check_continuity(&dir, &segments)?;
        // D'après les segments ouverts, premier segment tout juste créé compris.
        let bases: Vec<u64> = segments.iter().map(Segment::base_offset).collect();
        remove_orphan_indexes(&dir, &bases)?;
        let mut log = Self {
            dir,
            config,
            segments,
        };
        log.enforce_retention()?;
        Ok(log)
    }

    /// Premier offset encore disponible.
    pub fn start_offset(&self) -> u64 {
        self.segments[0].base_offset()
    }

    /// Offset que recevra le prochain message ajouté.
    pub fn next_offset(&self) -> u64 {
        self.active().next_offset()
    }

    /// Ajoute un message et renvoie son offset.
    pub fn append(&mut self, message: &Message) -> Result<u64> {
        self.append_batch(std::slice::from_ref(message))
            .map(|range| range.start)
    }

    /// Ajoute des messages dans l'ordre et renvoie la plage d'offsets attribués.
    ///
    /// Tous les messages sont validés avant la première écriture : un message
    /// invalide n'en laisse aucun écrit. Avec [`FsyncPolicy::Always`], tout
    /// le lot est sur le disque au retour.
    pub fn append_batch(&mut self, messages: &[Message]) -> Result<Range<u64>> {
        let sizes = messages
            .iter()
            .map(record::validate)
            .collect::<Result<Vec<_>>>()?;
        let first_offset = self.next_offset();
        let timestamp = record::now_millis();

        let mut pending = PendingWrite::default();
        let mut rolled = false;
        for (message, size) in messages.iter().zip(sizes) {
            if self.needs_roll(&pending, size) {
                self.write_pending(&mut pending)?;
                self.roll()?;
                rolled = true;
            }
            let offset = self.active().next_offset() + pending.sizes.len() as u64;
            let ts = message.timestamp.unwrap_or(timestamp);
            record::encode(message, offset, ts, &mut pending.bytes);
            pending.sizes.push(size);
        }
        self.write_pending(&mut pending)?;

        if self.config.fsync == FsyncPolicy::Always {
            self.active().sync()?;
        }
        // Un nouveau segment scellé peut avoir vieilli ; la taille, elle, se
        // vérifie à chaque ajout, à partir des tailles connues en mémoire.
        if rolled || self.exceeds_max_bytes() {
            self.enforce_retention()?;
        }
        Ok(first_offset..self.next_offset())
    }

    /// Supprime les segments scellés les plus anciens qui dépassent la
    /// rétention configurée, et renvoie leur nombre.
    ///
    /// Appelé à l'ouverture et à chaque nouveau segment. La limite d'âge
    /// avance avec le temps, sans écriture : appeler cette méthode
    /// régulièrement l'applique aussi pendant les périodes calmes.
    pub fn enforce_retention(&mut self) -> Result<usize> {
        let retention = self.config.retention;
        if retention.is_unlimited() {
            return Ok(0);
        }
        let (active, sealed) = self
            .segments
            .split_last()
            .expect("un log a toujours un segment actif");
        let sealed = sealed
            .iter()
            .map(|s| {
                Ok(SealedSegment {
                    size: s.size(),
                    last_write: match retention.max_age {
                        Some(_) => Some(s.last_write()?),
                        None => None,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let count =
            retention::segments_to_delete(&sealed, active.size(), &retention, SystemTime::now());
        if count == 0 {
            return Ok(0);
        }
        // Les plus anciens d'abord : en cas d'échec, ce qui reste est toujours
        // une suite continue de segments.
        for segment in self.segments.drain(..count) {
            segment.delete()?;
        }
        sync_dir(&self.dir)?;
        Ok(count)
    }

    /// Lit les enregistrements à partir de `offset`, sans dépasser `max_bytes`
    /// (taille encodée). Renvoie toujours au moins un enregistrement s'il en
    /// existe un, même plus gros que `max_bytes`, pour ne jamais bloquer un lecteur.
    ///
    /// Lire à [`next_offset`](Self::next_offset) renvoie une liste vide.
    /// Renvoie [`Error::OffsetOutOfRange`] en dehors de `start_offset..=next_offset`.
    pub fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<Record>> {
        let (start, end) = (self.start_offset(), self.next_offset());
        if offset < start || offset > end {
            return Err(Error::OffsetOutOfRange { offset, start, end });
        }
        let bases: Vec<u64> = self.segments.iter().map(Segment::base_offset).collect();
        let mut out = Vec::new();
        let mut used = 0;
        read_across(
            &bases,
            offset,
            end,
            max_bytes,
            &mut used,
            &mut out,
            |i, req, out| self.segments[i].read(req, out),
        )?;
        Ok(out)
    }

    fn exceeds_max_bytes(&self) -> bool {
        self.config.retention.max_bytes.is_some_and(|max| {
            // Un seul segment : rien à supprimer, le segment actif reste.
            self.segments.len() > 1 && self.segments.iter().map(Segment::size).sum::<u64>() > max
        })
    }

    /// Force l'écriture sur le disque de tout ce qui a été ajouté.
    pub fn sync(&self) -> Result<()> {
        self.active().sync()
    }

    fn active(&self) -> &Segment {
        self.segments
            .last()
            .expect("un log a toujours un segment actif")
    }

    fn active_mut(&mut self) -> &mut Segment {
        self.segments
            .last_mut()
            .expect("un log a toujours un segment actif")
    }

    /// Faut-il ouvrir un nouveau segment avant d'ajouter un enregistrement de `size` octets ?
    fn needs_roll(&self, pending: &PendingWrite, size: usize) -> bool {
        let active = self.active();
        let has_records = !active.is_empty() || !pending.sizes.is_empty();
        let would_exceed =
            active.size() + (pending.bytes.len() + size) as u64 > self.config.segment_max_bytes;
        // Les offsets relatifs de l'index sont des u32.
        let count = active.next_offset() - active.base_offset() + pending.sizes.len() as u64;
        has_records && (would_exceed || count >= u64::from(u32::MAX))
    }

    fn write_pending(&mut self, pending: &mut PendingWrite) -> Result<()> {
        if !pending.sizes.is_empty() {
            self.active_mut().append(&pending.bytes, &pending.sizes)?;
            pending.bytes.clear();
            pending.sizes.clear();
        }
        Ok(())
    }

    /// Scelle le segment actif et en ouvre un nouveau.
    fn roll(&mut self) -> Result<()> {
        // Un segment scellé n'est plus jamais réparé : il doit être complet sur le disque.
        self.active().sync()?;
        let base = self.next_offset();
        let segment = Segment::create(&self.dir, base, self.config.index_interval_bytes)?;
        segment.sync()?;
        sync_dir(&self.dir)?;
        self.segments.push(segment);
        Ok(())
    }
}

/// Enregistrements encodés, pas encore écrits dans le segment actif.
#[derive(Default)]
struct PendingWrite {
    bytes: Vec<u8>,
    sizes: Vec<usize>,
}

/// Chaque segment doit s'arrêter exactement là où commence le suivant :
/// sinon des offsets manquent ou se chevauchent.
fn check_continuity(dir: &Path, segments: &[Segment]) -> Result<()> {
    for pair in segments.windows(2) {
        let (current, next) = (&pair[0], &pair[1]);
        if current.next_offset() != next.base_offset() {
            return Err(corrupt(
                segment_log_path(dir, current.base_offset()),
                format!(
                    "le segment se termine à l'offset {} mais le suivant commence à {}",
                    current.next_offset(),
                    next.base_offset()
                ),
            ));
        }
    }
    Ok(())
}
