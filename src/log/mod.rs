//! Le log d'une partition : une suite ordonnée de segments, dans son propre dossier.
//!
//! - [`PartitionLog`] : le côté writer, seul à ajouter et à réparer.
//! - [`PartitionReader`] : le côté lecteur, en lecture seule, borné par la
//!   limite de lecture que publie le writer.
//!
//! Ce module ne prend aucun verrou inter-processus : c'est la couche topic qui
//! garantit qu'un seul writer existe par partition.

mod reader;
mod retention;
mod writer;

use std::fs;
use std::path::Path;
use std::time::Duration;

pub(crate) use reader::PartitionReader;
pub use retention::Retention;
pub use writer::PartitionLog;

use crate::error::{Error, Result, corrupt};
use crate::record::{MAX_RECORD_SIZE, Record};
use crate::segment::{self, HEADER_SIZE, ReadRequest, SegmentRead};

/// Quand forcer l'écriture des données sur le disque.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// `fsync` après chaque ajout, avant de rendre les messages lisibles.
    /// Survit à une coupure de courant. C'est le défaut.
    Always,
    /// `fsync` périodique, par un thread du producteur. Un [`PartitionLog`]
    /// utilisé seul n'en fait rien : l'appelant appelle [`PartitionLog::sync`].
    Interval(Duration),
    /// Laissé à l'OS. Survit à un crash du processus, pas à une coupure de courant.
    Never,
}

/// Configuration d'un log de partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    /// Taille au-delà de laquelle on passe au segment suivant.
    pub segment_max_bytes: u64,
    /// Nombre d'octets écrits entre deux entrées d'index.
    pub index_interval_bytes: u64,
    pub fsync: FsyncPolicy,
    /// Limites de taille et d'âge de la partition. Par défaut, aucune.
    pub retention: Retention,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: 64 * 1024 * 1024,
            index_interval_bytes: 4096,
            fsync: FsyncPolicy::Always,
            retention: Retention::default(),
        }
    }
}

/// Les positions d'index sont des `u32` : un segment ne peut pas dépasser 4 Gio,
/// marge comprise pour un dernier enregistrement de taille maximale.
const MAX_SEGMENT_BYTES: u64 = u32::MAX as u64 - MAX_RECORD_SIZE as u64;

impl LogConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.segment_max_bytes <= HEADER_SIZE || self.segment_max_bytes > MAX_SEGMENT_BYTES {
            return Err(Error::InvalidConfig(format!(
                "segment_max_bytes doit être compris entre {} et {MAX_SEGMENT_BYTES}",
                HEADER_SIZE + 1
            )));
        }
        if self.fsync == FsyncPolicy::Interval(Duration::ZERO) {
            return Err(Error::InvalidConfig(
                "l'intervalle de fsync doit être positif".into(),
            ));
        }
        Ok(())
    }
}

/// Offsets de base des segments présents dans `dir`, triés.
pub(crate) fn list_segment_bases(dir: &Path) -> Result<Vec<u64>> {
    let mut bases = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let Some(stem) = name.to_str().and_then(|n| n.strip_suffix(".log")) else {
            continue;
        };
        if stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit()) {
            let base = stem
                .parse()
                .map_err(|_| corrupt(dir.join(&name), "nom de segment invalide"))?;
            bases.push(base);
        }
    }
    bases.sort_unstable();
    Ok(bases)
}

/// Supprime les index dont le `.log` n'existe plus : restes d'une suppression
/// de segment interrompue par un crash.
pub(crate) fn remove_orphan_indexes(dir: &Path, bases: &[u64]) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let base = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix(".idx"))
            .and_then(|stem| stem.parse::<u64>().ok());
        if base.is_some_and(|b| !bases.contains(&b)) {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Pourquoi une lecture à travers les segments s'est arrêtée.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadStop {
    /// Le budget d'octets est atteint.
    BudgetReached,
    /// Tous les segments connus ont été lus jusqu'à `end`.
    SegmentsExhausted,
}

/// Lit à travers les segments, dans l'ordre, à partir de `from` et jusqu'à
/// `end` (exclu). `bases` donne l'offset de base de chaque segment, trié ;
/// `read_segment(i, …)` lit le segment `i`. `used` cumule les octets pris.
pub(crate) fn read_across(
    bases: &[u64],
    from: u64,
    end: u64,
    max_bytes: usize,
    used: &mut usize,
    out: &mut Vec<Record>,
    mut read_segment: impl FnMut(usize, &ReadRequest, &mut Vec<Record>) -> Result<SegmentRead>,
) -> Result<ReadStop> {
    let first = bases.partition_point(|&b| b <= from).saturating_sub(1);
    for i in first..bases.len() {
        let req = ReadRequest {
            from,
            end: bases.get(i + 1).map_or(end, |&next| next.min(end)),
            budget: max_bytes.saturating_sub(*used),
            take_one: out.is_empty(),
        };
        match read_segment(i, &req, out)? {
            SegmentRead::Exhausted { taken } => *used += taken,
            SegmentRead::BudgetReached => return Ok(ReadStop::BudgetReached),
        }
    }
    Ok(ReadStop::SegmentsExhausted)
}

pub(crate) fn segment_log_path(dir: &Path, base_offset: u64) -> std::path::PathBuf {
    segment::log_path(dir, base_offset)
}
