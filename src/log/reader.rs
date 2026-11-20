//! Le côté lecteur d'une partition : lecture seule, sans verrou, bornée par la
//! limite de lecture publiée par le writer.

use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::{ReadStop, list_segment_bases, read_across};
use crate::error::{Error, Result};
use crate::record::Record;
use crate::segment::SegmentReader;

/// Délai maximal avant de remarquer qu'un segment a été supprimé par la
/// rétention. D'ici là, il reste lisible (le fichier ouvert survit à sa
/// suppression) mais occupe encore de la place sur le disque.
const DISK_CHECK_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub(crate) struct PartitionReader {
    dir: PathBuf,
    /// Triés par offset de base. Vide tant que le writer n'a jamais ouvert la partition.
    segments: Vec<SegmentReader>,
    last_disk_check: Instant,
}

impl PartitionReader {
    pub(crate) fn open(dir: &Path) -> Result<Self> {
        let mut reader = Self {
            dir: dir.to_path_buf(),
            segments: Vec::new(),
            last_disk_check: Instant::now(),
        };
        reader.sync_with_disk()?;
        Ok(reader)
    }

    /// Premier offset disponible.
    pub(crate) fn start_offset(&self) -> u64 {
        self.segments.first().map_or(0, SegmentReader::base_offset)
    }

    /// Lit les enregistrements à partir de `offset`, sans dépasser `max_bytes`
    /// ni `read_limit`. Renvoie toujours au moins un enregistrement s'il en
    /// existe un avant la limite.
    pub(crate) fn read(
        &mut self,
        offset: u64,
        max_bytes: usize,
        read_limit: u64,
    ) -> Result<Vec<Record>> {
        if self.last_disk_check.elapsed() >= DISK_CHECK_INTERVAL {
            self.sync_with_disk()?;
        }
        let start = self.start_offset();
        if offset < start || offset > read_limit {
            return Err(Error::OffsetOutOfRange {
                offset,
                start,
                end: read_limit,
            });
        }

        let mut out: Vec<Record> = Vec::new();
        let mut used = 0;
        loop {
            let from = out.last().map_or(offset, |r| r.offset + 1);
            if from >= read_limit {
                break;
            }
            let bases: Vec<u64> = self
                .segments
                .iter()
                .map(SegmentReader::base_offset)
                .collect();
            let segments = &mut self.segments;
            let stop = read_across(
                &bases,
                from,
                read_limit,
                max_bytes,
                &mut used,
                &mut out,
                |i, req, out| segments[i].read(req, out),
            )?;
            // La limite n'est pas atteinte : le writer a pu ouvrir de nouveaux segments.
            if stop == ReadStop::BudgetReached || !self.sync_with_disk()? {
                break;
            }
        }
        Ok(out)
    }

    /// Met la liste des segments en accord avec le disque : oublie ceux que
    /// la rétention a supprimés (leur fichier est alors vraiment libéré) et
    /// ouvre ceux que le writer a créés. Renvoie `true` s'il y en avait de nouveaux.
    fn sync_with_disk(&mut self) -> Result<bool> {
        self.last_disk_check = Instant::now();
        let bases = list_segment_bases(&self.dir)?;
        // La rétention ne supprime que les plus anciens : tout ce qui précède
        // le premier segment présent a disparu.
        if let Some(&first) = bases.first() {
            self.segments.retain(|s| s.base_offset() >= first);
        }

        let last_known = self.segments.last().map(SegmentReader::base_offset);
        let mut found = false;
        for base in bases {
            if last_known.is_some_and(|last| base <= last) {
                continue;
            }
            match SegmentReader::open(&self.dir, base) {
                Ok(segment) => {
                    self.segments.push(segment);
                    found = true;
                }
                // Supprimé entre la liste du dossier et son ouverture.
                Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(found)
    }
}
