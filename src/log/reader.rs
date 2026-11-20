//! Le côté lecteur d'une partition : lecture seule, sans verrou, bornée par la
//! limite de lecture publiée par le writer.

use std::path::{Path, PathBuf};

use super::{ReadStop, list_segment_bases, read_across};
use crate::error::{Error, Result};
use crate::record::Record;
use crate::segment::SegmentReader;

#[derive(Debug)]
pub(crate) struct PartitionReader {
    dir: PathBuf,
    /// Triés par offset de base. Vide tant que le writer n'a jamais ouvert la partition.
    segments: Vec<SegmentReader>,
}

impl PartitionReader {
    pub(crate) fn open(dir: &Path) -> Result<Self> {
        let segments = list_segment_bases(dir)?
            .into_iter()
            .map(|base| SegmentReader::open(dir, base))
            .collect::<Result<_>>()?;
        Ok(Self {
            dir: dir.to_path_buf(),
            segments,
        })
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
            if stop == ReadStop::BudgetReached || !self.discover_new_segments()? {
                break;
            }
        }
        Ok(out)
    }

    /// Ouvre les segments créés par le writer depuis la dernière fois.
    /// Renvoie `true` s'il y en avait.
    fn discover_new_segments(&mut self) -> Result<bool> {
        let last_known = self.segments.last().map(SegmentReader::base_offset);
        let mut found = false;
        for base in list_segment_bases(&self.dir)? {
            if last_known.is_none_or(|last| base > last) {
                self.segments.push(SegmentReader::open(&self.dir, base)?);
                found = true;
            }
        }
        Ok(found)
    }
}
