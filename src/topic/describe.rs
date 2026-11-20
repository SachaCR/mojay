//! Description d'un topic pour l'outillage (CLI, supervision) : configuration,
//! partitions et segments. Lecture seule.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{TopicConfig, TopicReader, load_config, partition_dir};
use crate::error::Result;
use crate::log::{list_segment_bases, segment_log_path};

/// Un topic tel qu'il est sur le disque.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDescription {
    pub name: String,
    pub config: TopicConfig,
    pub partitions: Vec<PartitionDescription>,
}

/// Une partition : ses offsets lisibles et ses segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionDescription {
    pub partition: u32,
    /// Premier offset disponible.
    pub start_offset: u64,
    /// Premier offset pas encore lisible : on peut lire `start_offset..read_limit`.
    pub read_limit: u64,
    /// Du plus ancien au plus récent ; le dernier est le segment actif.
    pub segments: Vec<SegmentDescription>,
}

impl PartitionDescription {
    /// Taille totale des segments sur le disque, index non compris.
    pub fn size_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size_bytes).sum()
    }
}

/// Un fichier de segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescription {
    pub base_offset: u64,
    pub path: PathBuf,
    pub size_bytes: u64,
}

pub(crate) fn describe(topic_dir: &Path, name: &str) -> Result<TopicDescription> {
    let config = load_config(topic_dir)?;
    let reader = TopicReader::open(topic_dir, name)?;
    let partitions = (0..config.partitions)
        .map(|p| {
            Ok(PartitionDescription {
                partition: p,
                start_offset: reader.start_offset(p)?,
                read_limit: reader.read_limit(p)?,
                segments: segments(&partition_dir(topic_dir, p))?,
            })
        })
        .collect::<Result<_>>()?;
    Ok(TopicDescription {
        name: name.to_string(),
        config,
        partitions,
    })
}

fn segments(dir: &Path) -> Result<Vec<SegmentDescription>> {
    let mut segments = Vec::new();
    for base_offset in list_segment_bases(dir)? {
        let path = segment_log_path(dir, base_offset);
        match fs::metadata(&path) {
            Ok(meta) => segments.push(SegmentDescription {
                base_offset,
                path,
                size_bytes: meta.len(),
            }),
            // Supprimé par la rétention entre la liste et la lecture.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(segments)
}
