//! Description of a topic for tooling (CLI, monitoring): configuration,
//! partitions and segments. Read-only.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{TopicConfig, TopicReader, load_config, partition_dir};
use crate::error::Result;
use crate::log::{list_segment_bases, segment_log_path};

/// A topic as it is on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDescription {
    /// The topic name.
    pub name: String,
    /// Its configuration, from `meta.toml`.
    pub config: TopicConfig,
    /// Each partition, in order.
    pub partitions: Vec<PartitionDescription>,
}

/// A partition: its readable offsets and its segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionDescription {
    /// The partition number.
    pub partition: u32,
    /// First available offset.
    pub start_offset: u64,
    /// First offset not yet readable: `start_offset..read_limit` can be read.
    pub read_limit: u64,
    /// From oldest to most recent; the last one is the active segment.
    pub segments: Vec<SegmentDescription>,
}

impl PartitionDescription {
    /// Total size of the segments on disk, indexes excluded.
    pub fn size_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size_bytes).sum()
    }
}

/// A segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescription {
    /// Offset of the first record of the segment.
    pub base_offset: u64,
    /// The `.log` file.
    pub path: PathBuf,
    /// Its size on disk, header included.
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
            // Deleted by retention between listing and reading.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(segments)
}
