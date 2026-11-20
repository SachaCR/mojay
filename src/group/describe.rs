//! Offsets committed by groups, for tooling. Read-only.

use std::fs;
use std::io;
use std::path::Path;

use super::{GroupPaths, offsets};
use crate::error::Result;

/// The offset committed by a group for a partition of a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub group: String,
    pub partition: u32,
    pub offset: u64,
}

/// The offsets committed for `topic` by all groups, sorted by group
/// then by partition. A group that has committed nothing does not appear.
pub(crate) fn committed_offsets(
    groups_dir: &Path,
    topic: &str,
    partitions: u32,
) -> Result<Vec<CommittedOffset>> {
    let entries = match fs::read_dir(groups_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut groups: Vec<String> = Vec::new();
    for entry in entries {
        let entry = entry?;
        if let Ok(name) = entry.file_name().into_string()
            && !name.starts_with('.')
            && entry.file_type()?.is_dir()
        {
            groups.push(name);
        }
    }
    groups.sort();

    let mut out = Vec::new();
    for group in groups {
        let paths = GroupPaths {
            dir: groups_dir.join(&group),
        };
        for partition in 0..partitions {
            if let Some(offset) = offsets::read(&paths.committed_offset(topic, partition))? {
                out.push(CommittedOffset {
                    group: group.clone(),
                    partition,
                    offset,
                });
            }
        }
    }
    Ok(out)
}
