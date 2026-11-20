//! Segment inspection, for debugging: shows what is actually
//! stored, byte by byte. Read-only: nothing is ever repaired.
//!
//! Unlike normal reading, inspection does not stop with an
//! error on damaged data: it reports what it could read, then
//! where and why the rest is unreadable.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::index::{ENTRY_SIZE, SparseIndex};
use crate::record::{self, ReadOutcome, Record};
use crate::segment::{HEADER_SIZE, parse_header};

/// The contents of a `.log` segment file and its index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDump {
    pub path: PathBuf,
    pub size_bytes: u64,
    /// The base offset, read from the file name if it has the expected format.
    pub base_offset: Option<u64>,
    /// The format version, or the reason why the header is invalid.
    pub header: std::result::Result<u16, String>,
    /// The readable records, in order.
    pub records: Vec<DumpedRecord>,
    /// The unreadable end of the file, if there is one.
    pub unreadable_tail: Option<UnreadableTail>,
    /// The entries of the `.idx` index, if it exists.
    pub index: Option<IndexDump>,
}

/// A record and its location in the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpedRecord {
    pub position: u64,
    /// Encoded size, including the length prefix.
    pub size: usize,
    pub record: Record,
}

/// Bytes that cannot be read as a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableTail {
    pub position: u64,
    pub bytes: u64,
    pub reason: String,
}

/// The raw contents of an index file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDump {
    pub entries: Vec<DumpedIndexEntry>,
    /// Extra bytes at the end (truncated entry).
    pub leftover_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpedIndexEntry {
    pub relative_offset: u32,
    pub position: u32,
    /// `false` if the entry is inconsistent (outside the log, or not increasing):
    /// normal reading ignores it, along with all the following ones.
    pub valid: bool,
}

/// Reads a `.log` file and its index, without modifying anything.
pub fn dump_segment(path: impl AsRef<Path>) -> Result<SegmentDump> {
    let path = path.as_ref();
    let bytes = fs::read(path)?;
    let size_bytes = bytes.len() as u64;
    let header = parse_header(&bytes);

    let mut records = Vec::new();
    let mut unreadable_tail = None;
    if header.is_ok() {
        let mut position = HEADER_SIZE;
        let mut rest = &bytes[HEADER_SIZE as usize..];
        loop {
            let reason = match record::read_record(&mut rest)? {
                ReadOutcome::Record(record, size) => {
                    records.push(DumpedRecord {
                        position,
                        size,
                        record,
                    });
                    position += size as u64;
                    continue;
                }
                ReadOutcome::End => break,
                ReadOutcome::Incomplete => "incomplete record".to_string(),
                ReadOutcome::Corrupt(reason) => reason,
            };
            unreadable_tail = Some(UnreadableTail {
                position,
                bytes: size_bytes - position,
                reason,
            });
            break;
        }
    }

    Ok(SegmentDump {
        path: path.to_path_buf(),
        size_bytes,
        base_offset: base_offset_from_name(path),
        header,
        records,
        unreadable_tail,
        index: dump_index(&path.with_extension("idx"), size_bytes)?,
    })
}

fn base_offset_from_name(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    (stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit()))
        .then(|| stem.parse().ok())
        .flatten()
}

fn dump_index(path: &Path, log_size: u64) -> Result<Option<IndexDump>> {
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    // Normal reading keeps the longest consistent prefix.
    let valid_prefix = SparseIndex::parse(&raw, HEADER_SIZE, log_size).index.len();
    let (chunks, leftover) = raw.as_chunks::<ENTRY_SIZE>();
    let entries = chunks
        .iter()
        .enumerate()
        .map(|(i, &[r0, r1, r2, r3, p0, p1, p2, p3])| DumpedIndexEntry {
            relative_offset: u32::from_le_bytes([r0, r1, r2, r3]),
            position: u32::from_le_bytes([p0, p1, p2, p3]),
            valid: i < valid_prefix,
        })
        .collect();
    Ok(Some(IndexDump {
        entries,
        leftover_bytes: leftover.len(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{LogConfig, PartitionLog};
    use crate::record::Message;

    fn segment_with_records(dir: &Path, count: u64) -> PathBuf {
        let config = LogConfig {
            index_interval_bytes: 64,
            ..LogConfig::default()
        };
        let mut log = PartitionLog::open(dir, config).unwrap();
        for i in 0..count {
            log.append(&Message::new(format!("k{i}"), format!("v{i}")))
                .unwrap();
        }
        dir.join("00000000000000000000.log")
    }

    #[test]
    fn healthy_segment_is_dumped_in_full() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = segment_with_records(dir.path(), 10);
        let dump = dump_segment(&path).unwrap();
        assert_eq!(dump.header, Ok(1));
        assert_eq!(dump.base_offset, Some(0));
        assert_eq!(dump.records.len(), 10);
        assert_eq!(dump.records[0].position, HEADER_SIZE);
        assert_eq!(dump.records[3].record.key.as_deref(), Some(&b"k3"[..]));
        assert!(dump.unreadable_tail.is_none());
        let index = dump.index.unwrap();
        assert!(!index.entries.is_empty());
        assert!(index.entries.iter().all(|e| e.valid));
    }

    #[test]
    fn damage_is_reported_without_failing_or_repairing() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = segment_with_records(dir.path(), 5);
        let mut bytes = fs::read(&path).unwrap();
        let third = dump_segment(&path).unwrap().records[2].position as usize;
        bytes[third + 10] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        let dump = dump_segment(&path).unwrap();
        assert_eq!(dump.records.len(), 2);
        let tail = dump.unreadable_tail.unwrap();
        assert_eq!(tail.position, third as u64);
        assert_eq!(tail.reason, "invalid CRC");
        assert_eq!(fs::read(&path).unwrap(), bytes, "the file is not modified");
    }

    #[test]
    fn invalid_header_is_reported() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("notes.log");
        fs::write(&path, b"not a segment at all").unwrap();
        let dump = dump_segment(&path).unwrap();
        assert!(dump.header.is_err());
        assert_eq!(dump.base_offset, None);
        assert!(dump.records.is_empty());
        assert!(dump.index.is_none());
    }
}
