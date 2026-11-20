//! The writer side of a segment: appending, indexing, checking and repairing the end.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{
    HEADER_SIZE, PosReader, READ_BUFFER_SIZE, ReadRequest, SegmentRead, check_header, header_bytes,
    index_path, log_path, read_records,
};
use crate::error::{Result, corrupt};
use crate::index::{IndexEntry, IndexPolicy, SparseIndex};
use crate::record::{self, ReadOutcome, Record};

#[derive(Debug)]
pub(crate) struct Segment {
    base_offset: u64,
    /// Offset of the next record: the base of the next segment if this one is sealed.
    next_offset: u64,
    /// Number of valid bytes in the `.log`, header included.
    size: u64,
    log: File,
    index_file: File,
    log_path: PathBuf,
    index: SparseIndex,
    index_policy: IndexPolicy,
}

/// Where a sequential scan of the segment stops.
struct ScanEnd {
    position: u64,
    next_offset: u64,
    /// `None` = clean end of file; otherwise the reason for stopping.
    damage: Option<String>,
}

impl Segment {
    /// Creates an empty segment, overwriting any leftovers.
    pub(crate) fn create(dir: &Path, base_offset: u64, index_interval: u64) -> Result<Self> {
        let log_path = log_path(dir, base_offset);
        let log = open_rw(&log_path, true)?;
        log.write_all_at(&header_bytes(), 0)?;
        let index_file = open_rw(&index_path(dir, base_offset), true)?;
        Ok(Self {
            base_offset,
            next_offset: base_offset,
            size: HEADER_SIZE,
            log,
            index_file,
            log_path,
            index: SparseIndex::default(),
            index_policy: IndexPolicy::new(base_offset, index_interval),
        })
    }

    /// Opens an existing segment and checks its end, from the last valid
    /// index entry. The index is completed or rebuilt if needed.
    ///
    /// The active segment (`is_active`) is repaired: the longest prefix
    /// of valid records is kept and the rest, which comes from an interrupted
    /// write, is truncated. A sealed segment is never truncated:
    /// an anomaly there is real corruption, reported as such.
    pub(crate) fn open(
        dir: &Path,
        base_offset: u64,
        index_interval: u64,
        is_active: bool,
    ) -> Result<Self> {
        let log_path = log_path(dir, base_offset);
        let log = open_rw(&log_path, false)?;
        let len = log.metadata()?.len();

        if len < HEADER_SIZE {
            if is_active {
                // Crash between creating the file and writing the header.
                drop(log);
                return Self::create(dir, base_offset, index_interval);
            }
            return Err(corrupt(&log_path, "sealed segment without a header"));
        }
        check_header(&log, &log_path)?;

        let index_file = open_rw(&index_path(dir, base_offset), false)?;
        let mut segment = Self {
            base_offset,
            next_offset: base_offset,
            size: len,
            log,
            index_file,
            log_path,
            index: SparseIndex::default(),
            index_policy: IndexPolicy::new(base_offset, index_interval),
        };
        segment.check_tail(is_active)?;
        Ok(segment)
    }

    pub(crate) fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub(crate) fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.size == HEADER_SIZE
    }

    /// Current length of the `.log` file on disk.
    pub(crate) fn file_len(&self) -> Result<u64> {
        Ok(self.log.metadata()?.len())
    }

    /// Catches up with records that other writers appended to this active
    /// segment: its valid end and next offset come from the shared partition
    /// state (published by the last lock holder), and the index entries they
    /// added are reloaded from the index file.
    ///
    /// Must be called under the partition's write lock, after checking that
    /// the file length matches `end_position`.
    pub(crate) fn catch_up(&mut self, end_position: u64, next_offset: u64) -> Result<()> {
        self.size = end_position;
        self.next_offset = next_offset;

        let len = self.index_file.metadata()?.len();
        let mut raw = vec![0u8; usize::try_from(len).unwrap_or(0)];
        self.index_file.read_exact_at(&mut raw, 0)?;
        let parsed = SparseIndex::parse(&raw, HEADER_SIZE, end_position);
        self.index = parsed.index;
        if !parsed.clean {
            // Entries past the valid end: drop them so new ones follow the last valid one.
            self.write_index_file()?;
        }
        let last_entry = self.index.last().map_or(HEADER_SIZE, |e| e.position());
        self.index_policy.resume(end_position - last_entry);
        Ok(())
    }

    /// Appends already-encoded records, one after another.
    /// `sizes` gives the size of each one, in order; their offsets
    /// start at `next_offset`.
    ///
    /// The log is written before the index: an index entry never points
    /// to a record that has not been written yet.
    pub(crate) fn append(&mut self, bytes: &[u8], sizes: &[usize]) -> Result<()> {
        debug_assert_eq!(sizes.iter().sum::<usize>(), bytes.len());
        self.log.write_all_at(bytes, self.size)?;

        let mut new_entries = Vec::new();
        let mut position = self.size;
        let mut offset = self.next_offset;
        for &size in sizes {
            if let Some(entry) = self.index_policy.observe(offset, position, size) {
                new_entries.push(entry);
            }
            position += size as u64;
            offset += 1;
        }
        self.size = position;
        self.next_offset = offset;

        if !new_entries.is_empty() {
            let at = (self.index.len() * crate::index::ENTRY_SIZE) as u64;
            self.index_file
                .write_all_at(&SparseIndex::encode(&new_entries), at)?;
            self.index.extend(new_entries);
        }
        Ok(())
    }

    /// Time of the last write to the `.log`.
    pub(crate) fn last_write(&self) -> Result<SystemTime> {
        Ok(self.log.metadata()?.modified()?)
    }

    /// Deletes the segment from disk: the `.log` first, then the index. A
    /// crash between the two leaves an orphan index, ignored then cleaned up on
    /// the next open; never a log without its beginning.
    /// Already deleted files (by another writer) are not an error.
    pub(crate) fn delete(self) -> Result<()> {
        remove_if_present(&self.log_path)?;
        remove_if_present(&self.log_path.with_extension("idx"))
    }

    /// Forces the log and the index to disk.
    pub(crate) fn sync(&self) -> Result<()> {
        self.log.sync_data()?;
        self.index_file.sync_data()?;
        Ok(())
    }

    pub(crate) fn read(&self, req: &ReadRequest, out: &mut Vec<Record>) -> Result<SegmentRead> {
        let start = self
            .index
            .floor(self.base_offset, req.from)
            .map_or(HEADER_SIZE, |e| e.position());
        read_records(&self.log, &self.log_path, start, self.size, req, out)
    }

    /// Checks the end of the segment, repairs it if active, and brings the index
    /// in line with the log.
    ///
    /// The scan starts from the last valid index entry: it usually only reads
    /// a few KiB, and the whole segment if the index is missing.
    fn check_tail(&mut self, is_active: bool) -> Result<()> {
        let mut raw = Vec::new();
        (&self.index_file).read_to_end(&mut raw)?;
        let parsed = SparseIndex::parse(&raw, HEADER_SIZE, self.size);
        self.index = parsed.index;
        let loaded_entries = self.index.len();

        // Index entries may have survived lost records
        // (different files, no ordering guaranteed on disk): step back
        // to an entry that really points to the expected record.
        while let Some(entry) = self.index.last() {
            if self.entry_is_valid(entry)? {
                break;
            }
            self.index.pop();
        }
        let (start, start_offset) = match self.index.last() {
            Some(e) => (e.position(), e.offset(self.base_offset)),
            None => (HEADER_SIZE, self.base_offset),
        };

        let end = self.rebuild_from(start, start_offset)?;
        if let Some(reason) = end.damage {
            if !is_active {
                return Err(corrupt(&self.log_path, reason));
            }
            // End of an interrupted write: remove it.
            self.log.set_len(end.position)?;
            self.log.sync_data()?;
            self.size = end.position;
        }
        self.next_offset = end.next_offset;

        if !parsed.clean || self.index.len() != loaded_entries {
            self.write_index_file()?;
        }
        Ok(())
    }

    fn entry_is_valid(&self, entry: IndexEntry) -> io::Result<bool> {
        let mut reader = PosReader {
            file: &self.log,
            pos: entry.position(),
            end: self.size,
        };
        Ok(match record::read_record(&mut reader)? {
            ReadOutcome::Record(rec, _) => rec.offset == entry.offset(self.base_offset),
            _ => false,
        })
    }

    /// Scans the log from `start` (where `start_offset` must be),
    /// fills in the in-memory index along the way, and returns where the scan
    /// stopped. The index entry pointing to `start`, if there is one,
    /// must already be in the index.
    fn rebuild_from(&mut self, start: u64, start_offset: u64) -> Result<ScanEnd> {
        let mut reader = BufReader::with_capacity(
            READ_BUFFER_SIZE,
            PosReader {
                file: &self.log,
                pos: start,
                end: self.size,
            },
        );
        let mut position = start;
        let mut expected = start_offset;
        let mut new_entries = Vec::new();
        let policy = &mut self.index_policy;
        policy.reset();

        let damage = loop {
            match record::read_record(&mut reader)? {
                ReadOutcome::Record(rec, size) => {
                    if rec.offset != expected {
                        break Some(format!(
                            "offset {} found at position {position}, expected {expected}",
                            rec.offset
                        ));
                    }
                    let entry = policy.observe(rec.offset, position, size);
                    // The starting record already has its entry, if needed.
                    if let Some(entry) = entry.filter(|_| position != start) {
                        new_entries.push(entry);
                    }
                    position += size as u64;
                    expected += 1;
                }
                ReadOutcome::End => break None,
                ReadOutcome::Incomplete => {
                    break Some(format!("incomplete record at position {position}"));
                }
                ReadOutcome::Corrupt(reason) => {
                    break Some(format!("{reason} at position {position}"));
                }
            }
        };
        self.index.extend(new_entries);
        Ok(ScanEnd {
            position,
            next_offset: expected,
            damage,
        })
    }

    fn write_index_file(&self) -> Result<()> {
        self.index_file.set_len(0)?;
        self.index_file
            .write_all_at(&SparseIndex::encode(self.index.entries()), 0)?;
        Ok(())
    }
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn open_rw(path: &Path, truncate: bool) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate)
        .open(path)
}
