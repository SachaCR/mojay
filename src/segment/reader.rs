//! The reader side of a segment: read-only, lock-free, never modifying
//! anything, while another process may be writing.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use super::{
    HEADER_SIZE, ReadRequest, SegmentRead, check_header, index_path, log_path, read_indexed,
};
use crate::error::Result;
use crate::index::SparseIndex;
use crate::record::Record;

#[derive(Debug)]
pub(crate) struct SegmentReader {
    base_offset: u64,
    log: File,
    log_path: PathBuf,
    index_path: PathBuf,
    /// Copy of the index, re-read when the file grows.
    index: SparseIndex,
    index_bytes_seen: u64,
    header_checked: bool,
}

impl SegmentReader {
    pub(crate) fn open(dir: &Path, base_offset: u64) -> Result<Self> {
        let log_path = log_path(dir, base_offset);
        Ok(Self {
            base_offset,
            log: File::open(&log_path)?,
            log_path,
            index_path: index_path(dir, base_offset),
            index: SparseIndex::default(),
            index_bytes_seen: 0,
            header_checked: false,
        })
    }

    pub(crate) fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Reads the requested records. The file may grow between two
    /// calls: its size is re-read every time.
    pub(crate) fn read(&mut self, req: &ReadRequest, out: &mut Vec<Record>) -> Result<SegmentRead> {
        let size = self.log.metadata()?.len();
        if size < HEADER_SIZE {
            // Segment just created by the writer: nothing to read yet.
            return Ok(SegmentRead::Exhausted { taken: 0 });
        }
        if !self.header_checked {
            check_header(&self.log, &self.log_path)?;
            self.header_checked = true;
        }
        self.refresh_index(size)?;
        let (read, trusted) = read_indexed(
            &self.log,
            &self.log_path,
            self.base_offset,
            &self.index,
            size,
            req,
            out,
        )?;
        // Forget the wrong entries until the index file changes: the next
        // reads go straight to an entry that holds its record.
        self.index.truncate(trusted);
        Ok(read)
    }

    /// Re-reads the index if its file has changed size. A missing or
    /// damaged index only slows reading down: entries that are out of bounds
    /// are dropped here, and those that do not point at their record are
    /// skipped when reading (see `read_indexed`).
    fn refresh_index(&mut self, log_size: u64) -> Result<()> {
        let len = match fs::metadata(&self.index_path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        if len == self.index_bytes_seen {
            return Ok(());
        }
        let raw = match fs::read(&self.index_path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        self.index = SparseIndex::parse(&raw, HEADER_SIZE, log_size).index;
        self.index_bytes_seen = len;
        Ok(())
    }
}
