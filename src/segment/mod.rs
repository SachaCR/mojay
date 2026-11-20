//! A segment = an append-only `.log` file and its sparse `.idx` index.
//!
//! The `.log` starts with a 16-byte header (magic `MOJAY\0`, `u16`
//! version, 8 reserved bytes), followed by the records one after another.
//!
//! - [`Segment`]: the writer side, which appends, repairs and indexes.
//! - [`SegmentReader`]: the reader side, read-only, usable from another
//!   process while the writer is writing.

mod reader;
mod writer;

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub(crate) use reader::SegmentReader;
pub(crate) use writer::Segment;

use crate::error::{Result, corrupt};
use crate::record::{self, ReadOutcome, Record};

pub(crate) const HEADER_SIZE: u64 = 16;
const MAGIC: &[u8; 6] = b"MOJAY\0";
const FORMAT_VERSION: u16 = 1;
const READ_BUFFER_SIZE: usize = 64 * 1024;

pub(crate) fn log_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.log"))
}

pub(crate) fn index_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.idx"))
}

/// What a segment read must return.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadRequest {
    /// First wanted offset (inclusive).
    pub(crate) from: u64,
    /// Offset at which to stop (exclusive): the read limit, or the base of the next segment.
    pub(crate) end: u64,
    /// Number of encoded bytes not to exceed.
    pub(crate) budget: usize,
    /// Take the first record even if it exceeds the budget.
    pub(crate) take_one: bool,
}

/// Why a segment read stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentRead {
    /// Nothing left to read in this segment before `end`: reading can continue with the next one.
    Exhausted { taken: usize },
    /// The next record would exceed the budget: reading stops there.
    BudgetReached,
}

/// Reads the records requested by `req`, starting at position `start`
/// and without going past `size` bytes of the file.
///
/// Beyond `req.end`, another process may be writing: an
/// unreadable record there is not an error. Before it, it is one.
fn read_records(
    log: &File,
    path: &Path,
    start: u64,
    size: u64,
    req: &ReadRequest,
    out: &mut Vec<Record>,
) -> Result<SegmentRead> {
    let mut reader = BufReader::with_capacity(
        READ_BUFFER_SIZE,
        PosReader {
            file: log,
            pos: start,
            end: size,
        },
    );
    let mut taken = 0;
    let mut first = req.take_one;
    let mut next_offset: Option<u64> = None;
    loop {
        match record::read_record(&mut reader)? {
            ReadOutcome::Record(rec, size) => {
                next_offset = Some(rec.offset + 1);
                if rec.offset < req.from {
                    continue;
                }
                if rec.offset >= req.end {
                    return Ok(SegmentRead::Exhausted { taken });
                }
                if !first && taken + size > req.budget {
                    return Ok(SegmentRead::BudgetReached);
                }
                first = false;
                taken += size;
                out.push(rec);
            }
            ReadOutcome::End => return Ok(SegmentRead::Exhausted { taken }),
            ReadOutcome::Incomplete | ReadOutcome::Corrupt(_) => {
                if next_offset.is_some_and(|next| next >= req.end) {
                    return Ok(SegmentRead::Exhausted { taken });
                }
                return Err(corrupt(path, "unreadable record before the read limit"));
            }
        }
    }
}

/// Bounded positional read (`pread`), with no shared cursor on the file.
struct PosReader<'a> {
    file: &'a File,
    pos: u64,
    end: u64,
}

impl Read for PosReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.end.saturating_sub(self.pos);
        let max = buf.len().min(remaining.try_into().unwrap_or(usize::MAX));
        if max == 0 {
            return Ok(0);
        }
        let n = self.file.read_at(&mut buf[..max], self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

fn header_bytes() -> [u8; HEADER_SIZE as usize] {
    let mut h = [0u8; HEADER_SIZE as usize];
    h[..6].copy_from_slice(MAGIC);
    h[6..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    h
}

fn check_header(log: &File, path: &Path) -> Result<()> {
    let mut h = [0u8; HEADER_SIZE as usize];
    log.read_exact_at(&mut h, 0)?;
    parse_header(&h)
        .map(|_| ())
        .map_err(|reason| corrupt(path, reason))
}

/// Checks a segment's header and returns its format version.
pub(crate) fn parse_header(bytes: &[u8]) -> std::result::Result<u16, String> {
    let Some(h) = bytes.get(..HEADER_SIZE as usize) else {
        return Err(format!("{} bytes, incomplete header", bytes.len()));
    };
    if &h[..6] != MAGIC {
        return Err("invalid magic, not a Mojay segment".into());
    }
    let version = u16::from_le_bytes([h[6], h[7]]);
    if version != FORMAT_VERSION {
        return Err(format!("unsupported format version {version}"));
    }
    Ok(version)
}
