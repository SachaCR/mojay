//! Sparse index of a segment: `(relative offset u32, position u32)` pairs
//! in little-endian, one every `interval` bytes written.
//!
//! The index is a cache: it can always be rebuilt from the `.log`.
//! This module is pure logic, with no I/O.

pub(crate) const ENTRY_SIZE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub(crate) relative_offset: u32,
    pub(crate) position: u32,
}

impl IndexEntry {
    pub(crate) fn offset(&self, base_offset: u64) -> u64 {
        base_offset + u64::from(self.relative_offset)
    }

    pub(crate) fn position(&self) -> u64 {
        u64::from(self.position)
    }
}

/// The entries of an index, sorted by offset and by position.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SparseIndex {
    entries: Vec<IndexEntry>,
}

/// Result of decoding an index file.
pub(crate) struct ParsedIndex {
    pub(crate) index: SparseIndex,
    /// `false` if bytes had to be discarded (truncated end, inconsistent entry).
    pub(crate) clean: bool,
}

impl SparseIndex {
    /// Decodes an index file, keeping only the longest consistent
    /// prefix: positions in `[min_position, log_size)`, offsets and
    /// positions strictly increasing.
    pub(crate) fn parse(raw: &[u8], min_position: u64, log_size: u64) -> ParsedIndex {
        let (chunks, leftover) = raw.as_chunks::<ENTRY_SIZE>();
        let mut entries: Vec<IndexEntry> = Vec::with_capacity(chunks.len());
        let mut clean = leftover.is_empty();
        for &[r0, r1, r2, r3, p0, p1, p2, p3] in chunks {
            let entry = IndexEntry {
                relative_offset: u32::from_le_bytes([r0, r1, r2, r3]),
                position: u32::from_le_bytes([p0, p1, p2, p3]),
            };
            let in_bounds = entry.position() >= min_position && entry.position() < log_size;
            let increasing = entries.last().is_none_or(|p| {
                entry.relative_offset > p.relative_offset && entry.position > p.position
            });
            if !in_bounds || !increasing {
                clean = false;
                break;
            }
            entries.push(entry);
        }
        ParsedIndex {
            index: Self { entries },
            clean,
        }
    }

    pub(crate) fn encode(entries: &[IndexEntry]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(entries.len() * ENTRY_SIZE);
        for e in entries {
            buf.extend_from_slice(&e.relative_offset.to_le_bytes());
            buf.extend_from_slice(&e.position.to_le_bytes());
        }
        buf
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    pub(crate) fn last(&self) -> Option<IndexEntry> {
        self.entries.last().copied()
    }

    pub(crate) fn pop(&mut self) {
        self.entries.pop();
    }

    pub(crate) fn extend(&mut self, entries: impl IntoIterator<Item = IndexEntry>) {
        self.entries.extend(entries);
    }

    /// The last entry whose offset is less than or equal to `offset`.
    pub(crate) fn floor(&self, base_offset: u64, offset: u64) -> Option<IndexEntry> {
        let relative = offset.saturating_sub(base_offset);
        let idx = self
            .entries
            .partition_point(|e| u64::from(e.relative_offset) <= relative);
        idx.checked_sub(1).map(|i| self.entries[i])
    }
}

/// Indexing rule: one entry as soon as `interval` bytes have been written
/// since the previous one.
#[derive(Debug)]
pub(crate) struct IndexPolicy {
    base_offset: u64,
    interval: u64,
    bytes_since_entry: u64,
}

impl IndexPolicy {
    pub(crate) fn new(base_offset: u64, interval: u64) -> Self {
        Self {
            base_offset,
            interval,
            bytes_since_entry: 0,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.bytes_since_entry = 0;
    }

    /// Resumes after records written by someone else: `bytes` were written
    /// since the last index entry.
    pub(crate) fn resume(&mut self, bytes: u64) {
        self.bytes_since_entry = bytes;
    }

    /// Accounts for a record of `size` bytes written at `position`,
    /// and returns the index entry that must point to it, if one is needed.
    pub(crate) fn observe(
        &mut self,
        offset: u64,
        position: u64,
        size: usize,
    ) -> Option<IndexEntry> {
        let entry = if self.bytes_since_entry >= self.interval {
            self.bytes_since_entry = 0;
            Some(IndexEntry {
                relative_offset: (offset - self.base_offset) as u32,
                position: position as u32,
            })
        } else {
            None
        };
        self.bytes_since_entry += size as u64;
        entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(relative_offset: u32, position: u32) -> IndexEntry {
        IndexEntry {
            relative_offset,
            position,
        }
    }

    #[test]
    fn parse_keeps_the_longest_coherent_prefix() {
        let raw = SparseIndex::encode(&[entry(2, 100), entry(5, 300), entry(4, 400)]);
        let parsed = SparseIndex::parse(&raw, 16, 1000);
        assert!(!parsed.clean);
        assert_eq!(parsed.index.entries(), &[entry(2, 100), entry(5, 300)]);
    }

    #[test]
    fn parse_rejects_positions_outside_the_log() {
        let raw = SparseIndex::encode(&[entry(2, 100), entry(5, 300)]);
        let parsed = SparseIndex::parse(&raw, 16, 200);
        assert!(!parsed.clean);
        assert_eq!(parsed.index.len(), 1);
    }

    #[test]
    fn parse_flags_a_truncated_entry() {
        let mut raw = SparseIndex::encode(&[entry(2, 100)]);
        raw.push(7);
        let parsed = SparseIndex::parse(&raw, 16, 1000);
        assert!(!parsed.clean);
        assert_eq!(parsed.index.len(), 1);
    }

    #[test]
    fn floor_finds_the_closest_entry_below() {
        let raw = SparseIndex::encode(&[entry(10, 100), entry(20, 200)]);
        let index = SparseIndex::parse(&raw, 16, 1000).index;
        assert_eq!(index.floor(1000, 1005), None);
        assert_eq!(index.floor(1000, 1010), Some(entry(10, 100)));
        assert_eq!(index.floor(1000, 1019), Some(entry(10, 100)));
        assert_eq!(index.floor(1000, 1500), Some(entry(20, 200)));
    }

    #[test]
    fn policy_adds_an_entry_after_each_interval() {
        let mut policy = IndexPolicy::new(100, 50);
        let mut position = 16;
        let entries: Vec<_> = (100..110)
            .filter_map(|offset| {
                let e = policy.observe(offset, position, 20);
                position += 20;
                e
            })
            .collect();
        // 20 bytes per record: one entry every 3 records.
        assert_eq!(entries, vec![entry(3, 76), entry(6, 136), entry(9, 196)]);
    }
}
