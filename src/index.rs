//! Index clairsemé d'un segment : des paires `(offset relatif u32, position u32)`
//! en little-endian, une tous les `interval` octets écrits.
//!
//! L'index est un cache : il peut toujours être reconstruit depuis le `.log`.
//! Ce module est de la logique pure, sans I/O.

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

/// Les entrées d'un index, triées par offset et par position.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SparseIndex {
    entries: Vec<IndexEntry>,
}

/// Résultat du décodage d'un fichier d'index.
pub(crate) struct ParsedIndex {
    pub(crate) index: SparseIndex,
    /// `false` si des octets ont dû être écartés (fin tronquée, entrée incohérente).
    pub(crate) clean: bool,
}

impl SparseIndex {
    /// Décode un fichier d'index en ne gardant que le plus long préfixe
    /// cohérent : positions dans `[min_position, log_size)`, offsets et
    /// positions strictement croissants.
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

    /// La dernière entrée dont l'offset est inférieur ou égal à `offset`.
    pub(crate) fn floor(&self, base_offset: u64, offset: u64) -> Option<IndexEntry> {
        let relative = offset.saturating_sub(base_offset);
        let idx = self
            .entries
            .partition_point(|e| u64::from(e.relative_offset) <= relative);
        idx.checked_sub(1).map(|i| self.entries[i])
    }
}

/// Règle d'indexation : une entrée dès que `interval` octets ont été écrits
/// depuis la précédente.
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

    /// Prend en compte un enregistrement de `size` octets écrit à `position`,
    /// et renvoie l'entrée d'index qui doit pointer sur lui, s'il en faut une.
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
        // 20 octets par enregistrement : une entrée tous les 3 enregistrements.
        assert_eq!(entries, vec![entry(3, 76), entry(6, 136), entry(9, 196)]);
    }
}
