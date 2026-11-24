//! A group's committed offsets: one file per topic, `offsets/<topic>`, with
//! an entry for each partition.
//!
//! Layout, little-endian: the number of partitions (`u32`), one `u64` per
//! partition (`NOT_COMMITTED` when the group has committed nothing there),
//! then the CRC32C (`u32`) of everything before it.
//!
//! One file per topic rather than per partition: a commit costs two `fsync`
//! (file, then folder) whatever the number of partitions it covers. Members
//! rewrite the file under the group's `offsets.lock`, each changing only the
//! entries of the partitions it holds; readers take no lock, since the file
//! is always replaced atomically.

use std::fs;
use std::io;
use std::path::Path;

use crate::durable::write_atomic;
use crate::error::{Result, corrupt};

const COUNT_SIZE: usize = 4;
const ENTRY_SIZE: usize = 8;
const CRC_SIZE: usize = 4;
/// Entry of a partition the group has never committed.
const NOT_COMMITTED: u64 = u64::MAX;

/// The committed offset of each partition (`None`: nothing committed), all
/// `None` if the file does not exist yet.
///
/// Returns [`Error::Corrupt`](crate::Error::Corrupt) if the file is damaged or
/// does not have `partitions` entries.
pub(super) fn read(path: &Path, partitions: u32) -> Result<Vec<Option<u64>>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(vec![None; partitions as usize]);
        }
        Err(e) => return Err(e.into()),
    };
    let offsets = decode(&bytes).ok_or_else(|| corrupt(path, "invalid committed offsets file"))?;
    if offsets.len() != partitions as usize {
        return Err(corrupt(
            path,
            format!(
                "{} partitions in the committed offsets file, the topic has {partitions}",
                offsets.len()
            ),
        ));
    }
    Ok(offsets)
}

/// Sets the offsets of `changes` (partition, offset), keeping the other
/// entries, and replaces the file atomically and durably.
///
/// The caller holds the group's `offsets.lock`, so that no other member
/// rewrites the file between the read and the write.
pub(super) fn update(path: &Path, partitions: u32, changes: &[(u32, u64)]) -> Result<()> {
    let mut offsets = read(path, partitions)?;
    for &(partition, offset) in changes {
        if let Some(entry) = offsets.get_mut(partition as usize) {
            *entry = Some(offset);
        }
    }
    write_atomic(path, &encode(&offsets))
}

fn encode(offsets: &[Option<u64>]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(COUNT_SIZE + offsets.len() * ENTRY_SIZE + CRC_SIZE);
    // At most `MAX_PARTITIONS` entries (checked at topic creation): fits in a u32.
    bytes.extend_from_slice(&(offsets.len() as u32).to_le_bytes());
    for offset in offsets {
        bytes.extend_from_slice(&offset.unwrap_or(NOT_COMMITTED).to_le_bytes());
    }
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode(bytes: &[u8]) -> Option<Vec<Option<u64>>> {
    let (body, crc) = bytes.split_last_chunk::<CRC_SIZE>()?;
    if crc32c::crc32c(body) != u32::from_le_bytes(*crc) {
        return None;
    }
    let (count, entries) = body.split_first_chunk::<COUNT_SIZE>()?;
    let count = u32::from_le_bytes(*count) as usize;
    if entries.len() != count.checked_mul(ENTRY_SIZE)? {
        return None;
    }
    let (entries, _) = entries.as_chunks::<ENTRY_SIZE>();
    let offsets = entries
        .iter()
        .map(|entry| {
            let value = u64::from_le_bytes(*entry);
            (value != NOT_COMMITTED).then_some(value)
        })
        .collect();
    Some(offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_means_nothing_committed() {
        let dir = tempfile::TempDir::new().unwrap();
        assert_eq!(read(&dir.path().join("orders"), 3).unwrap(), vec![None; 3]);
    }

    #[test]
    fn an_update_keeps_the_other_partitions() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders");
        update(&path, 4, &[(0, 42), (2, 7)]).unwrap();
        update(&path, 4, &[(2, 1_000_000)]).unwrap();
        assert_eq!(
            read(&path, 4).unwrap(),
            vec![Some(42), None, Some(1_000_000), None]
        );
    }

    #[test]
    fn encode_then_decode_round_trips() {
        let offsets = vec![Some(0), None, Some(u64::MAX - 1)];
        assert_eq!(decode(&encode(&offsets)), Some(offsets));
        assert_eq!(decode(&encode(&[])), Some(vec![]));
    }

    #[test]
    fn every_flipped_bit_and_every_truncation_is_detected() {
        let bytes = encode(&[Some(5), None, Some(9)]);
        for position in 0..bytes.len() {
            for bit in 0..8 {
                let mut damaged = bytes.clone();
                damaged[position] ^= 1 << bit;
                assert_eq!(decode(&damaged), None, "byte {position}, bit {bit}");
            }
        }
        for length in 0..bytes.len() {
            assert_eq!(decode(&bytes[..length]), None, "cut at {length}");
        }
    }

    #[test]
    fn damaged_file_or_wrong_partition_count_is_reported() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders");
        update(&path, 2, &[(0, 3)]).unwrap();
        assert!(read(&path, 3).is_err());
        fs::write(&path, b"oops").unwrap();
        assert!(read(&path, 2).is_err());
    }
}
