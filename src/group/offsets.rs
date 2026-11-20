//! A group's committed offsets: one file per partition, `offsets/<topic>-<p>`.
//!
//! The file is 12 bytes: the offset (`u64`) then its CRC32C (`u32`), in
//! little-endian. It is only written by the holder of the partition's
//! lock, and always replaced atomically.

use std::fs;
use std::io;
use std::path::Path;

use crate::durable::write_atomic;
use crate::error::{Result, corrupt};

const FILE_SIZE: usize = 12;

/// The committed offset, or `None` if the group has never committed anything here.
pub(super) fn read(path: &Path) -> Result<Option<u64>> {
    match fs::read(path) {
        Ok(bytes) => decode(&bytes)
            .map(Some)
            .ok_or_else(|| corrupt(path, "invalid committed offset file")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Replaces the committed offset, atomically and durably.
pub(super) fn write(path: &Path, offset: u64) -> Result<()> {
    write_atomic(path, &encode(offset))
}

fn encode(offset: u64) -> [u8; FILE_SIZE] {
    let value = offset.to_le_bytes();
    let mut bytes = [0u8; FILE_SIZE];
    bytes[..8].copy_from_slice(&value);
    bytes[8..].copy_from_slice(&crc32c::crc32c(&value).to_le_bytes());
    bytes
}

fn decode(bytes: &[u8]) -> Option<u64> {
    let [o0, o1, o2, o3, o4, o5, o6, o7, c0, c1, c2, c3] = *bytes else {
        return None;
    };
    let value = [o0, o1, o2, o3, o4, o5, o6, o7];
    (crc32c::crc32c(&value) == u32::from_le_bytes([c0, c1, c2, c3]))
        .then(|| u64::from_le_bytes(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_means_nothing_committed() {
        let dir = tempfile::TempDir::new().unwrap();
        assert_eq!(read(&dir.path().join("orders-0")).unwrap(), None);
    }

    #[test]
    fn written_offset_is_read_back() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders-0");
        write(&path, 42).unwrap();
        write(&path, 1_000_000).unwrap();
        assert_eq!(read(&path).unwrap(), Some(1_000_000));
    }

    #[test]
    fn damaged_file_is_reported() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders-0");
        let mut bytes = encode(7);
        bytes[0] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(read(&path).is_err());
        fs::write(&path, [0u8; 5]).unwrap();
        assert!(read(&path).is_err());
    }
}
