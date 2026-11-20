//! Inter-process file locks, using `flock`.
//!
//! POSIX `fcntl` locks are never used: they are released as soon as the
//! process closes *any* descriptor on the file. A `flock` lock
//! belongs to the open file description: it is released when
//! *that* descriptor is closed, or by the OS if the process dies.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use rustix::fs::{FlockOperation, flock};
use rustix::io::Errno;

use crate::error::Result;

/// An exclusive lock, held for as long as the value lives.
#[derive(Debug)]
pub(crate) struct FileLock {
    file: File,
}

/// Result of an attempt to lock a file that must already exist.
#[derive(Debug)]
pub(crate) enum Probe {
    /// The file does not exist (or no longer exists).
    Missing,
    /// Someone holds the lock.
    Held,
    /// Nobody was holding it: it is now ours.
    Acquired(FileLock),
}

impl FileLock {
    /// Tries to take the exclusive lock on `path` (created if needed), without waiting.
    /// Returns `None` if someone else holds it, including in this process.
    pub(crate) fn try_exclusive(path: &Path) -> Result<Option<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Self::try_lock(file)
    }

    /// Like [`try_exclusive`](Self::try_exclusive), but never creates the
    /// file: used to find out whether a file's owner is still alive.
    pub(crate) fn probe(path: &Path) -> Result<Probe> {
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Probe::Missing),
            Err(e) => return Err(e.into()),
        };
        Ok(match Self::try_lock(file)? {
            Some(lock) => Probe::Acquired(lock),
            None => Probe::Held,
        })
    }

    fn try_lock(file: File) -> Result<Option<Self>> {
        match flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => Ok(Some(Self { file })),
            Err(Errno::WOULDBLOCK) => Ok(None),
            Err(errno) => Err(io::Error::from(errno).into()),
        }
    }
}

impl Drop for FileLock {
    /// Explicitly unlocks before closing the file.
    ///
    /// Closing the descriptor is not enough: if another thread has just spawned
    /// a subprocess (`fork`), the child shares the same open file
    /// description until its `exec`, and the lock would survive until then. `LOCK_UN`
    /// releases the lock for all copies at once.
    fn drop(&mut self) {
        // A failure here has no remedy: closing the file, right
        // afterwards, releases the lock in the common case.
        let _ = flock(&self.file, FlockOperation::Unlock);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_is_exclusive_until_dropped() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("writer.lock");
        let first = FileLock::try_exclusive(&path).unwrap();
        assert!(first.is_some());
        assert!(FileLock::try_exclusive(&path).unwrap().is_none());
        drop(first);
        assert!(FileLock::try_exclusive(&path).unwrap().is_some());
    }

    #[test]
    fn probe_tells_missing_held_and_free_apart() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("member.lock");
        assert!(matches!(FileLock::probe(&path).unwrap(), Probe::Missing));
        assert!(!path.exists(), "the probe never creates the file");

        let held = FileLock::try_exclusive(&path).unwrap().unwrap();
        assert!(matches!(FileLock::probe(&path).unwrap(), Probe::Held));
        drop(held);
        assert!(matches!(
            FileLock::probe(&path).unwrap(),
            Probe::Acquired(_)
        ));
    }
}
