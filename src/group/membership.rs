//! Group membership: each live member holds the lock on its
//! `members/<id>.lock` file.
//!
//! - **Live**: a member is alive as long as someone holds the lock on its
//!   file. A process that dies loses its locks instantly: no
//!   heartbeat, no expiry timeout.
//! - **Race-free registration**: the file is created under a temporary name,
//!   locked, then renamed into `members/`. The lock follows the inode: nobody
//!   ever sees this file unlocked and mistakes it for a dead member's.
//! - **Rank**: identifiers start with the registration time, so
//!   alphabetical order puts the oldest members first.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::durable::sync_dir;
use crate::error::Result;
use crate::lock::{FileLock, Probe};

const LOCK_SUFFIX: &str = ".lock";

/// Distinguishes members created in the same process in the same nanosecond.
static NEXT_LOCAL_ID: AtomicU64 = AtomicU64::new(0);

/// This consumer's registration in a group. It lasts as long as the value lives.
#[derive(Debug)]
pub(super) struct Membership {
    id: String,
    members_dir: PathBuf,
    path: PathBuf,
    _lock: FileLock,
}

impl Membership {
    /// Registers a new member in the group whose directory is `group_dir`.
    pub(super) fn join(group_dir: &Path) -> Result<Self> {
        let members_dir = group_dir.join("members");
        fs::create_dir_all(&members_dir)?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let id = format!(
            "{nanos:024}-{}-{}",
            std::process::id(),
            NEXT_LOCAL_ID.fetch_add(1, Ordering::Relaxed)
        );

        // Prepared outside `members/`, then renamed once locked.
        let joining = group_dir.join(format!(".joining-{id}"));
        let lock = FileLock::try_exclusive(&joining)?
            .ok_or_else(|| io::Error::other(format!("membership file {id} already locked")))?;
        let path = members_dir.join(format!("{id}{LOCK_SUFFIX}"));
        fs::rename(&joining, &path)?;
        sync_dir(&members_dir)?;
        Ok(Self {
            id,
            members_dir,
            path,
            _lock: lock,
        })
    }

    pub(super) fn id(&self) -> &str {
        &self.id
    }

    /// The identifiers of the live members, sorted from oldest to most
    /// recent, this member included. The files of dead members are deleted.
    pub(super) fn living_members(&self) -> Result<Vec<String>> {
        let mut living = vec![self.id.clone()];
        for entry in fs::read_dir(&self.members_dir)? {
            let name = entry?.file_name();
            let Some(id) = name.to_str().and_then(|n| n.strip_suffix(LOCK_SUFFIX)) else {
                continue;
            };
            if id == self.id {
                continue;
            }
            let path = self.members_dir.join(&name);
            match FileLock::probe(&path)? {
                Probe::Held => living.push(id.to_string()),
                Probe::Missing => {}
                Probe::Acquired(_dead) => remove_if_present(&path)?,
            }
        }
        living.sort_unstable();
        Ok(living)
    }
}

impl Drop for Membership {
    /// Clean departure: the file disappears before the lock is released.
    /// Otherwise, the others would delete it themselves upon seeing it free.
    fn drop(&mut self) {
        let _ = remove_if_present(&self.path);
    }
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn members_see_each_other_oldest_first() {
        let dir = tempfile::TempDir::new().unwrap();
        let first = Membership::join(dir.path()).unwrap();
        let second = Membership::join(dir.path()).unwrap();
        let expected = vec![first.id().to_string(), second.id().to_string()];
        assert_eq!(first.living_members().unwrap(), expected);
        assert_eq!(second.living_members().unwrap(), expected);
    }

    #[test]
    fn departed_member_is_no_longer_counted() {
        let dir = tempfile::TempDir::new().unwrap();
        let staying = Membership::join(dir.path()).unwrap();
        drop(Membership::join(dir.path()).unwrap());
        assert_eq!(staying.living_members().unwrap().len(), 1);
    }

    #[test]
    fn dead_member_file_is_cleaned_up() {
        let dir = tempfile::TempDir::new().unwrap();
        let alive = Membership::join(dir.path()).unwrap();
        // A dead member leaves its file behind, unlocked.
        let dead = dir.path().join("members/000-dead-0.lock");
        fs::write(&dead, b"").unwrap();
        assert_eq!(
            alive.living_members().unwrap(),
            vec![alive.id().to_string()]
        );
        assert!(!dead.exists());
    }
}
