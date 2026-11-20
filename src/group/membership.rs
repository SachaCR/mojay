//! Appartenance à un groupe : chaque membre vivant tient le verrou de son
//! fichier `members/<id>.lock`.
//!
//! - **Vivant** : un membre est vivant tant que quelqu'un tient le verrou de son
//!   fichier. Un processus qui meurt perd ses verrous instantanément : pas de
//!   heartbeat, pas de délai d'expiration.
//! - **Inscription sans course** : le fichier est créé sous un nom temporaire,
//!   verrouillé, puis renommé dans `members/`. Le verrou suit l'inode : personne
//!   ne voit jamais ce fichier sans verrou et ne le prend pour celui d'un mort.
//! - **Rang** : les identifiants commencent par l'instant d'inscription, donc
//!   l'ordre alphabétique place les membres les plus anciens en premier.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::durable::sync_dir;
use crate::error::Result;
use crate::lock::{FileLock, Probe};

const LOCK_SUFFIX: &str = ".lock";

/// Distingue les membres créés dans le même processus à la même nanoseconde.
static NEXT_LOCAL_ID: AtomicU64 = AtomicU64::new(0);

/// L'inscription de ce consumer dans un groupe. Elle dure tant que la valeur vit.
#[derive(Debug)]
pub(super) struct Membership {
    id: String,
    members_dir: PathBuf,
    path: PathBuf,
    _lock: FileLock,
}

impl Membership {
    /// Inscrit un nouveau membre dans le groupe dont `group_dir` est le dossier.
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

        // Préparé hors de `members/`, puis renommé une fois verrouillé.
        let joining = group_dir.join(format!(".joining-{id}"));
        let lock = FileLock::try_exclusive(&joining)?.ok_or_else(|| {
            io::Error::other(format!("fichier d'inscription {id} déjà verrouillé"))
        })?;
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

    /// Les identifiants des membres vivants, triés du plus ancien au plus
    /// récent, ce membre compris. Les fichiers des membres morts sont supprimés.
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
    /// Départ propre : le fichier disparaît avant que le verrou ne soit relâché.
    /// Sans cela, les autres le supprimeraient eux-mêmes en le voyant libre.
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
        // Un membre mort laisse son fichier, sans verrou.
        let dead = dir.path().join("members/000-dead-0.lock");
        fs::write(&dead, b"").unwrap();
        assert_eq!(
            alive.living_members().unwrap(),
            vec![alive.id().to_string()]
        );
        assert!(!dead.exists());
    }
}
