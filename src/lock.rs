//! Verrous inter-processus sur fichier, avec `flock`.
//!
//! On n'utilise jamais les verrous POSIX `fcntl` : ils sont relâchés dès que le
//! processus ferme *n'importe quel* descripteur sur le fichier. Un verrou
//! `flock` appartient à la description de fichier ouverte : il est relâché à la
//! fermeture de *ce* descripteur, ou par l'OS si le processus meurt.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use rustix::fs::{FlockOperation, flock};
use rustix::io::Errno;

use crate::error::Result;

/// Un verrou exclusif, tenu tant que la valeur vit.
#[derive(Debug)]
pub(crate) struct FileLock {
    file: File,
}

/// Résultat d'une tentative de verrouillage d'un fichier qui doit déjà exister.
#[derive(Debug)]
pub(crate) enum Probe {
    /// Le fichier n'existe pas (ou plus).
    Missing,
    /// Quelqu'un tient le verrou.
    Held,
    /// Personne ne le tenait : il est maintenant à nous.
    Acquired(FileLock),
}

impl FileLock {
    /// Tente de prendre le verrou exclusif sur `path` (créé au besoin), sans attendre.
    /// Renvoie `None` si quelqu'un d'autre le tient, y compris dans ce processus.
    pub(crate) fn try_exclusive(path: &Path) -> Result<Option<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Self::try_lock(file)
    }

    /// Comme [`try_exclusive`](Self::try_exclusive), mais sans jamais créer le
    /// fichier : sert à savoir si le propriétaire d'un fichier est encore vivant.
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
    /// Déverrouille explicitement avant de fermer le fichier.
    ///
    /// Fermer le descripteur ne suffit pas : si un autre thread vient de lancer
    /// un sous-processus (`fork`), l'enfant partage la même description de
    /// fichier jusqu'à son `exec`, et le verrou survivrait jusque-là. `LOCK_UN`
    /// libère le verrou pour toutes les copies à la fois.
    fn drop(&mut self) {
        // Un échec ici n'a pas de recours : la fermeture du fichier, juste
        // après, relâche le verrou dans le cas courant.
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
        assert!(!path.exists(), "la sonde ne crée jamais le fichier");

        let held = FileLock::try_exclusive(&path).unwrap().unwrap();
        assert!(matches!(FileLock::probe(&path).unwrap(), Probe::Held));
        drop(held);
        assert!(matches!(
            FileLock::probe(&path).unwrap(),
            Probe::Acquired(_)
        ));
    }
}
