//! Écritures durables sur le système de fichiers : ce qui doit survivre à une
//! coupure de courant passe par ici.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::error::Result;

/// Remplace `path` par `contents` de façon atomique : un lecteur voit soit
/// l'ancien contenu, soit le nouveau, jamais un mélange, même après un crash.
///
/// Fichier temporaire, `fsync`, `rename`, puis `fsync` du dossier.
pub(crate) fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let tmp = dir.join(format!(".{file_name}.tmp-{}", std::process::id()));

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);

    fs::rename(&tmp, path)?;
    sync_dir(dir)
}

/// Rend durable la création, la suppression ou le renommage d'une entrée de `dir`.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_atomic_replaces_content_and_leaves_no_temporary_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("meta.toml");
        write_atomic(&path, b"v1").unwrap();
        write_atomic(&path, b"v2").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"v2");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
