//! Limites de lecture partagées entre processus.
//!
//! Le fichier `read_limits` d'un topic contient un `u64` par partition : le
//! premier offset que les lecteurs n'ont pas encore le droit de lire. Le writer
//! le publie après chaque ajout ; les lecteurs ne lisent jamais au-delà.
//!
//! Le fichier est mappé en mémoire (`mmap` partagé) par tous les processus :
//! publier ou lire une limite est une simple opération atomique en mémoire,
//! sans appel système.
//!
//! C'est le seul module où `unsafe` est autorisé (voir AGENT.md).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapOptions, MmapRaw};

use crate::error::{Error, Result, corrupt};

const SLOT_SIZE: usize = std::mem::size_of::<u64>();

/// Crée le fichier de `partitions` limites, toutes à zéro.
pub(crate) fn create(path: &Path, partitions: u32) -> Result<()> {
    let mut file = File::create_new(path)?;
    file.write_all(&vec![0u8; partitions as usize * SLOT_SIZE])?;
    file.sync_all()?;
    Ok(())
}

/// Le fichier mappé. Toujours en lecture-écriture, même pour un lecteur :
/// une référence atomique exige une mémoire inscriptible (voir `slot`).
#[derive(Debug)]
struct Mapping {
    map: MmapRaw,
    partitions: u32,
}

impl Mapping {
    fn open(path: &Path, partitions: u32) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let expected = partitions as usize * SLOT_SIZE;
        let len = file.metadata()?.len();
        if partitions == 0 || len != expected as u64 {
            return Err(corrupt(
                path,
                format!("{len} octets, {expected} attendus pour {partitions} partitions"),
            ));
        }
        let map = MmapOptions::new().len(expected).map_raw(&file)?;
        Ok(Self { map, partitions })
    }

    fn slot(&self, partition: u32) -> Result<&AtomicU64> {
        if partition >= self.partitions {
            return Err(Error::UnknownPartition {
                partition,
                partitions: self.partitions,
            });
        }
        let ptr = self.map.as_mut_ptr().cast::<u64>();
        // SAFETY:
        // - Alignement : un mapping commence sur une frontière de page, et chaque
        //   case est à un multiple de 8 octets du début.
        // - Bornes : `partition < partitions`, et le mapping fait exactement
        //   `partitions * 8` octets (vérifié à l'ouverture).
        // - Durée de vie : la référence emprunte `self`, donc le mapping vit au
        //   moins aussi longtemps qu'elle.
        // - Mémoire inscriptible : le mapping est toujours en lecture-écriture.
        // - Accès concurrents : dans tous les processus, ces octets ne sont lus
        //   et écrits qu'à travers des `AtomicU64` (ce module), jamais par
        //   `read`/`write` une fois le fichier créé.
        Ok(unsafe { AtomicU64::from_ptr(ptr.add(partition as usize)) })
    }
}

/// Vue lecteur : lit les limites publiées par le writer.
#[derive(Debug)]
pub(crate) struct ReadLimits(Mapping);

impl ReadLimits {
    pub(crate) fn open(path: &Path, partitions: u32) -> Result<Self> {
        Mapping::open(path, partitions).map(Self)
    }

    /// Limite de lecture de `partition`. L'ordre *Acquire* garantit que les
    /// données écrites avant la publication sont visibles après cette lecture.
    pub(crate) fn get(&self, partition: u32) -> Result<u64> {
        Ok(self.0.slot(partition)?.load(Ordering::Acquire))
    }
}

/// Vue writer : publie les limites. Une seule doit exister par topic, ce que
/// garantit le verrou du writer.
#[derive(Debug)]
pub(crate) struct ReadLimitsWriter(Mapping);

impl ReadLimitsWriter {
    pub(crate) fn open(path: &Path, partitions: u32) -> Result<Self> {
        Mapping::open(path, partitions).map(Self)
    }

    /// Rend lisibles tous les offsets inférieurs à `limit`. L'ordre *Release*
    /// garantit que les écritures du log faites avant sont visibles de tout
    /// lecteur qui voit la nouvelle limite.
    pub(crate) fn publish(&self, partition: u32, limit: u64) -> Result<()> {
        self.0.slot(partition)?.store(limit, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_publishes_and_reader_sees() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("read_limits");
        create(&path, 3).unwrap();
        let writer = ReadLimitsWriter::open(&path, 3).unwrap();
        let reader = ReadLimits::open(&path, 3).unwrap();
        assert_eq!(reader.get(2).unwrap(), 0);
        writer.publish(2, 42).unwrap();
        assert_eq!(reader.get(2).unwrap(), 42);
        assert_eq!(reader.get(0).unwrap(), 0);
    }

    #[test]
    fn unknown_partition_is_an_error_not_a_crash() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("read_limits");
        create(&path, 2).unwrap();
        let reader = ReadLimits::open(&path, 2).unwrap();
        assert!(matches!(
            reader.get(2),
            Err(Error::UnknownPartition { partition: 2, .. })
        ));
    }

    #[test]
    fn file_of_the_wrong_size_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("read_limits");
        create(&path, 2).unwrap();
        assert!(matches!(
            ReadLimits::open(&path, 3),
            Err(Error::Corrupt { .. })
        ));
    }
}
