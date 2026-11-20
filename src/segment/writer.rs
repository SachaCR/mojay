//! Le côté writer d'un segment : ajout, index, vérification et réparation de la fin.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use super::{
    HEADER_SIZE, PosReader, READ_BUFFER_SIZE, ReadRequest, SegmentRead, check_header, header_bytes,
    index_path, log_path, read_records,
};
use crate::error::{Result, corrupt};
use crate::index::{IndexEntry, IndexPolicy, SparseIndex};
use crate::record::{self, ReadOutcome, Record};

#[derive(Debug)]
pub(crate) struct Segment {
    base_offset: u64,
    /// Offset du prochain enregistrement : la base du segment suivant s'il est scellé.
    next_offset: u64,
    /// Nombre d'octets valides du `.log`, en-tête compris.
    size: u64,
    log: File,
    index_file: File,
    log_path: PathBuf,
    index: SparseIndex,
    index_policy: IndexPolicy,
}

/// Où s'arrête un parcours séquentiel du segment.
struct ScanEnd {
    position: u64,
    next_offset: u64,
    /// `None` = fin propre du fichier ; sinon la raison de l'arrêt.
    damage: Option<String>,
}

impl Segment {
    /// Crée un segment vide, en écrasant d'éventuels restes.
    pub(crate) fn create(dir: &Path, base_offset: u64, index_interval: u64) -> Result<Self> {
        let log_path = log_path(dir, base_offset);
        let log = open_rw(&log_path, true)?;
        log.write_all_at(&header_bytes(), 0)?;
        let index_file = open_rw(&index_path(dir, base_offset), true)?;
        Ok(Self {
            base_offset,
            next_offset: base_offset,
            size: HEADER_SIZE,
            log,
            index_file,
            log_path,
            index: SparseIndex::default(),
            index_policy: IndexPolicy::new(base_offset, index_interval),
        })
    }

    /// Ouvre un segment existant et vérifie sa fin, depuis la dernière entrée
    /// d'index valide. L'index est complété ou reconstruit au besoin.
    ///
    /// Le segment actif (`is_active`) est réparé : on garde le plus long
    /// préfixe d'enregistrements valides et on tronque le reste, qui provient
    /// d'une écriture interrompue. Un segment scellé n'est jamais tronqué :
    /// une anomalie y est une vraie corruption, signalée comme telle.
    pub(crate) fn open(
        dir: &Path,
        base_offset: u64,
        index_interval: u64,
        is_active: bool,
    ) -> Result<Self> {
        let log_path = log_path(dir, base_offset);
        let log = open_rw(&log_path, false)?;
        let len = log.metadata()?.len();

        if len < HEADER_SIZE {
            if is_active {
                // Crash entre la création du fichier et l'écriture de l'en-tête.
                drop(log);
                return Self::create(dir, base_offset, index_interval);
            }
            return Err(corrupt(&log_path, "segment scellé sans en-tête"));
        }
        check_header(&log, &log_path)?;

        let index_file = open_rw(&index_path(dir, base_offset), false)?;
        let mut segment = Self {
            base_offset,
            next_offset: base_offset,
            size: len,
            log,
            index_file,
            log_path,
            index: SparseIndex::default(),
            index_policy: IndexPolicy::new(base_offset, index_interval),
        };
        segment.check_tail(is_active)?;
        Ok(segment)
    }

    pub(crate) fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub(crate) fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.size == HEADER_SIZE
    }

    /// Ajoute des enregistrements déjà encodés, à la suite.
    /// `sizes` donne la taille de chacun, dans l'ordre ; leurs offsets
    /// partent de `next_offset`.
    ///
    /// Le log est écrit avant l'index : une entrée d'index ne pointe jamais
    /// sur un enregistrement pas encore écrit.
    pub(crate) fn append(&mut self, bytes: &[u8], sizes: &[usize]) -> Result<()> {
        debug_assert_eq!(sizes.iter().sum::<usize>(), bytes.len());
        self.log.write_all_at(bytes, self.size)?;

        let mut new_entries = Vec::new();
        let mut position = self.size;
        let mut offset = self.next_offset;
        for &size in sizes {
            if let Some(entry) = self.index_policy.observe(offset, position, size) {
                new_entries.push(entry);
            }
            position += size as u64;
            offset += 1;
        }
        self.size = position;
        self.next_offset = offset;

        if !new_entries.is_empty() {
            let at = (self.index.len() * crate::index::ENTRY_SIZE) as u64;
            self.index_file
                .write_all_at(&SparseIndex::encode(&new_entries), at)?;
            self.index.extend(new_entries);
        }
        Ok(())
    }

    /// Force l'écriture du log et de l'index sur le disque.
    pub(crate) fn sync(&self) -> Result<()> {
        self.log.sync_data()?;
        self.index_file.sync_data()?;
        Ok(())
    }

    pub(crate) fn read(&self, req: &ReadRequest, out: &mut Vec<Record>) -> Result<SegmentRead> {
        let start = self
            .index
            .floor(self.base_offset, req.from)
            .map_or(HEADER_SIZE, |e| e.position());
        read_records(&self.log, &self.log_path, start, self.size, req, out)
    }

    /// Vérifie la fin du segment, la répare s'il est actif, et remet l'index
    /// en accord avec le log.
    ///
    /// Le parcours part de la dernière entrée d'index valide : il ne lit en
    /// général que quelques Kio, et tout le segment si l'index est absent.
    fn check_tail(&mut self, is_active: bool) -> Result<()> {
        let mut raw = Vec::new();
        (&self.index_file).read_to_end(&mut raw)?;
        let parsed = SparseIndex::parse(&raw, HEADER_SIZE, self.size);
        self.index = parsed.index;
        let loaded_entries = self.index.len();

        // Les entrées d'index ont pu survivre à des enregistrements perdus
        // (fichiers différents, aucun ordre garanti sur le disque) : on recule
        // jusqu'à une entrée qui pointe bien sur l'enregistrement attendu.
        while let Some(entry) = self.index.last() {
            if self.entry_is_valid(entry)? {
                break;
            }
            self.index.pop();
        }
        let (start, start_offset) = match self.index.last() {
            Some(e) => (e.position(), e.offset(self.base_offset)),
            None => (HEADER_SIZE, self.base_offset),
        };

        let end = self.rebuild_from(start, start_offset)?;
        if let Some(reason) = end.damage {
            if !is_active {
                return Err(corrupt(&self.log_path, reason));
            }
            // Fin d'une écriture interrompue : on la retire.
            self.log.set_len(end.position)?;
            self.log.sync_data()?;
            self.size = end.position;
        }
        self.next_offset = end.next_offset;

        if !parsed.clean || self.index.len() != loaded_entries {
            self.write_index_file()?;
        }
        Ok(())
    }

    fn entry_is_valid(&self, entry: IndexEntry) -> io::Result<bool> {
        let mut reader = PosReader {
            file: &self.log,
            pos: entry.position(),
            end: self.size,
        };
        Ok(match record::read_record(&mut reader)? {
            ReadOutcome::Record(rec, _) => rec.offset == entry.offset(self.base_offset),
            _ => false,
        })
    }

    /// Parcourt le log depuis `start` (où doit se trouver `start_offset`),
    /// complète l'index en mémoire au fil de l'eau, et renvoie où le parcours
    /// s'est arrêté. L'entrée d'index qui pointe sur `start`, s'il y en a une,
    /// doit déjà être dans l'index.
    fn rebuild_from(&mut self, start: u64, start_offset: u64) -> Result<ScanEnd> {
        let mut reader = BufReader::with_capacity(
            READ_BUFFER_SIZE,
            PosReader {
                file: &self.log,
                pos: start,
                end: self.size,
            },
        );
        let mut position = start;
        let mut expected = start_offset;
        let mut new_entries = Vec::new();
        let policy = &mut self.index_policy;
        policy.reset();

        let damage = loop {
            match record::read_record(&mut reader)? {
                ReadOutcome::Record(rec, size) => {
                    if rec.offset != expected {
                        break Some(format!(
                            "offset {} trouvé à la position {position}, {expected} attendu",
                            rec.offset
                        ));
                    }
                    let entry = policy.observe(rec.offset, position, size);
                    // L'enregistrement de départ a déjà son entrée, si besoin.
                    if let Some(entry) = entry.filter(|_| position != start) {
                        new_entries.push(entry);
                    }
                    position += size as u64;
                    expected += 1;
                }
                ReadOutcome::End => break None,
                ReadOutcome::Incomplete => {
                    break Some(format!("enregistrement incomplet à la position {position}"));
                }
                ReadOutcome::Corrupt(reason) => {
                    break Some(format!("{reason} à la position {position}"));
                }
            }
        };
        self.index.extend(new_entries);
        Ok(ScanEnd {
            position,
            next_offset: expected,
            damage,
        })
    }

    fn write_index_file(&self) -> Result<()> {
        self.index_file.set_len(0)?;
        self.index_file
            .write_all_at(&SparseIndex::encode(self.index.entries()), 0)?;
        Ok(())
    }
}

fn open_rw(path: &Path, truncate: bool) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate)
        .open(path)
}
