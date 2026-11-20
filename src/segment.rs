//! Un segment = un fichier `.log` append-only et son index clairsemé `.idx`.
//!
//! - `.log` : un en-tête de 16 octets (magic `MOJAY\0`, version `u16`,
//!   8 octets réservés), puis les enregistrements les uns après les autres.
//! - `.idx` : des paires `(offset relatif u32, position u32)` en little-endian,
//!   une tous les `index_interval_bytes` écrits. L'index peut toujours être
//!   reconstruit depuis le `.log` : il n'est jamais la source de vérité.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::record::{self, ReadOutcome, Record};

pub(crate) const HEADER_SIZE: u64 = 16;
const MAGIC: &[u8; 6] = b"MOJAY\0";
const FORMAT_VERSION: u16 = 1;
const INDEX_ENTRY_SIZE: u64 = 8;
const READ_BUFFER_SIZE: usize = 64 * 1024;

pub(crate) fn log_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.log"))
}

pub(crate) fn index_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.idx"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexEntry {
    relative_offset: u32,
    position: u32,
}

#[derive(Debug)]
pub(crate) struct Segment {
    base_offset: u64,
    /// Offset du prochain enregistrement. Exact pour le segment actif ;
    /// pour un segment scellé, c'est le log qui connaît la borne (base du suivant).
    next_offset: u64,
    /// Nombre d'octets valides du `.log`, en-tête compris.
    size: u64,
    log: File,
    index: File,
    log_path: PathBuf,
    entries: Vec<IndexEntry>,
    index_policy: IndexPolicy,
}

/// Règle d'indexation : une entrée dès que `interval` octets ont été écrits
/// depuis la précédente. Logique pure, sans I/O.
#[derive(Debug)]
struct IndexPolicy {
    base_offset: u64,
    interval: u64,
    bytes_since_entry: u64,
}

impl IndexPolicy {
    fn new(base_offset: u64, interval: u64) -> Self {
        Self {
            base_offset,
            interval,
            bytes_since_entry: 0,
        }
    }

    fn reset(&mut self) {
        self.bytes_since_entry = 0;
    }

    /// Prend en compte un enregistrement de `size` octets écrit à `position`,
    /// et renvoie l'entrée d'index qui doit pointer sur lui, s'il en faut une.
    fn observe(&mut self, offset: u64, position: u64, size: usize) -> Option<IndexEntry> {
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

/// Pourquoi une lecture de segment s'est arrêtée.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentRead {
    /// Plus rien à lire dans ce segment : la lecture peut continuer au suivant.
    Exhausted { taken: usize },
    /// Le prochain enregistrement dépasserait le budget : la lecture s'arrête là.
    BudgetReached { taken: usize },
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
        let index = open_rw(&index_path(dir, base_offset), true)?;
        Ok(Self {
            base_offset,
            next_offset: base_offset,
            size: HEADER_SIZE,
            log,
            index,
            log_path,
            entries: Vec::new(),
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
                log.set_len(0)?;
                drop(log);
                return Self::create(dir, base_offset, index_interval);
            }
            return Err(corrupt(&log_path, "segment scellé sans en-tête"));
        }
        check_header(&log, &log_path)?;

        let index = open_rw(&index_path(dir, base_offset), false)?;
        let mut segment = Self {
            base_offset,
            next_offset: base_offset,
            size: len,
            log,
            index,
            log_path,
            entries: Vec::new(),
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
            let mut buf = Vec::with_capacity(new_entries.len() * INDEX_ENTRY_SIZE as usize);
            for e in &new_entries {
                buf.extend_from_slice(&e.relative_offset.to_le_bytes());
                buf.extend_from_slice(&e.position.to_le_bytes());
            }
            let at = self.entries.len() as u64 * INDEX_ENTRY_SIZE;
            self.index.write_all_at(&buf, at)?;
            self.entries.extend(new_entries);
        }
        Ok(())
    }

    /// Force l'écriture du log et de l'index sur le disque.
    pub(crate) fn sync(&self) -> Result<()> {
        self.log.sync_data()?;
        self.index.sync_data()?;
        Ok(())
    }

    /// Lit les enregistrements à partir de `from` (inclus), jusqu'à `end`
    /// (exclus), en s'arrêtant avant de dépasser `budget` octets.
    /// Si `take_one` est vrai, le premier enregistrement est pris même s'il
    /// dépasse le budget.
    pub(crate) fn read(
        &self,
        from: u64,
        end: u64,
        budget: usize,
        take_one: bool,
        out: &mut Vec<Record>,
    ) -> Result<SegmentRead> {
        let start = self.lookup(from);
        let mut reader = BufReader::with_capacity(
            READ_BUFFER_SIZE,
            PosReader {
                file: &self.log,
                pos: start,
                end: self.size,
            },
        );
        let mut taken = 0;
        let mut first = take_one;
        loop {
            match record::read_record(&mut reader)? {
                ReadOutcome::Record(rec, size) => {
                    if rec.offset < from {
                        continue;
                    }
                    if rec.offset >= end {
                        return Ok(SegmentRead::Exhausted { taken });
                    }
                    if !first && taken + size > budget {
                        return Ok(SegmentRead::BudgetReached { taken });
                    }
                    first = false;
                    taken += size;
                    out.push(rec);
                }
                ReadOutcome::End => return Ok(SegmentRead::Exhausted { taken }),
                ReadOutcome::Incomplete | ReadOutcome::Corrupt(_) => {
                    return Err(corrupt(
                        &self.log_path,
                        "enregistrement illisible dans la zone valide",
                    ));
                }
            }
        }
    }

    /// Position à partir de laquelle chercher `offset` : la dernière entrée
    /// d'index dont l'offset est inférieur ou égal.
    fn lookup(&self, offset: u64) -> u64 {
        let relative = offset.saturating_sub(self.base_offset);
        let idx = self
            .entries
            .partition_point(|e| u64::from(e.relative_offset) <= relative);
        match idx {
            0 => HEADER_SIZE,
            i => u64::from(self.entries[i - 1].position),
        }
    }

    /// Charge l'index du disque en ne gardant que les entrées cohérentes.
    /// Renvoie `false` si le fichier contenait des octets à écarter.
    fn load_index(&mut self) -> Result<bool> {
        let mut raw = Vec::new();
        (&self.index).read_to_end(&mut raw)?;
        let (chunks, leftover) = raw.as_chunks::<{ INDEX_ENTRY_SIZE as usize }>();
        let mut clean = leftover.is_empty();
        let mut prev: Option<IndexEntry> = None;
        for &[r0, r1, r2, r3, p0, p1, p2, p3] in chunks {
            let entry = IndexEntry {
                relative_offset: u32::from_le_bytes([r0, r1, r2, r3]),
                position: u32::from_le_bytes([p0, p1, p2, p3]),
            };
            let in_bounds =
                u64::from(entry.position) >= HEADER_SIZE && u64::from(entry.position) < self.size;
            let increasing = prev.is_none_or(|p| {
                entry.relative_offset > p.relative_offset && entry.position > p.position
            });
            if !in_bounds || !increasing {
                clean = false;
                break;
            }
            self.entries.push(entry);
            prev = Some(entry);
        }
        Ok(clean)
    }

    /// Vérifie la fin du segment, la répare s'il est actif, et remet l'index
    /// en accord avec le log.
    ///
    /// Le parcours part de la dernière entrée d'index valide : il ne lit en
    /// général que quelques Kio, et tout le segment si l'index est absent.
    fn check_tail(&mut self, is_active: bool) -> Result<()> {
        let index_clean = self.load_index()?;
        let loaded_entries = self.entries.len();

        // Les entrées d'index ont pu survivre à des enregistrements perdus
        // (fichiers différents, aucun ordre garanti sur le disque) : on recule
        // jusqu'à une entrée qui pointe bien sur l'enregistrement attendu.
        while let Some(&entry) = self.entries.last() {
            if self.entry_is_valid(entry)? {
                break;
            }
            self.entries.pop();
        }
        let (start, start_offset) = match self.entries.last() {
            Some(e) => (
                u64::from(e.position),
                self.base_offset + u64::from(e.relative_offset),
            ),
            None => (HEADER_SIZE, self.base_offset),
        };

        let end = self.rebuild_from(start, start_offset)?;
        if let Some(reason) = end.damage {
            if !is_active {
                return Err(corrupt(&self.log_path, &reason));
            }
            // Fin d'une écriture interrompue : on la retire.
            self.log.set_len(end.position)?;
            self.log.sync_data()?;
            self.size = end.position;
        }
        self.next_offset = end.next_offset;

        if !index_clean || self.entries.len() != loaded_entries {
            self.write_index_file()?;
        }
        Ok(())
    }

    fn entry_is_valid(&self, entry: IndexEntry) -> io::Result<bool> {
        let mut reader = PosReader {
            file: &self.log,
            pos: u64::from(entry.position),
            end: self.size,
        };
        Ok(match record::read_record(&mut reader)? {
            ReadOutcome::Record(rec, _) => {
                rec.offset == self.base_offset + u64::from(entry.relative_offset)
            }
            _ => false,
        })
    }

    /// Parcourt le log depuis `start` (où doit se trouver `start_offset`),
    /// complète l'index en mémoire au fil de l'eau, et renvoie où le parcours
    /// s'est arrêté. L'entrée d'index qui pointe sur `start`, s'il y en a une,
    /// doit déjà être dans `self.entries`.
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
        self.entries.extend(new_entries);
        Ok(ScanEnd {
            position,
            next_offset: expected,
            damage,
        })
    }

    fn write_index_file(&self) -> Result<()> {
        let mut buf = Vec::with_capacity(self.entries.len() * INDEX_ENTRY_SIZE as usize);
        for e in &self.entries {
            buf.extend_from_slice(&e.relative_offset.to_le_bytes());
            buf.extend_from_slice(&e.position.to_le_bytes());
        }
        self.index.set_len(0)?;
        self.index.write_all_at(&buf, 0)?;
        Ok(())
    }
}

/// Lecture positionnelle (`pread`) bornée, sans curseur partagé sur le fichier.
struct PosReader<'a> {
    file: &'a File,
    pos: u64,
    end: u64,
}

impl Read for PosReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.end.saturating_sub(self.pos);
        let max = buf.len().min(remaining.try_into().unwrap_or(usize::MAX));
        if max == 0 {
            return Ok(0);
        }
        let n = self.file.read_at(&mut buf[..max], self.pos)?;
        self.pos += n as u64;
        Ok(n)
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

fn header_bytes() -> [u8; HEADER_SIZE as usize] {
    let mut h = [0u8; HEADER_SIZE as usize];
    h[..6].copy_from_slice(MAGIC);
    h[6..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    h
}

fn check_header(log: &File, path: &Path) -> Result<()> {
    let mut h = [0u8; HEADER_SIZE as usize];
    log.read_exact_at(&mut h, 0)?;
    if &h[..6] != MAGIC {
        return Err(corrupt(
            path,
            "magic invalide, ce n'est pas un segment Mojay",
        ));
    }
    let version = u16::from_le_bytes([h[6], h[7]]);
    if version != FORMAT_VERSION {
        return Err(corrupt(
            path,
            &format!("version de format {version} non supportée"),
        ));
    }
    Ok(())
}

fn corrupt(path: &Path, reason: &str) -> Error {
    Error::Corrupt {
        path: path.to_path_buf(),
        reason: reason.to_string(),
    }
}
