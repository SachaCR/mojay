//! Un segment = un fichier `.log` append-only et son index clairsemé `.idx`.
//!
//! Le `.log` commence par un en-tête de 16 octets (magic `MOJAY\0`, version
//! `u16`, 8 octets réservés), suivi des enregistrements les uns après les autres.
//!
//! - [`Segment`] : le côté writer, qui ajoute, répare et indexe.
//! - [`SegmentReader`] : le côté lecteur, en lecture seule, utilisable depuis un
//!   autre processus pendant que le writer écrit.

mod reader;
mod writer;

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub(crate) use reader::SegmentReader;
pub(crate) use writer::Segment;

use crate::error::{Result, corrupt};
use crate::record::{self, ReadOutcome, Record};

pub(crate) const HEADER_SIZE: u64 = 16;
const MAGIC: &[u8; 6] = b"MOJAY\0";
const FORMAT_VERSION: u16 = 1;
const READ_BUFFER_SIZE: usize = 64 * 1024;

pub(crate) fn log_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.log"))
}

pub(crate) fn index_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.idx"))
}

/// Ce qu'une lecture de segment doit renvoyer.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadRequest {
    /// Premier offset voulu (inclus).
    pub(crate) from: u64,
    /// Offset où s'arrêter (exclu) : la limite de lecture, ou la base du segment suivant.
    pub(crate) end: u64,
    /// Nombre d'octets encodés à ne pas dépasser.
    pub(crate) budget: usize,
    /// Prendre le premier enregistrement même s'il dépasse le budget.
    pub(crate) take_one: bool,
}

/// Pourquoi une lecture de segment s'est arrêtée.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentRead {
    /// Plus rien à lire dans ce segment avant `end` : la lecture peut continuer au suivant.
    Exhausted { taken: usize },
    /// Le prochain enregistrement dépasserait le budget : la lecture s'arrête là.
    BudgetReached,
}

/// Lit les enregistrements demandés par `req`, à partir de la position `start`
/// et sans dépasser `size` octets du fichier.
///
/// Au-delà de `req.end`, un autre processus peut être en train d'écrire : un
/// enregistrement illisible n'y est pas une erreur. En deçà, c'en est une.
fn read_records(
    log: &File,
    path: &Path,
    start: u64,
    size: u64,
    req: &ReadRequest,
    out: &mut Vec<Record>,
) -> Result<SegmentRead> {
    let mut reader = BufReader::with_capacity(
        READ_BUFFER_SIZE,
        PosReader {
            file: log,
            pos: start,
            end: size,
        },
    );
    let mut taken = 0;
    let mut first = req.take_one;
    let mut next_offset: Option<u64> = None;
    loop {
        match record::read_record(&mut reader)? {
            ReadOutcome::Record(rec, size) => {
                next_offset = Some(rec.offset + 1);
                if rec.offset < req.from {
                    continue;
                }
                if rec.offset >= req.end {
                    return Ok(SegmentRead::Exhausted { taken });
                }
                if !first && taken + size > req.budget {
                    return Ok(SegmentRead::BudgetReached);
                }
                first = false;
                taken += size;
                out.push(rec);
            }
            ReadOutcome::End => return Ok(SegmentRead::Exhausted { taken }),
            ReadOutcome::Incomplete | ReadOutcome::Corrupt(_) => {
                if next_offset.is_some_and(|next| next >= req.end) {
                    return Ok(SegmentRead::Exhausted { taken });
                }
                return Err(corrupt(
                    path,
                    "enregistrement illisible avant la limite de lecture",
                ));
            }
        }
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
            format!("version de format {version} non supportée"),
        ));
    }
    Ok(())
}
