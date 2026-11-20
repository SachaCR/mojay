//! `mojay dump` : le contenu brut d'un segment, pour vérifier à l'œil ce qui
//! est stocké. Lecture seule : rien n'est jamais réparé.

use std::io::Write;
use std::path::{Path, PathBuf};

use mojay::inspect::{SegmentDump, dump_segment};

use super::{CliError, CliResult, open_existing};
use crate::display;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Un fichier de segment `.log`, ou un nom de topic.
    pub target: String,
    /// Avec un topic : une seule partition. Par défaut, toutes.
    #[arg(long, short)]
    pub partition: Option<u32>,
    /// Afficher aussi les entrées de l'index.
    #[arg(long)]
    pub index: bool,
    /// Afficher les valeurs en entier, sans les tronquer.
    #[arg(long)]
    pub full: bool,
}

pub fn run(data: &Path, args: Args, out: &mut impl Write) -> CliResult {
    for path in segments(data, &args)? {
        write_dump(&dump_segment(&path)?, &args, out)?;
    }
    Ok(())
}

/// Les segments à afficher : le fichier donné, ou ceux du topic.
fn segments(data: &Path, args: &Args) -> Result<Vec<PathBuf>, CliError> {
    let as_file = Path::new(&args.target);
    if as_file.extension().is_some_and(|e| e == "log") {
        if !as_file.is_file() {
            return Err(CliError::Message(format!(
                "fichier introuvable : {}",
                args.target
            )));
        }
        return Ok(vec![as_file.to_path_buf()]);
    }
    let description = open_existing(data)?.describe_topic(&args.target)?;
    let partitions = description
        .partitions
        .iter()
        .filter(|p| args.partition.is_none_or(|wanted| p.partition == wanted))
        .collect::<Vec<_>>();
    if partitions.is_empty() {
        return Err(CliError::Message(format!(
            "partition inexistante (le topic en a {})",
            description.partitions.len()
        )));
    }
    Ok(partitions
        .into_iter()
        .flat_map(|p| p.segments.iter().map(|s| s.path.clone()))
        .collect())
}

fn write_dump(dump: &SegmentDump, args: &Args, out: &mut impl Write) -> CliResult {
    let base = dump
        .base_offset
        .map_or_else(|| "inconnue".to_string(), |b| b.to_string());
    writeln!(
        out,
        "== {} ({}, base {base})",
        dump.path.display(),
        display::size(dump.size_bytes)
    )?;
    match &dump.header {
        Ok(version) => writeln!(out, "en-tête valide, format v{version}")?,
        Err(reason) => {
            writeln!(out, "!! en-tête invalide : {reason}")?;
            return Ok(());
        }
    }

    writeln!(
        out,
        "{:>10} {:>10} {:>7}  {:<24}  contenu",
        "position", "offset", "taille", "horodatage"
    )?;
    for entry in &dump.records {
        let r = &entry.record;
        write!(
            out,
            "{:>10} {:>10} {:>7}  {:<24}  key={} value={}",
            entry.position,
            r.offset,
            entry.size,
            display::timestamp(r.timestamp),
            display::optional_bytes(r.key.as_deref(), args.full),
            display::optional_bytes(r.value.as_deref(), args.full),
        )?;
        if !r.headers.is_empty() {
            write!(out, " {}", display::headers(&r.headers, args.full))?;
        }
        writeln!(out)?;
    }
    writeln!(out, "{} enregistrement(s) lisible(s)", dump.records.len())?;
    if let Some(tail) = &dump.unreadable_tail {
        writeln!(
            out,
            "!! fin illisible à la position {} : {} ({})",
            tail.position,
            display::size(tail.bytes),
            tail.reason
        )?;
    }

    if args.index {
        write_index(dump, out)?;
    }
    writeln!(out)?;
    Ok(())
}

fn write_index(dump: &SegmentDump, out: &mut impl Write) -> CliResult {
    let Some(index) = &dump.index else {
        writeln!(out, "index : absent")?;
        return Ok(());
    };
    writeln!(out, "index : {} entrée(s)", index.entries.len())?;
    for entry in &index.entries {
        let offset = match dump.base_offset {
            Some(base) => (base + u64::from(entry.relative_offset)).to_string(),
            None => format!("+{}", entry.relative_offset),
        };
        let note = if entry.valid {
            ""
        } else {
            "  (incohérente, ignorée)"
        };
        writeln!(
            out,
            "  offset {offset} -> position {}{note}",
            entry.position
        )?;
    }
    if index.leftover_bytes > 0 {
        writeln!(
            out,
            "  !! {} octet(s) en trop à la fin",
            index.leftover_bytes
        )?;
    }
    Ok(())
}
