//! `mojay dump`: the raw contents of a segment, to eyeball what is
//! stored. Read-only: nothing is ever repaired.

use std::io::Write;
use std::path::{Path, PathBuf};

use mojay::inspect::{SegmentDump, dump_segment};

use super::{CliError, CliResult, open_existing};
use crate::display;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// A `.log` segment file, or a topic name.
    pub target: String,
    /// With a topic: a single partition. Defaults to all.
    #[arg(long, short)]
    pub partition: Option<u32>,
    /// Also show the index entries.
    #[arg(long)]
    pub index: bool,
    /// Show values in full, without truncating them.
    #[arg(long)]
    pub full: bool,
}

pub fn run(data: &Path, args: Args, out: &mut impl Write) -> CliResult {
    for path in segments(data, &args)? {
        write_dump(&dump_segment(&path)?, &args, out)?;
    }
    Ok(())
}

/// The segments to show: the given file, or those of the topic.
fn segments(data: &Path, args: &Args) -> Result<Vec<PathBuf>, CliError> {
    let as_file = Path::new(&args.target);
    if as_file.extension().is_some_and(|e| e == "log") {
        if !as_file.is_file() {
            return Err(CliError::Message(format!(
                "file not found: {}",
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
            "partition does not exist (the topic has {})",
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
        .map_or_else(|| "unknown".to_string(), |b| b.to_string());
    writeln!(
        out,
        "== {} ({}, base {base})",
        dump.path.display(),
        display::size(dump.size_bytes)
    )?;
    match &dump.header {
        Ok(version) => writeln!(out, "valid header, format v{version}")?,
        Err(reason) => {
            writeln!(out, "!! invalid header: {reason}")?;
            return Ok(());
        }
    }

    writeln!(
        out,
        "{:>10} {:>10} {:>7}  {:<24}  contents",
        "position", "offset", "size", "timestamp"
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
    writeln!(out, "{} readable record(s)", dump.records.len())?;
    if let Some(tail) = &dump.unreadable_tail {
        writeln!(
            out,
            "!! unreadable tail at position {}: {} ({})",
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
        writeln!(out, "index: absent")?;
        return Ok(());
    };
    writeln!(out, "index: {} entry(ies)", index.entries.len())?;
    for entry in &index.entries {
        let offset = match dump.base_offset {
            Some(base) => (base + u64::from(entry.relative_offset)).to_string(),
            None => format!("+{}", entry.relative_offset),
        };
        let note = if entry.valid {
            ""
        } else {
            "  (inconsistent, ignored)"
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
            "  !! {} extra byte(s) at the end",
            index.leftover_bytes
        )?;
    }
    Ok(())
}
