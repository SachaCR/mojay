//! `mojay describe`: configuration, partitions, segments and group offsets.

use std::io::Write;
use std::path::Path;

use mojay::{FsyncPolicy, Retention, TopicDescription};

use super::{CliResult, open_existing};
use crate::display;

pub fn run(data: &Path, topic: &str, out: &mut impl Write) -> CliResult {
    let db = open_existing(data)?;
    let description = db.describe_topic(topic)?;
    let committed = db.committed_offsets(topic)?;

    write_config(&description, out)?;

    writeln!(out)?;
    writeln!(
        out,
        "{:<10} {:>12} {:>12} {:>12} {:>9} {:>12}",
        "partition", "start", "read limit", "messages", "segments", "size"
    )?;
    for p in &description.partitions {
        writeln!(
            out,
            "{:<10} {:>12} {:>12} {:>12} {:>9} {:>12}",
            p.partition,
            p.start_offset,
            p.read_limit,
            p.read_limit - p.start_offset,
            p.segments.len(),
            display::size(p.size_bytes()),
        )?;
    }

    writeln!(out)?;
    if committed.is_empty() {
        writeln!(out, "no group has committed an offset on this topic")?;
        return Ok(());
    }
    writeln!(
        out,
        "{:<20} {:<10} {:>12} {:>12}",
        "group", "partition", "committed", "lag"
    )?;
    for c in &committed {
        let limit = description.partitions[c.partition as usize].read_limit;
        writeln!(
            out,
            "{:<20} {:<10} {:>12} {:>12}",
            c.group,
            c.partition,
            c.offset,
            limit.saturating_sub(c.offset)
        )?;
    }
    Ok(())
}

fn write_config(description: &TopicDescription, out: &mut impl Write) -> CliResult {
    let config = &description.config;
    writeln!(out, "topic       {}", description.name)?;
    writeln!(out, "partitions  {}", config.partitions)?;
    writeln!(
        out,
        "segments    {} max",
        display::size(config.log.segment_max_bytes)
    )?;
    let fsync = match config.log.fsync {
        FsyncPolicy::Always => "always".to_string(),
        FsyncPolicy::Never => "never".to_string(),
        FsyncPolicy::Interval(every) => format!("every {}", display::duration(every)),
    };
    writeln!(out, "fsync       {fsync}")?;
    writeln!(out, "retention   {}", retention(&config.log.retention))?;
    Ok(())
}

fn retention(retention: &Retention) -> String {
    let mut limits = Vec::new();
    if let Some(bytes) = retention.max_bytes {
        limits.push(format!("{} per partition", display::size(bytes)));
    }
    if let Some(age) = retention.max_age {
        limits.push(format!("{} per segment", display::duration(age)));
    }
    if limits.is_empty() {
        "none".to_string()
    } else {
        limits.join(", ")
    }
}
