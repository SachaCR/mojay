//! `mojay create-topic` and `mojay topics`.

use std::io::Write;
use std::path::Path;
use std::time::Duration;

use mojay::{Db, FsyncPolicy, LogConfig, Retention, TopicConfig};

use super::{CliResult, open_existing};
use crate::parse;

#[derive(Debug, clap::Args)]
pub struct CreateArgs {
    /// Topic name: letters, digits, ".", "_" and "-".
    pub name: String,
    /// Number of partitions, fixed at creation.
    #[arg(long, short, default_value_t = 1)]
    pub partitions: u32,
    /// Maximum segment size, e.g. 64MiB.
    #[arg(long, value_parser = parse::size, default_value = "64MiB")]
    pub segment_size: u64,
    /// fsync policy: always, never, or an interval such as 100ms.
    #[arg(long, value_parser = parse::fsync, default_value = "always")]
    pub fsync: FsyncPolicy,
    /// Maximum partition size before the oldest segments are deleted, e.g. 10GiB.
    #[arg(long, value_parser = parse::size)]
    pub retention_size: Option<u64>,
    /// Maximum segment age before deletion, e.g. 7d.
    #[arg(long, value_parser = parse::duration)]
    pub retention_time: Option<Duration>,
}

pub fn create(data: &Path, args: CreateArgs, out: &mut impl Write) -> CliResult {
    let db = Db::open(data)?;
    let config = TopicConfig {
        partitions: args.partitions,
        log: LogConfig {
            segment_max_bytes: args.segment_size,
            fsync: args.fsync,
            retention: Retention {
                max_bytes: args.retention_size,
                max_age: args.retention_time,
            },
            ..LogConfig::default()
        },
    };
    db.create_topic(&args.name, config)?;
    writeln!(
        out,
        "topic \"{}\" created ({} partition{})",
        args.name,
        args.partitions,
        if args.partitions > 1 { "s" } else { "" }
    )?;
    Ok(())
}

pub fn list(data: &Path, out: &mut impl Write) -> CliResult {
    for name in open_existing(data)?.list_topics()? {
        writeln!(out, "{name}")?;
    }
    Ok(())
}
