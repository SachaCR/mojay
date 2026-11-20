//! `mojay create-topic` et `mojay topics`.

use std::io::Write;
use std::path::Path;
use std::time::Duration;

use mojay::{Db, FsyncPolicy, LogConfig, Retention, TopicConfig};

use super::{CliResult, open_existing};
use crate::parse;

#[derive(Debug, clap::Args)]
pub struct CreateArgs {
    /// Nom du topic : lettres, chiffres, « . », « _ » et « - ».
    pub name: String,
    /// Nombre de partitions, fixé à la création.
    #[arg(long, short, default_value_t = 1)]
    pub partitions: u32,
    /// Taille maximale d'un segment, par exemple 64Mio.
    #[arg(long, value_parser = parse::size, default_value = "64Mio")]
    pub segment_size: u64,
    /// Politique fsync : always, never, ou un intervalle comme 100ms.
    #[arg(long, value_parser = parse::fsync, default_value = "always")]
    pub fsync: FsyncPolicy,
    /// Taille maximale d'une partition avant suppression des plus vieux segments, par exemple 10Gio.
    #[arg(long, value_parser = parse::size)]
    pub retention_size: Option<u64>,
    /// Âge maximal d'un segment avant suppression, par exemple 7d.
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
        "topic « {} » créé ({} partition{})",
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
