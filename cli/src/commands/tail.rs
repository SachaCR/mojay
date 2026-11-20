//! `mojay tail` : affiche les messages d'un topic. Lecture seule : aucun
//! groupe, aucun commit, aucun effet sur les consumers.

use std::io::Write;
use std::path::Path;
use std::thread;
use std::time::Duration;

use mojay::{Error, Record, TopicReader};

use super::{CliError, CliResult, open_existing};
use crate::display;

/// Attente entre deux vérifications en mode `--follow`.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(100);
const READ_BATCH_BYTES: usize = 256 * 1024;

#[derive(Debug, clap::Args)]
pub struct Args {
    pub topic: String,
    /// Une seule partition. Par défaut, toutes.
    #[arg(long, short)]
    pub partition: Option<u32>,
    /// Commencer à cet offset (avec --partition).
    #[arg(long, requires = "partition", conflicts_with = "last")]
    pub offset: Option<u64>,
    /// Ne montrer que les N derniers messages de chaque partition.
    #[arg(long, short = 'n')]
    pub last: Option<u64>,
    /// Continuer à afficher les nouveaux messages, jusqu'à Ctrl-C.
    #[arg(long, short)]
    pub follow: bool,
    /// Afficher les valeurs en entier, sans les tronquer.
    #[arg(long)]
    pub full: bool,
}

pub fn run(data: &Path, args: Args, out: &mut impl Write) -> CliResult {
    let db = open_existing(data)?;
    let mut reader = db.reader(&args.topic)?;
    let partitions: Vec<u32> = match args.partition {
        Some(p) if p >= reader.partitions() => {
            return Err(CliError::Message(format!(
                "partition {p} inexistante (le topic en a {})",
                reader.partitions()
            )));
        }
        Some(p) => vec![p],
        None => (0..reader.partitions()).collect(),
    };
    let mut positions = partitions
        .into_iter()
        .map(|p| Ok((p, first_position(&reader, p, &args)?)))
        .collect::<Result<Vec<_>, CliError>>()?;

    loop {
        let mut progressed = false;
        for (partition, position) in &mut positions {
            for record in read_available(&mut reader, *partition, position)? {
                writeln!(out, "{}", line(*partition, &record, args.full))?;
                *position = record.offset + 1;
                progressed = true;
            }
        }
        if !progressed {
            if !args.follow {
                return Ok(());
            }
            out.flush()?;
            thread::sleep(FOLLOW_INTERVAL);
        }
    }
}

fn first_position(reader: &TopicReader, partition: u32, args: &Args) -> Result<u64, CliError> {
    if let Some(offset) = args.offset {
        return Ok(offset);
    }
    let start = reader.start_offset(partition)?;
    Ok(match args.last {
        Some(n) => reader.read_limit(partition)?.saturating_sub(n).max(start),
        None => start,
    })
}

/// Ce qui est lisible à partir de `position`. Si la rétention a supprimé la
/// position entre-temps, on repart du nouveau début.
fn read_available(
    reader: &mut TopicReader,
    partition: u32,
    position: &mut u64,
) -> Result<Vec<Record>, CliError> {
    if *position >= reader.read_limit(partition)? {
        return Ok(Vec::new());
    }
    match reader.read(partition, *position, READ_BATCH_BYTES) {
        Err(Error::OffsetOutOfRange { start, .. }) if *position < start => {
            *position = start;
            Ok(Vec::new())
        }
        other => Ok(other?),
    }
}

/// `partition:offset  horodatage  key=…  value=…  headers…`
fn line(partition: u32, record: &Record, full: bool) -> String {
    let mut line = format!(
        "{partition}:{}  {}  key={}  value={}",
        record.offset,
        display::timestamp(record.timestamp),
        display::optional_bytes(record.key.as_deref(), full),
        display::optional_bytes(record.value.as_deref(), full),
    );
    if !record.headers.is_empty() {
        line.push_str("  ");
        line.push_str(&display::headers(&record.headers, full));
    }
    line
}
