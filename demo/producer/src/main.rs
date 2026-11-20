//! Démo Mojay : publie des événements JSON dans un topic de 3 partitions,
//! jusqu'à Ctrl-C. À lancer avant un ou plusieurs `mojay-demo-consumer`.

mod event;

use std::error::Error as StdError;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use mojay::{Db, Error, FsyncPolicy, LogConfig, Message, Retention, TopicConfig};

use event::EventGenerator;

const PARTITIONS: u32 = 3;
/// Le topic entier ne dépasse pas environ 500 Mio : la limite s'applique par partition.
const TOPIC_RETENTION_BYTES: u64 = 500 * 1024 * 1024;
/// Des segments assez petits pour que la rétention supprime finement.
const SEGMENT_BYTES: u64 = 16 * 1024 * 1024;
/// Au-delà de ce débit, un résumé par seconde remplace une ligne par événement.
const MAX_RATE_SHOWN_PER_EVENT: u32 = 50;

#[derive(Debug, Parser)]
#[command(about = "Publie des événements JSON dans un topic Mojay, jusqu'à Ctrl-C")]
struct Args {
    /// Le dossier de données, partagé avec les consumers.
    #[arg(long, short, default_value = "./demo-data")]
    data: PathBuf,
    /// Le topic, créé au besoin avec 3 partitions.
    #[arg(long, short, default_value = "demo-events")]
    topic: String,
    /// Nombre d'événements par seconde.
    #[arg(long, short, default_value_t = 5)]
    rate: u32,
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("erreur : {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> Result<(), Box<dyn StdError>> {
    let db = Db::open(&args.data)?;
    create_topic_if_missing(&db, &args.topic)?;
    let mut producer = db.producer(&args.topic)?;

    let running = Arc::new(AtomicBool::new(true));
    let stop = Arc::clone(&running);
    ctrlc::set_handler(move || stop.store(false, Ordering::SeqCst))?;

    println!(
        "Publication dans « {} » ({}), {} événements/s. Ctrl-C pour arrêter.",
        args.topic,
        args.data.display(),
        args.rate
    );
    let rate = args.rate.max(1);
    let per_event = Duration::from_secs(1) / rate;
    let mut events = EventGenerator::new(now_millis());
    let mut sent = 0u64;
    let mut last_summary = Instant::now();
    let mut next_send = Instant::now();

    while running.load(Ordering::SeqCst) {
        let event = events.next_event(now_millis());
        // La clé est le client : ses événements restent dans une partition, dans l'ordre.
        let message =
            Message::new(event.customer.clone(), event.to_json()).header("type", event.kind);
        let (partition, offset) = producer.send(&message)?;
        sent += 1;

        if rate <= MAX_RATE_SHOWN_PER_EVENT {
            println!(
                "→ p{partition}:{offset:<6} {:<16} {:<10} {:>9}",
                event.kind,
                event.customer,
                euros(event.amount_cents)
            );
        } else if last_summary.elapsed() >= Duration::from_secs(1) {
            println!("{sent} événements envoyés");
            last_summary = Instant::now();
        }

        next_send += per_event;
        if let Some(wait) = next_send.checked_duration_since(Instant::now()) {
            thread::sleep(wait);
        }
    }

    producer.flush()?;
    println!("\n{sent} événements envoyés. Au revoir.");
    Ok(())
}

/// Crée le topic de la démo. S'il existe déjà, on le réutilise tel quel.
fn create_topic_if_missing(db: &Db, topic: &str) -> Result<(), Error> {
    let config = TopicConfig {
        partitions: PARTITIONS,
        log: LogConfig {
            segment_max_bytes: SEGMENT_BYTES,
            // Un fsync toutes les 100 ms suffit pour une démo, et permet des débits élevés.
            fsync: FsyncPolicy::Interval(Duration::from_millis(100)),
            retention: Retention {
                max_bytes: Some(TOPIC_RETENTION_BYTES / u64::from(PARTITIONS)),
                max_age: None,
            },
            ..LogConfig::default()
        },
    };
    match db.create_topic(topic, config) {
        Ok(()) => {
            println!("Topic « {topic} » créé : {PARTITIONS} partitions, 500 Mio au plus.");
            Ok(())
        }
        Err(Error::TopicExists(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

fn euros(cents: u64) -> String {
    format!("{},{:02} €", cents / 100, cents % 100)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
