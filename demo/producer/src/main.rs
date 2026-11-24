//! Mojay demo: publishes JSON events to a 3-partition topic until Ctrl-C.
//! Start it before one or more `mojay-demo-consumer`.

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

/// The whole topic stays under about 500 MiB: the limit applies per partition.
const TOPIC_RETENTION_BYTES: u64 = 500 * 1024 * 1024;

/// Segments small enough for retention to delete at a fine grain.
const SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

/// Above this rate, a summary per second replaces one line per event.
const MAX_RATE_SHOWN_PER_EVENT: u32 = 50;

#[derive(Debug, Parser)]
#[command(about = "Publishes JSON events to a Mojay topic until Ctrl-C")]
struct Args {
    /// The data directory, shared with the consumers.
    #[arg(long, short, default_value = "./demo-data")]
    data: PathBuf,
    /// The topic, created with 3 partitions if needed.
    #[arg(long, short, default_value = "demo-events")]
    topic: String,
    /// Number of events per second.
    #[arg(long, short, default_value_t = 5)]
    rate: u32,
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
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
        "Publishing to \"{}\" ({}), {} events/s. Ctrl-C to stop.",
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
        // The key is the customer: their events stay in one partition, in order.
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
            println!("{sent} events sent");
            last_summary = Instant::now();
        }

        next_send += per_event;
        if let Some(wait) = next_send.checked_duration_since(Instant::now()) {
            thread::sleep(wait);
        }
    }

    producer.flush()?;
    println!("\n{sent} events sent. Goodbye.");
    Ok(())
}

/// Creates the demo topic. If it already exists, it is reused as is.
fn create_topic_if_missing(db: &Db, topic: &str) -> Result<(), Error> {
    let config = TopicConfig {
        partitions: PARTITIONS,
        log: LogConfig {
            segment_max_bytes: SEGMENT_BYTES,
            // An fsync every 100 ms is enough for a demo, and allows high rates.
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
            println!("Topic \"{topic}\" created: {PARTITIONS} partitions, 500 MiB max.");
            Ok(())
        }
        Err(Error::TopicExists(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

fn euros(cents: u64) -> String {
    format!("€{}.{:02}", cents / 100, cents % 100)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
