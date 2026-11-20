//! Mojay demo: a member of the `demo-loggers` group that prints the messages
//! it receives. Start 1 to 3 of them to see the partitions being shared out.

mod format;

use std::error::Error as StdError;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use clap::Parser;
use mojay::{Consumer, ConsumerConfig, Db, Error, RebalanceEvent, StartFrom};

const POLL_TIMEOUT: Duration = Duration::from_millis(500);
const TOPIC_WAIT: Duration = Duration::from_secs(1);

#[derive(Debug, Parser)]
#[command(about = "Prints the messages of a Mojay topic, as a member of a group")]
struct Args {
    /// The data directory, shared with the producer.
    #[arg(long, short, default_value = "./demo-data")]
    data: PathBuf,
    #[arg(long, short, default_value = "demo-events")]
    topic: String,
    /// The group: members of the same group share the partitions.
    #[arg(long, short, default_value = "demo-loggers")]
    group: String,
    /// With no committed offset, read only new messages.
    #[arg(long)]
    latest: bool,
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
    let running = Arc::new(AtomicBool::new(true));
    let stop = Arc::clone(&running);
    ctrlc::set_handler(move || stop.store(false, Ordering::SeqCst))?;

    let me = format!("reader {}", std::process::id());
    let config = ConsumerConfig {
        start_from: if args.latest {
            StartFrom::Latest
        } else {
            StartFrom::Earliest
        },
        // Automatic commit: a member that leaves or gives up a partition hands
        // it back exactly where it stopped.
        auto_commit: Some(Duration::from_secs(1)),
        ..ConsumerConfig::default()
    };
    let Some(mut consumer) = join_group(&args, config, &running)? else {
        return Ok(());
    };
    println!(
        "[{me}] member of group \"{}\" on \"{}\". Ctrl-C to leave.",
        args.group, args.topic
    );

    while running.load(Ordering::SeqCst) {
        let records = consumer.poll(POLL_TIMEOUT)?;
        report_rebalance(&me, &mut consumer);
        for record in records {
            println!(
                "[{me}] {}",
                format::record_line(record.partition, record.offset, record.value.as_deref())
            );
        }
    }

    // Commits, releases the partitions and leaves the group: the other members
    // take them over on the next round, with no duplicates.
    drop(consumer);
    println!("\n[{me}] left: my partitions are back with the group.");
    Ok(())
}

/// Joins the group, waiting for the producer to create the topic.
/// Returns `None` if the user stops before then.
fn join_group(
    args: &Args,
    config: ConsumerConfig,
    running: &AtomicBool,
) -> Result<Option<Consumer>, Error> {
    let db = Db::open(&args.data)?;
    let mut announced = false;
    while running.load(Ordering::SeqCst) {
        match db.consumer(&args.group, &[args.topic.as_str()], config.clone()) {
            Ok(consumer) => return Ok(Some(consumer)),
            Err(Error::UnknownTopic(_)) => {
                if !announced {
                    println!("Waiting for topic \"{}\": start the producer.", args.topic);
                    announced = true;
                }
                thread::sleep(TOPIC_WAIT);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

fn report_rebalance(me: &str, consumer: &mut Consumer) {
    for event in consumer.take_rebalance_events() {
        match event {
            RebalanceEvent::Assigned { partition, .. } => {
                println!("[{me}] + received partition {partition}");
            }
            RebalanceEvent::Revoked { partition, .. } => {
                println!("[{me}] - gave partition {partition} to another member");
            }
        }
    }
}
