//! Démo Mojay : un membre du groupe `demo-loggers` qui affiche les messages
//! qu'il reçoit. Lancez-en 1 à 3 pour voir les partitions se répartir.

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
#[command(about = "Affiche les messages d'un topic Mojay, en tant que membre d'un groupe")]
struct Args {
    /// Le dossier de données, partagé avec le producteur.
    #[arg(long, short, default_value = "./demo-data")]
    data: PathBuf,
    #[arg(long, short, default_value = "demo-events")]
    topic: String,
    /// Le groupe : les membres d'un même groupe se partagent les partitions.
    #[arg(long, short, default_value = "demo-loggers")]
    group: String,
    /// Sans offset commité, lire seulement les nouveaux messages.
    #[arg(long)]
    latest: bool,
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
    let running = Arc::new(AtomicBool::new(true));
    let stop = Arc::clone(&running);
    ctrlc::set_handler(move || stop.store(false, Ordering::SeqCst))?;

    let me = format!("lecteur {}", std::process::id());
    let config = ConsumerConfig {
        start_from: if args.latest {
            StartFrom::Latest
        } else {
            StartFrom::Earliest
        },
        // Commit automatique : un membre qui part ou cède une partition la
        // rend exactement là où il s'est arrêté.
        auto_commit: Some(Duration::from_secs(1)),
        ..ConsumerConfig::default()
    };
    let Some(mut consumer) = join_group(&args, config, &running)? else {
        return Ok(());
    };
    println!(
        "[{me}] membre du groupe « {} » sur « {} ». Ctrl-C pour partir.",
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

    // Commite, rend les partitions et quitte le groupe : les autres membres
    // les reprennent au prochain tour, sans doublon.
    drop(consumer);
    println!("\n[{me}] parti : mes partitions sont rendues au groupe.");
    Ok(())
}

/// Rejoint le groupe, en attendant que le producteur ait créé le topic.
/// Renvoie `None` si l'utilisateur arrête avant.
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
                    println!(
                        "En attente du topic « {} » : lancez le producteur.",
                        args.topic
                    );
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
                println!("[{me}] + partition {partition} reçue");
            }
            RebalanceEvent::Revoked { partition, .. } => {
                println!("[{me}] - partition {partition} cédée à un autre membre");
            }
        }
    }
}
