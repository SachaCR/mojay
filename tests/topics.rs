//! Tests des topics : création, writer unique, lecteurs, limites de lecture.
//!
//! Les tests multi-processus relancent ce même binaire de test dans un
//! processus enfant (voir `child_process`), qui joue un scénario donné.

use std::env;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use mojay::{Db, Error, FsyncPolicy, LogConfig, Message, Record, TopicConfig};
use tempfile::TempDir;

const ALL: usize = usize::MAX;
const CHILD_SCENARIO: &str = "MOJAY_TEST_CHILD_SCENARIO";
const CHILD_DIR: &str = "MOJAY_TEST_CHILD_DIR";
const STREAM_LEN: u64 = 3000;

fn message(i: u64) -> Message {
    Message::new(format!("k{i}"), format!("valeur-{i}-{}", "x".repeat(64)))
}

fn assert_is_message(rec: &Record, i: u64) {
    assert_eq!(rec.offset, i);
    assert_eq!(rec.value, message(i).value, "contenu de l'offset {i}");
}

fn config(partitions: u32, segment_max_bytes: u64, fsync: FsyncPolicy) -> TopicConfig {
    TopicConfig {
        partitions,
        log: LogConfig {
            segment_max_bytes,
            index_interval_bytes: 256,
            fsync,
        },
    }
}

fn new_db() -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    (dir, db)
}

/// Attend qu'une condition devienne vraie, ou échoue après `timeout`.
fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "délai dépassé");
        thread::sleep(Duration::from_millis(5));
    }
}

// ---------------------------------------------------------------------------
// Processus enfant
// ---------------------------------------------------------------------------

fn child_command(scenario: &str, dir: &Path) -> Command {
    let mut cmd = Command::new(env::current_exe().unwrap());
    cmd.args([
        "child_process",
        "--exact",
        "--nocapture",
        "--test-threads=1",
    ])
    .env(CHILD_SCENARIO, scenario)
    .env(CHILD_DIR, dir)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    cmd
}

/// Lance un scénario enfant, attend sa fin, et renvoie sa ligne `RESULT:`.
fn run_child(scenario: &str, dir: &Path) -> String {
    let output = child_command(scenario, dir).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "l'enfant « {scenario} » a échoué :\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // La sortie du harnais de test peut précéder le résultat sur la même ligne.
    stdout
        .split("RESULT:")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("pas de résultat dans :\n{stdout}"))
        .to_string()
}

fn spawn_child(scenario: &str, dir: &Path) -> Child {
    child_command(scenario, dir).spawn().unwrap()
}

/// Point d'entrée des processus enfants. Ne fait rien dans un test normal.
#[test]
fn child_process() {
    let Ok(scenario) = env::var(CHILD_SCENARIO) else {
        return;
    };
    let db = Db::open(env::var(CHILD_DIR).unwrap()).unwrap();
    match scenario.as_str() {
        "try_producer" => match db.producer("orders") {
            Ok(_) => println!("RESULT:ok"),
            Err(Error::Busy(_)) => println!("RESULT:busy"),
            Err(e) => panic!("erreur inattendue : {e}"),
        },
        "write_stream" => {
            let mut producer = db.producer("stream").unwrap();
            for i in 0..STREAM_LEN {
                producer.send_to(0, &message(i)).unwrap();
            }
            println!("RESULT:done");
        }
        "write_until_killed" => {
            let mut producer = db.producer("stream").unwrap();
            // Écrit sans fin : le parent tue ce processus en pleine écriture.
            for i in 0..u64::MAX {
                producer.send_to(0, &message(i)).unwrap();
            }
        }
        other => panic!("scénario inconnu : {other}"),
    }
}

// ---------------------------------------------------------------------------
// Création et noms
// ---------------------------------------------------------------------------

#[test]
fn topics_are_created_listed_and_unique() {
    let (_dir, db) = new_db();
    db.create_topic("orders", TopicConfig::default()).unwrap();
    db.create_topic("audit.v2", TopicConfig::default()).unwrap();
    assert_eq!(db.list_topics().unwrap(), vec!["audit.v2", "orders"]);
    assert!(matches!(
        db.create_topic("orders", TopicConfig::default()),
        Err(Error::TopicExists(_))
    ));
}

#[test]
fn unknown_topics_and_bad_names_are_rejected() {
    let (_dir, db) = new_db();
    assert!(matches!(
        db.producer("missing"),
        Err(Error::UnknownTopic(_))
    ));
    assert!(matches!(db.reader("missing"), Err(Error::UnknownTopic(_))));
    assert!(matches!(
        db.create_topic("../evil", TopicConfig::default()),
        Err(Error::InvalidName { .. })
    ));
    let zero = TopicConfig {
        partitions: 0,
        ..TopicConfig::default()
    };
    assert!(matches!(
        db.create_topic("empty", zero),
        Err(Error::InvalidConfig(_))
    ));
}

#[test]
fn data_folder_from_an_unknown_format_is_refused() {
    let dir = TempDir::new().unwrap();
    Db::open(dir.path()).unwrap();
    fs::write(dir.path().join("mojay.toml"), "format_version = 99\n").unwrap();
    assert!(matches!(Db::open(dir.path()), Err(Error::Corrupt { .. })));
}

// ---------------------------------------------------------------------------
// Production et lecture
// ---------------------------------------------------------------------------

#[test]
fn messages_with_the_same_key_stay_together_and_in_order() {
    let (_dir, db) = new_db();
    db.create_topic("orders", config(4, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    let mut placed = Vec::new();
    for i in 0..20 {
        let key = format!("client-{}", i % 3);
        let msg = Message::new(key.clone(), i.to_string());
        placed.push((key, producer.send(&msg).unwrap()));
    }

    let mut reader = db.reader("orders").unwrap();
    for (key, (partition, offset)) in &placed {
        let same_key: Vec<_> = placed.iter().filter(|(k, _)| k == key).collect();
        assert!(same_key.iter().all(|(_, (p, _))| p == partition));
        let rec = &reader.read(*partition, *offset, ALL).unwrap()[0];
        assert_eq!(rec.key.as_deref(), Some(key.as_bytes()));
    }
}

#[test]
fn reader_follows_new_messages_and_segments_without_reopening() {
    let (_dir, db) = new_db();
    db.create_topic("stream", config(1, 2048, FsyncPolicy::Never))
        .unwrap();
    let mut reader = db.reader("stream").unwrap();
    assert_eq!(reader.read_limit(0).unwrap(), 0);
    assert!(reader.read(0, 0, ALL).unwrap().is_empty());

    let mut producer = db.producer("stream").unwrap();
    let mut next = 0;
    for batch in 0..10 {
        for i in batch * 30..(batch + 1) * 30 {
            producer.send_to(0, &message(i)).unwrap();
        }
        // Le lecteur, ouvert avant, voit les nouveaux messages et segments.
        while next < reader.read_limit(0).unwrap() {
            for rec in reader.read(0, next, 4096).unwrap() {
                assert_is_message(&rec, next);
                next += 1;
            }
        }
        assert_eq!(next, (batch + 1) * 30);
    }
}

#[test]
fn unknown_partition_is_reported() {
    let (_dir, db) = new_db();
    db.create_topic("orders", config(2, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    assert!(matches!(
        producer.send_to(2, &message(0)),
        Err(Error::UnknownPartition {
            partition: 2,
            partitions: 2
        })
    ));
    let mut reader = db.reader("orders").unwrap();
    assert!(matches!(
        reader.read(5, 0, ALL),
        Err(Error::UnknownPartition { .. })
    ));
}

// ---------------------------------------------------------------------------
// Limite de lecture
// ---------------------------------------------------------------------------

#[test]
fn reader_never_reads_past_the_read_limit() {
    let (dir, db) = new_db();
    db.create_topic("orders", config(1, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    for i in 0..10 {
        producer.send_to(0, &message(i)).unwrap();
    }
    drop(producer);

    // On simule un writer qui n'a publié que les 4 premiers messages.
    let limits = dir.path().join("topics/orders/read_limits");
    let file = OpenOptions::new().write(true).open(limits).unwrap();
    file.write_all_at(&4u64.to_le_bytes(), 0).unwrap();

    let mut reader = db.reader("orders").unwrap();
    assert_eq!(reader.read_limit(0).unwrap(), 4);
    let records = reader.read(0, 0, ALL).unwrap();
    assert_eq!(records.len(), 4);
    assert!(matches!(
        reader.read(0, 5, ALL),
        Err(Error::OffsetOutOfRange { end: 4, .. })
    ));
}

#[test]
fn read_limit_follows_the_log_after_recovery() {
    let (dir, db) = new_db();
    db.create_topic("orders", config(1, 1 << 20, FsyncPolicy::Always))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    for i in 0..5 {
        producer.send_to(0, &message(i)).unwrap();
    }
    drop(producer);

    // Fin d'écriture interrompue : des octets en trop dans le segment actif.
    let segment = dir.path().join("topics/orders/p0/00000000000000000000.log");
    let mut bytes = fs::read(&segment).unwrap();
    bytes.extend_from_slice(&[0xAB; 20]);
    fs::write(&segment, bytes).unwrap();

    let _producer = db.producer("orders").unwrap();
    let mut reader = db.reader("orders").unwrap();
    assert_eq!(reader.read_limit(0).unwrap(), 5);
    assert_eq!(reader.read(0, 0, ALL).unwrap().len(), 5);
}

// ---------------------------------------------------------------------------
// Writer unique
// ---------------------------------------------------------------------------

#[test]
fn second_producer_in_the_same_process_is_busy() {
    let (_dir, db) = new_db();
    db.create_topic("orders", TopicConfig::default()).unwrap();
    let first = db.producer("orders").unwrap();
    assert!(matches!(db.producer("orders"), Err(Error::Busy(_))));
    drop(first);
    db.producer("orders").unwrap();
}

#[test]
fn second_producer_in_another_process_is_busy() {
    let (dir, db) = new_db();
    db.create_topic("orders", TopicConfig::default()).unwrap();

    let producer = db.producer("orders").unwrap();
    assert_eq!(run_child("try_producer", dir.path()), "busy");
    drop(producer);
    assert_eq!(run_child("try_producer", dir.path()), "ok");
}

/// Un writer tué en pleine écriture libère le verrou, et tout ce qui avait été
/// rendu lisible (avec `Always`) est encore là après réparation.
#[test]
fn killed_writer_releases_the_lock_and_keeps_published_messages() {
    let (dir, db) = new_db();
    db.create_topic("stream", config(1, 4096, FsyncPolicy::Always))
        .unwrap();
    let mut child = spawn_child("write_until_killed", dir.path());

    let reader = db.reader("stream").unwrap();
    wait_until(Duration::from_secs(30), || {
        reader.read_limit(0).unwrap() >= 200
    });
    child.kill().unwrap();
    child.wait().unwrap();
    let published_before_kill = reader.read_limit(0).unwrap();

    let _producer = db.producer("stream").expect("le verrou doit être libéré");
    let mut reader = db.reader("stream").unwrap();
    let recovered = reader.read_limit(0).unwrap();
    assert!(recovered >= published_before_kill);
    let mut next = 0;
    while next < recovered {
        for rec in reader.read(0, next, 64 * 1024).unwrap() {
            assert_is_message(&rec, next);
            next += 1;
        }
    }
}

/// Un lecteur suit un writer d'un autre processus, segment après segment, et
/// ne voit jamais un message incomplet.
#[test]
fn reader_follows_a_writer_in_another_process() {
    let (dir, db) = new_db();
    db.create_topic("stream", config(1, 4096, FsyncPolicy::Never))
        .unwrap();
    let mut reader = db.reader("stream").unwrap();
    let child = spawn_child("write_stream", dir.path());

    let mut next = 0;
    wait_until(Duration::from_secs(60), || {
        for rec in reader.read(0, next, 8 * 1024).unwrap() {
            assert_is_message(&rec, next);
            next += 1;
        }
        next == STREAM_LEN
    });
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
}

// ---------------------------------------------------------------------------
// fsync périodique
// ---------------------------------------------------------------------------

#[test]
fn interval_policy_runs_in_background_and_stops_cleanly() {
    let (_dir, db) = new_db();
    let interval = FsyncPolicy::Interval(Duration::from_millis(5));
    db.create_topic("orders", config(2, 1 << 20, interval))
        .unwrap();
    let mut producer = db.producer("orders").unwrap();
    for i in 0..50 {
        producer.send_to((i % 2) as u32, &message(i)).unwrap();
        if i % 10 == 0 {
            thread::sleep(Duration::from_millis(10));
        }
    }
    producer.flush().unwrap();
    drop(producer);

    let mut reader = db.reader("orders").unwrap();
    assert_eq!(reader.read(0, 0, ALL).unwrap().len(), 25);
    assert_eq!(reader.read(1, 0, ALL).unwrap().len(), 25);
}
