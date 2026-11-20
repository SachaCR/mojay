//! Tests de la rétention : suppression des plus vieux segments par taille et
//! par âge, et comportement des lecteurs et consumers en retard.

#[allow(dead_code)]
mod common;

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime};

use common::{message, new_db};
use mojay::{ConsumerConfig, Error, FsyncPolicy, LogConfig, PartitionLog, Retention, TopicConfig};
use tempfile::TempDir;

const ALL: usize = usize::MAX;
const HOUR: Duration = Duration::from_secs(3600);
/// Au-delà du délai (500 ms) après lequel un lecteur remarque une suppression.
const READER_NOTICES: Duration = Duration::from_millis(700);

fn config(retention: Retention) -> LogConfig {
    LogConfig {
        segment_max_bytes: 400,
        index_interval_bytes: 64,
        fsync: FsyncPolicy::Never,
        retention,
    }
}

fn by_size(max_bytes: u64) -> Retention {
    Retention {
        max_bytes: Some(max_bytes),
        max_age: None,
    }
}

fn by_age(max_age: Duration) -> Retention {
    Retention {
        max_bytes: None,
        max_age: Some(max_age),
    }
}

fn files_with_extension(dir: &Path, extension: &str) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == extension))
        .collect();
    files.sort();
    files
}

fn total_size(files: &[PathBuf]) -> u64 {
    files.iter().map(|f| fs::metadata(f).unwrap().len()).sum()
}

/// Vieillit artificiellement un fichier, comme s'il n'avait pas été écrit depuis `age`.
fn make_older(path: &Path, age: Duration) {
    let file = File::options().write(true).open(path).unwrap();
    file.set_modified(SystemTime::now() - age).unwrap();
}

fn fill(log: &mut PartitionLog, count: u64) {
    let first = log.next_offset();
    for i in first..first + count {
        log.append(&message(i)).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Log d'une partition
// ---------------------------------------------------------------------------

#[test]
fn without_retention_nothing_is_ever_deleted() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), config(Retention::default())).unwrap();
    fill(&mut log, 100);
    for file in files_with_extension(dir.path(), "log") {
        make_older(&file, HOUR * 1000);
    }
    assert_eq!(log.enforce_retention().unwrap(), 0);
    assert_eq!(log.start_offset(), 0);
}

#[test]
fn size_limit_deletes_the_oldest_segments() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), config(by_size(2000))).unwrap();
    for i in 0..200 {
        log.append(&message(i)).unwrap();
        // La limite tient après chaque ajout, pas seulement à chaque segment.
        let logs = files_with_extension(dir.path(), "log");
        assert!(
            logs.len() == 1 || total_size(&logs) <= 2000,
            "après l'offset {i}"
        );
    }

    let logs = files_with_extension(dir.path(), "log");
    assert!(
        log.start_offset() > 0,
        "des segments doivent avoir été supprimés"
    );
    assert_eq!(log.next_offset(), 200);

    // Chaque index a disparu avec son segment.
    assert_eq!(files_with_extension(dir.path(), "idx").len(), logs.len());
    let start = log.start_offset();
    assert_eq!(log.read(start, ALL).unwrap()[0].offset, start);
    assert!(matches!(
        log.read(start - 1, ALL),
        Err(Error::OffsetOutOfRange { .. })
    ));

    // Rouvert, le log commence au même endroit et accepte de nouvelles écritures.
    drop(log);
    let mut log = PartitionLog::open(dir.path(), config(by_size(2000))).unwrap();
    assert_eq!(log.start_offset(), start);
    assert_eq!(log.append(&message(200)).unwrap(), 200);
}

#[test]
fn active_segment_is_never_deleted() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), config(by_size(0))).unwrap();
    fill(&mut log, 100);
    assert_eq!(files_with_extension(dir.path(), "log").len(), 1);
    let start = log.start_offset();
    assert_eq!(log.read(start, ALL).unwrap().len() as u64, 100 - start);
}

#[test]
fn age_limit_deletes_segments_not_written_for_too_long() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), config(by_age(HOUR))).unwrap();
    fill(&mut log, 100);
    let logs = files_with_extension(dir.path(), "log");
    assert!(logs.len() > 3);
    assert_eq!(
        log.enforce_retention().unwrap(),
        0,
        "rien n'est encore vieux"
    );

    // Les deux plus anciens n'ont pas été écrits depuis deux heures.
    make_older(&logs[0], HOUR * 2);
    make_older(&logs[1], HOUR * 2);
    assert_eq!(log.enforce_retention().unwrap(), 2);
    assert!(!logs[0].exists() && !logs[1].exists());
    assert!(logs[2].exists());
}

#[test]
fn old_active_segment_is_kept() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), config(by_age(HOUR))).unwrap();
    fill(&mut log, 1);
    let active = files_with_extension(dir.path(), "log").pop().unwrap();
    make_older(&active, HOUR * 2);
    assert_eq!(log.enforce_retention().unwrap(), 0);
    assert!(active.exists());
}

#[test]
fn orphan_index_left_by_an_interrupted_deletion_is_removed() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), config(Retention::default())).unwrap();
    fill(&mut log, 10);
    drop(log);
    let orphan = dir.path().join("00000000000000000999.idx");
    fs::write(&orphan, [0u8; 8]).unwrap();

    PartitionLog::open(dir.path(), config(Retention::default())).unwrap();
    assert!(!orphan.exists());
}

// ---------------------------------------------------------------------------
// Topics, lecteurs et consumers
// ---------------------------------------------------------------------------

fn topic_config(retention: Retention) -> TopicConfig {
    TopicConfig {
        partitions: 1,
        log: config(retention),
    }
}

#[test]
fn retention_is_kept_in_the_topic_metadata() {
    let (dir, db) = new_db();
    db.create_topic("stream", topic_config(by_size(2000)))
        .unwrap();
    let meta = fs::read_to_string(dir.path().join("topics/stream/meta.toml")).unwrap();
    assert!(meta.contains("retention_bytes = 2000"), "{meta}");

    // Un producteur rouvert applique la rétention lue dans meta.toml.
    let mut producer = db.producer("stream").unwrap();
    for i in 0..200 {
        producer.send_to(0, &message(i)).unwrap();
    }
    assert!(db.reader("stream").unwrap().start_offset(0).unwrap() > 0);
}

#[test]
fn producer_applies_the_age_limit_on_demand() {
    let (dir, db) = new_db();
    db.create_topic("stream", topic_config(by_age(HOUR)))
        .unwrap();
    let mut producer = db.producer("stream").unwrap();
    for i in 0..100 {
        producer.send_to(0, &message(i)).unwrap();
    }
    let logs = files_with_extension(&dir.path().join("topics/stream/p0"), "log");
    for sealed in &logs[..logs.len() - 1] {
        make_older(sealed, HOUR * 2);
    }
    assert_eq!(producer.enforce_retention().unwrap(), logs.len() - 1);
}

#[test]
fn reader_notices_deleted_segments_and_reports_the_new_start() {
    let (_dir, db) = new_db();
    db.create_topic("stream", topic_config(by_size(2000)))
        .unwrap();
    let mut reader = db.reader("stream").unwrap();
    let mut producer = db.producer("stream").unwrap();
    for i in 0..10 {
        producer.send_to(0, &message(i)).unwrap();
    }
    assert_eq!(reader.read(0, 0, ALL).unwrap()[0].offset, 0);

    for i in 10..300 {
        producer.send_to(0, &message(i)).unwrap();
    }
    thread::sleep(READER_NOTICES);
    let err = reader.read(0, 0, ALL).unwrap_err();
    let Error::OffsetOutOfRange { start, .. } = err else {
        panic!("OffsetOutOfRange attendu, obtenu {err:?}");
    };
    assert!(start > 0);
    assert_eq!(reader.start_offset(0).unwrap(), start);
    assert_eq!(reader.read(0, start, ALL).unwrap()[0].offset, start);
}

/// Un consumer dont la position a été supprimée repart du nouveau début
/// (politique `Earliest`), sans erreur et sans trou ensuite.
#[test]
fn consumer_left_behind_restarts_at_the_new_start() {
    let (_dir, db) = new_db();
    db.create_topic("stream", topic_config(by_size(2000)))
        .unwrap();
    let mut consumer = db
        .consumer("slow", &["stream"], ConsumerConfig::default())
        .unwrap();
    assert_eq!(consumer.position("stream", 0), Some(0));

    let mut producer = db.producer("stream").unwrap();
    for i in 0..300 {
        producer.send_to(0, &message(i)).unwrap();
    }
    thread::sleep(READER_NOTICES);
    let start = db.reader("stream").unwrap().start_offset(0).unwrap();
    assert!(start > 0);

    let mut next = None;
    while next != Some(300) {
        for record in consumer.poll(Duration::from_millis(100)).unwrap() {
            let expected = next.unwrap_or(start);
            assert_eq!(record.offset, expected, "lu dans l'ordre, sans trou");
            next = Some(record.offset + 1);
        }
    }
}
