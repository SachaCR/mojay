//! Tests de la CLI : le vrai binaire `mojay`, lancé sur un dossier temporaire.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use mojay::{ConsumerConfig, Db};
use tempfile::TempDir;

/// Lance `mojay --data <dir> <args>`, avec `stdin` en entrée.
fn mojay(dir: &Path, args: &[&str], stdin: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mojay"))
        .arg("--data")
        .arg(dir)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

/// Lance une commande qui doit réussir, et renvoie sa sortie.
fn ok(dir: &Path, args: &[&str]) -> String {
    ok_with_input(dir, args, "")
}

fn ok_with_input(dir: &Path, args: &[&str], stdin: &str) -> String {
    let output = mojay(dir, args, stdin);
    assert!(
        output.status.success(),
        "mojay {args:?} a échoué : {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

/// Lance une commande qui doit échouer, et renvoie son message d'erreur.
fn fails(dir: &Path, args: &[&str]) -> String {
    let output = mojay(dir, args, "");
    assert!(!output.status.success(), "mojay {args:?} aurait dû échouer");
    String::from_utf8(output.stderr).unwrap()
}

fn data_with_topic(partitions: &str) -> TempDir {
    let dir = TempDir::new().unwrap();
    ok(dir.path(), &["create-topic", "orders", "-p", partitions]);
    dir
}

fn first_segment(dir: &Path) -> PathBuf {
    dir.join("topics/orders/p0/00000000000000000000.log")
}

#[test]
fn produce_then_tail_shows_every_message() {
    let dir = data_with_topic("1");
    assert_eq!(
        ok(
            dir.path(),
            &["produce", "orders", "bonjour", "-k", "k1", "-H", "src=web"]
        ),
        "0:0\n"
    );
    ok_with_input(dir.path(), &["produce", "orders", "-q"], "deux\ntrois\n");

    let tail = ok(dir.path(), &["tail", "orders"]);
    let lines: Vec<_> = tail.lines().collect();
    assert_eq!(lines.len(), 3, "{tail}");
    assert!(lines[0].starts_with("0:0  "));
    assert!(lines[0].contains("key=\"k1\"  value=\"bonjour\"  src=\"web\""));
    assert!(lines[2].contains("key=null  value=\"trois\""));
}

#[test]
fn tail_can_start_from_an_offset_or_show_the_last_messages() {
    let dir = data_with_topic("1");
    ok_with_input(dir.path(), &["produce", "orders", "-q"], "a\nb\nc\nd\n");
    let offsets = |args: &[&str]| -> Vec<String> {
        ok(dir.path(), args)
            .lines()
            .map(|l| l.split_whitespace().next().unwrap().to_string())
            .collect()
    };
    assert_eq!(offsets(&["tail", "orders", "-n", "2"]), ["0:2", "0:3"]);
    assert_eq!(
        offsets(&["tail", "orders", "-p", "0", "--offset", "1"]),
        ["0:1", "0:2", "0:3"]
    );
}

#[test]
fn tail_follow_shows_new_messages_as_they_arrive() {
    let dir = data_with_topic("2");
    let mut tail = Command::new(env!("CARGO_BIN_EXE_mojay"))
        .arg("--data")
        .arg(dir.path())
        .args(["tail", "orders", "-f"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let (lines, received) = mpsc::channel();
    let stdout = tail.stdout.take().unwrap();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if lines.send(line.unwrap()).is_err() {
                return;
            }
        }
    });

    ok(dir.path(), &["produce", "orders", "en direct", "-p", "1"]);
    let line = received.recv_timeout(Duration::from_secs(10)).unwrap();
    tail.kill().unwrap();
    tail.wait().unwrap();
    assert!(
        line.starts_with("1:0  ") && line.contains("\"en direct\""),
        "{line}"
    );
}

#[test]
fn describe_shows_partitions_and_group_lag() {
    let dir = data_with_topic("2");
    ok_with_input(
        dir.path(),
        &["produce", "orders", "-q", "-p", "1"],
        "a\nb\nc\n",
    );
    {
        let db = Db::open(dir.path()).unwrap();
        let mut consumer = db
            .consumer("billing", &["orders"], ConsumerConfig::default())
            .unwrap();
        let read = consumer.poll(Duration::from_millis(500)).unwrap();
        assert_eq!(read.len(), 3);
        consumer.commit().unwrap();
    }
    ok(dir.path(), &["produce", "orders", "d", "-p", "1"]);

    let describe = ok(dir.path(), &["describe", "orders"]);
    assert!(describe.contains("partitions  2"), "{describe}");
    assert!(describe.contains("rétention   aucune"), "{describe}");
    let partition_1 = describe.lines().find(|l| l.starts_with("1 ")).unwrap();
    let columns: Vec<_> = partition_1.split_whitespace().collect();
    assert_eq!(&columns[..4], ["1", "0", "4", "4"], "{partition_1}");
    // Le consumer a commité ses deux partitions : la 0, vide, et la 1.
    let groups: Vec<Vec<&str>> = describe
        .lines()
        .filter(|l| l.starts_with("billing"))
        .map(|l| l.split_whitespace().collect())
        .collect();
    assert_eq!(
        groups,
        [["billing", "0", "0", "0"], ["billing", "1", "3", "1"]],
        "groupe, partition, commité, retard"
    );
}

#[test]
fn dump_shows_records_and_reports_damage_without_repairing() {
    let dir = data_with_topic("1");
    ok_with_input(
        dir.path(),
        &["produce", "orders", "-q"],
        "un\ndeux\ntrois\n",
    );

    let dump = ok(dir.path(), &["dump", "orders", "--index"]);
    assert!(dump.contains("en-tête valide, format v1"), "{dump}");
    assert!(dump.contains("3 enregistrement(s) lisible(s)"), "{dump}");
    assert!(dump.contains("value=\"deux\""), "{dump}");
    assert!(!dump.contains("!!"), "{dump}");

    // On abîme le dernier enregistrement : le dump le signale, sans rien réparer.
    let segment = first_segment(dir.path());
    let mut bytes = fs::read(&segment).unwrap();
    let last = bytes.len() - 3;
    bytes[last] ^= 0xFF;
    fs::write(&segment, &bytes).unwrap();

    let dump = ok(dir.path(), &["dump", segment.to_str().unwrap()]);
    assert!(dump.contains("2 enregistrement(s) lisible(s)"), "{dump}");
    assert!(dump.contains("!! fin illisible"), "{dump}");
    assert!(dump.contains("CRC invalide"), "{dump}");
    assert_eq!(
        fs::read(&segment).unwrap(),
        bytes,
        "le fichier n'est pas modifié"
    );
}

#[test]
fn errors_are_explained_and_exit_with_failure() {
    let dir = data_with_topic("1");
    assert!(fails(dir.path(), &["tail", "missing"]).contains("n'existe pas"));
    assert!(fails(dir.path(), &["tail", "orders", "-p", "4"]).contains("partition 4 inexistante"));
    assert!(fails(dir.path(), &["create-topic", "orders"]).contains("existe déjà"));
    assert!(
        fails(dir.path(), &["create-topic", "x", "--fsync", "parfois"]).contains("always, never")
    );

    let elsewhere = TempDir::new().unwrap();
    let message = fails(elsewhere.path(), &["topics"]);
    assert!(
        message.contains("n'est pas un dossier de données Mojay"),
        "{message}"
    );
    assert_eq!(
        fs::read_dir(elsewhere.path()).unwrap().count(),
        0,
        "rien n'est créé"
    );

    // Un producteur déjà ouvert : la CLI ne peut pas écrire en même temps.
    let db = Db::open(dir.path()).unwrap();
    let _writer = db.producer("orders").unwrap();
    assert!(fails(dir.path(), &["produce", "orders", "x"]).contains("a déjà un writer"));
}

#[test]
fn create_topic_accepts_human_units() {
    let dir = TempDir::new().unwrap();
    ok(
        dir.path(),
        &[
            "create-topic",
            "events",
            "--segment-size",
            "8Mio",
            "--fsync",
            "250ms",
            "--retention-size",
            "1Gio",
            "--retention-time",
            "7d",
        ],
    );
    let describe = ok(dir.path(), &["describe", "events"]);
    assert!(describe.contains("segments    8 Mio au plus"), "{describe}");
    assert!(
        describe.contains("fsync       toutes les 250 ms"),
        "{describe}"
    );
    assert!(
        describe.contains("1 Gio par partition, 7 j par segment"),
        "{describe}"
    );
}
