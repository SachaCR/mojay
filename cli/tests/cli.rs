//! CLI tests: the real `mojay` binary, run against a temporary directory.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use mojay::{ConsumerConfig, Db};
use tempfile::TempDir;

/// Run `mojay --data <dir> <args>` with `stdin` as input.
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

/// Run a command that must succeed, and return its output.
fn ok(dir: &Path, args: &[&str]) -> String {
    ok_with_input(dir, args, "")
}

fn ok_with_input(dir: &Path, args: &[&str], stdin: &str) -> String {
    let output = mojay(dir, args, stdin);
    assert!(
        output.status.success(),
        "mojay {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

/// Run a command that must fail, and return its error message.
fn fails(dir: &Path, args: &[&str]) -> String {
    let output = mojay(dir, args, "");
    assert!(
        !output.status.success(),
        "mojay {args:?} should have failed"
    );
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
            &["produce", "orders", "hello", "-k", "k1", "-H", "src=web"]
        ),
        "0:0\n"
    );
    ok_with_input(dir.path(), &["produce", "orders", "-q"], "two\nthree\n");

    let tail = ok(dir.path(), &["tail", "orders"]);
    let lines: Vec<_> = tail.lines().collect();
    assert_eq!(lines.len(), 3, "{tail}");
    assert!(lines[0].starts_with("0:0  "));
    assert!(lines[0].contains("key=\"k1\"  value=\"hello\"  src=\"web\""));
    assert!(lines[2].contains("key=null  value=\"three\""));
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

    ok(dir.path(), &["produce", "orders", "live", "-p", "1"]);
    let line = received.recv_timeout(Duration::from_secs(10)).unwrap();
    tail.kill().unwrap();
    tail.wait().unwrap();
    assert!(
        line.starts_with("1:0  ") && line.contains("\"live\""),
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
    assert!(describe.contains("retention   none"), "{describe}");
    let partition_1 = describe.lines().find(|l| l.starts_with("1 ")).unwrap();
    let columns: Vec<_> = partition_1.split_whitespace().collect();
    assert_eq!(&columns[..4], ["1", "0", "4", "4"], "{partition_1}");
    // The consumer committed both of its partitions: 0 (empty) and 1.
    let groups: Vec<Vec<&str>> = describe
        .lines()
        .filter(|l| l.starts_with("billing"))
        .map(|l| l.split_whitespace().collect())
        .collect();
    assert_eq!(
        groups,
        [["billing", "0", "0", "0"], ["billing", "1", "3", "1"]],
        "group, partition, committed, lag"
    );
}

#[test]
fn dump_shows_records_and_reports_damage_without_repairing() {
    let dir = data_with_topic("1");
    ok_with_input(
        dir.path(),
        &["produce", "orders", "-q"],
        "one\ntwo\nthree\n",
    );

    let dump = ok(dir.path(), &["dump", "orders", "--index"]);
    assert!(dump.contains("valid header, format v1"), "{dump}");
    assert!(dump.contains("3 readable record(s)"), "{dump}");
    assert!(dump.contains("value=\"two\""), "{dump}");
    assert!(!dump.contains("!!"), "{dump}");

    // Corrupt the last record: the dump reports it without repairing anything.
    let segment = first_segment(dir.path());
    let mut bytes = fs::read(&segment).unwrap();
    let last = bytes.len() - 3;
    bytes[last] ^= 0xFF;
    fs::write(&segment, &bytes).unwrap();

    let dump = ok(dir.path(), &["dump", segment.to_str().unwrap()]);
    assert!(dump.contains("2 readable record(s)"), "{dump}");
    assert!(dump.contains("!! unreadable tail"), "{dump}");
    assert!(dump.contains("invalid CRC"), "{dump}");
    assert_eq!(
        fs::read(&segment).unwrap(),
        bytes,
        "the file is not modified"
    );
}

#[test]
fn errors_are_explained_and_exit_with_failure() {
    let dir = data_with_topic("1");
    assert!(fails(dir.path(), &["tail", "missing"]).contains("does not exist"));
    assert!(
        fails(dir.path(), &["tail", "orders", "-p", "4"]).contains("partition 4 does not exist")
    );
    assert!(fails(dir.path(), &["create-topic", "orders"]).contains("already exists"));
    assert!(
        fails(dir.path(), &["create-topic", "x", "--fsync", "sometimes"]).contains("always, never")
    );

    let elsewhere = TempDir::new().unwrap();
    let message = fails(elsewhere.path(), &["topics"]);
    assert!(
        message.contains("is not a Mojay data directory"),
        "{message}"
    );
    assert_eq!(
        fs::read_dir(elsewhere.path()).unwrap().count(),
        0,
        "nothing is created"
    );

    // A producer is open in this process: the CLI can still write next to it.
    let db = Db::open(dir.path()).unwrap();
    let mut writer = db.producer("orders").unwrap();
    writer.send(&mojay::Message::with_value("app")).unwrap();
    assert_eq!(ok(dir.path(), &["produce", "orders", "cli"]), "0:1\n");
    writer
        .send(&mojay::Message::with_value("app again"))
        .unwrap();
    assert_eq!(ok(dir.path(), &["tail", "orders"]).lines().count(), 3);
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
            "8MiB",
            "--fsync",
            "250ms",
            "--retention-size",
            "1GiB",
            "--retention-time",
            "7d",
        ],
    );
    let describe = ok(dir.path(), &["describe", "events"]);
    assert!(describe.contains("segments    8 MiB max"), "{describe}");
    assert!(describe.contains("fsync       every 250ms"), "{describe}");
    assert!(
        describe.contains("1 GiB per partition, 7d per segment"),
        "{describe}"
    );
}
