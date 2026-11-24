//! Child processes, to measure Mojay as it is meant to be used: several
//! programs sharing one data folder. The bench starts itself again with a
//! hidden subcommand; parent and child talk over stdin/stdout, one line at a
//! time:
//!
//! - the child prints `ready` once its producer or consumer is open;
//! - a producer waits for `go`, sends for the given time, then prints its result;
//! - a consumer polls right away, until it reads `stop` (or stdin closes), then
//!   prints its result.
//!
//! The result line is `result <messages> <bytes> <histogram>`.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Subcommand;
use mojay::{ConsumerConfig, Db};

use crate::histogram::Histogram;
use crate::payload::{self, Workload};
use crate::realistic::{TOPIC, delivery_latency};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);
/// Consumers commit like the demo does: Kafka's habit, at-least-once.
const AUTO_COMMIT: Duration = Duration::from_secs(1);

/// The hidden subcommands run by child processes.
#[derive(Debug, Subcommand)]
pub enum WorkerCommand {
    /// Sends `mixed` messages as fast as possible, one keyed send at a time.
    #[command(hide = true)]
    ProduceWorker {
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        seconds: f64,
        #[arg(long)]
        seed: u64,
    },
    /// Consumes as a member of `group` and measures delivery latency.
    #[command(hide = true)]
    ConsumeWorker {
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        group: String,
    },
}

/// What a child process measured.
pub struct WorkerResult {
    pub messages: u64,
    pub bytes: u64,
    pub latency: Histogram,
}

/// Runs a child process's side of the protocol.
pub fn run(command: WorkerCommand) {
    match command {
        WorkerCommand::ProduceWorker {
            data,
            seconds,
            seed,
        } => produce(&data, Duration::from_secs_f64(seconds), seed),
        WorkerCommand::ConsumeWorker { data, group } => consume(&data, &group),
    }
}

fn produce(data: &PathBuf, duration: Duration, seed: u64) {
    let db = Db::open(data).expect("data folder");
    let mut producer = db.producer(TOPIC).expect("producer");
    let pool = payload::pool(Workload::Mixed, seed);
    println!("ready");
    wait_for_line("go");

    let mut result = empty_result();
    let started = Instant::now();
    while started.elapsed() < duration {
        let message = &pool[result.messages as usize % pool.len()];
        let before = Instant::now();
        producer.send(message).expect("send");
        result.latency.record(before.elapsed());
        result.messages += 1;
        result.bytes += payload::message_bytes(message);
    }
    print_result(&result);
}

fn consume(data: &PathBuf, group: &str) {
    let db = Db::open(data).expect("data folder");
    let config = ConsumerConfig {
        auto_commit: Some(AUTO_COMMIT),
        ..ConsumerConfig::default()
    };
    let mut consumer = db.consumer(group, &[TOPIC], config).expect("consumer");
    let stopped = Arc::new(AtomicBool::new(false));
    let stop = Arc::clone(&stopped);
    thread::spawn(move || {
        wait_for_line("stop");
        stop.store(true, Ordering::SeqCst);
    });
    println!("ready");

    let mut result = empty_result();
    while !stopped.load(Ordering::SeqCst) {
        for record in consumer.poll(POLL_TIMEOUT).expect("poll") {
            if let Some(latency) = delivery_latency(&record.headers) {
                result.latency.record(latency);
            }
            result.messages += 1;
            result.bytes += record.value.map_or(0, |v| v.len() as u64);
        }
    }
    // Dropping the consumer commits and leaves the group.
    drop(consumer);
    print_result(&result);
}

fn empty_result() -> WorkerResult {
    WorkerResult {
        messages: 0,
        bytes: 0,
        latency: Histogram::default(),
    }
}

/// Blocks until stdin gives `expected`, or is closed (the parent died).
fn wait_for_line(expected: &str) {
    for line in std::io::stdin().lock().lines() {
        match line {
            Ok(line) if line.trim() == expected => return,
            Ok(_) => continue,
            Err(_) => return,
        }
    }
}

fn print_result(result: &WorkerResult) {
    println!(
        "result {} {} {}",
        result.messages,
        result.bytes,
        result.latency.encode()
    );
}

/// The parent's handle on a child process.
pub struct Child {
    process: std::process::Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Child {
    /// Starts a producer child on the data folder, and waits until it is ready.
    pub fn producer(data: &std::path::Path, duration: Duration, seed: u64) -> Self {
        Self::spawn(&[
            "produce-worker".into(),
            format!("--data={}", data.display()),
            format!("--seconds={}", duration.as_secs_f64()),
            format!("--seed={seed}"),
        ])
    }

    /// Starts a consumer child in `group`, and waits until it is ready.
    pub fn consumer(data: &std::path::Path, group: &str) -> Self {
        Self::spawn(&[
            "consume-worker".into(),
            format!("--data={}", data.display()),
            format!("--group={group}"),
        ])
    }

    fn spawn(args: &[String]) -> Self {
        let exe = std::env::current_exe().expect("path of the bench executable");
        let mut process = Command::new(exe)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("child process");
        let stdin = process.stdin.take().expect("piped stdin");
        let stdout = BufReader::new(process.stdout.take().expect("piped stdout"));
        let mut child = Self {
            process,
            stdin,
            stdout,
        };
        let line = child.read_line();
        assert_eq!(line.trim(), "ready", "unexpected line from a worker");
        child
    }

    pub fn send(&mut self, line: &str) {
        writeln!(self.stdin, "{line}").expect("write to a worker");
    }

    /// Waits for the result line and for the process to exit.
    pub fn result(mut self) -> WorkerResult {
        let line = self.read_line();
        let status = self.process.wait().expect("wait for a worker");
        assert!(status.success(), "worker failed: {status}");
        parse_result(&line).unwrap_or_else(|| panic!("malformed worker result: {line:?}"))
    }

    /// Ends the process abruptly, as a crash would.
    pub fn kill(mut self) {
        self.process.kill().expect("kill a worker");
        self.process.wait().expect("wait for a worker");
    }

    fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.stdout
            .read_line(&mut line)
            .expect("read from a worker");
        line
    }
}

fn parse_result(line: &str) -> Option<WorkerResult> {
    let rest = line.trim().strip_prefix("result ")?;
    let mut fields = rest.splitn(3, ' ');
    Some(WorkerResult {
        messages: fields.next()?.parse().ok()?,
        bytes: fields.next()?.parse().ok()?,
        latency: Histogram::decode(fields.next()?)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_result_line_round_trips() {
        let mut result = empty_result();
        result.messages = 12;
        result.bytes = 3400;
        result.latency.record(Duration::from_micros(250));
        let line = format!(
            "result {} {} {}\n",
            result.messages,
            result.bytes,
            result.latency.encode()
        );
        let parsed = parse_result(&line).expect("valid line");
        assert_eq!((parsed.messages, parsed.bytes), (12, 3400));
        assert_eq!(parsed.latency.max(), Duration::from_micros(250));
        assert!(parse_result("ready").is_none());
    }
}
