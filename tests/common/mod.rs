//! Helpers shared by the integration tests.
//!
//! Multi-process tests rerun their own test binary in a child process:
//! each file's `child_process` test reads the scenario to play from the
//! environment (see [`child_scenario`]).

use std::env;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use mojay::{Db, FsyncPolicy, LogConfig, Message, TopicConfig};
use tempfile::TempDir;

pub const ALL: usize = usize::MAX;
const CHILD_SCENARIO: &str = "MOJAY_TEST_CHILD_SCENARIO";
const CHILD_DIR: &str = "MOJAY_TEST_CHILD_DIR";

/// The message at index `i`, recognizable when read back.
pub fn message(i: u64) -> Message {
    Message::new(format!("k{i}"), format!("value-{i}-{}", "x".repeat(64)))
}

pub fn config(partitions: u32, segment_max_bytes: u64, fsync: FsyncPolicy) -> TopicConfig {
    TopicConfig {
        partitions,
        log: LogConfig {
            segment_max_bytes,
            index_interval_bytes: 256,
            fsync,
            ..LogConfig::default()
        },
    }
}

pub fn new_db() -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    (dir, db)
}

/// Wait for a condition to become true, or fail after `timeout`.
pub fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out");
        thread::sleep(Duration::from_millis(5));
    }
}

/// In a child process: the scenario to play and the data directory.
/// `None` in a normal test run.
pub fn child_scenario() -> Option<(String, Db)> {
    let scenario = env::var(CHILD_SCENARIO).ok()?;
    let db = Db::open(env::var(CHILD_DIR).unwrap()).unwrap();
    Some((scenario, db))
}

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

/// Run a child scenario, wait for it to finish, and return what it wrote after `RESULT:`.
pub fn run_child(scenario: &str, dir: &Path) -> String {
    let output = child_command(scenario, dir).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "child \"{scenario}\" failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // The test harness output may precede the result on the same line.
    stdout
        .split("RESULT:")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("no result in:\n{stdout}"))
        .to_string()
}

/// Run a child scenario in the background.
pub fn spawn_child(scenario: &str, dir: &Path) -> Child {
    child_command(scenario, dir).spawn().unwrap()
}
