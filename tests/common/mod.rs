//! Outils partagés par les tests d'intégration.
//!
//! Les tests multi-processus relancent leur propre binaire de test dans un
//! processus enfant : le test `child_process` de chaque fichier lit le scénario
//! à jouer dans l'environnement (voir [`child_scenario`]).

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

/// Le message d'indice `i`, reconnaissable à la relecture.
pub fn message(i: u64) -> Message {
    Message::new(format!("k{i}"), format!("valeur-{i}-{}", "x".repeat(64)))
}

pub fn config(partitions: u32, segment_max_bytes: u64, fsync: FsyncPolicy) -> TopicConfig {
    TopicConfig {
        partitions,
        log: LogConfig {
            segment_max_bytes,
            index_interval_bytes: 256,
            fsync,
        },
    }
}

pub fn new_db() -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    (dir, db)
}

/// Attend qu'une condition devienne vraie, ou échoue après `timeout`.
pub fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "délai dépassé");
        thread::sleep(Duration::from_millis(5));
    }
}

/// Dans un processus enfant : le scénario à jouer et le dossier de données.
/// `None` dans un test normal.
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

/// Lance un scénario enfant, attend sa fin, et renvoie ce qu'il a écrit après `RESULT:`.
pub fn run_child(scenario: &str, dir: &Path) -> String {
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

/// Lance un scénario enfant en arrière-plan.
pub fn spawn_child(scenario: &str, dir: &Path) -> Child {
    child_command(scenario, dir).spawn().unwrap()
}
