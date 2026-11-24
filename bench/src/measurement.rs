//! What every scenario shares: its settings and the shape of its result.

use std::time::Duration;

use crate::histogram::Histogram;

/// Command-line settings that scenarios read.
pub struct Settings {
    /// How long each time-boxed scenario runs.
    pub duration: Duration,
    /// How long the soak scenario runs.
    pub soak: Duration,
    /// Size of the backlog written before the catch-up scenario.
    pub backlog_bytes: u64,
    /// Wait for the page cache to be dropped before reading the backlog.
    pub cold: bool,
}

/// The result of one scenario, or of one window of a long scenario.
pub struct Measurement {
    pub name: String,
    pub messages: u64,
    /// Payload bytes (keys, values and headers), not counting the on-disk overhead.
    pub bytes: u64,
    pub elapsed: Duration,
    /// Size of the topic's segments on disk at the end, when meaningful.
    pub disk_bytes: Option<u64>,
    /// What the latency measures ("per send", "delivery", ...), and its histogram.
    pub latency: Option<(&'static str, Histogram)>,
}

/// A named scenario. A long one can report several rows (one per window).
pub struct Scenario {
    pub name: String,
    pub run: RunScenario,
}

pub type RunScenario = Box<dyn Fn(&Settings) -> Vec<Measurement>>;
