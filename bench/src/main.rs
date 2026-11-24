//! Mojay benchmarks. Two suites, run in release mode:
//!
//! - `micro`: the cost of each primitive, with synthetic values on one partition;
//! - `realistic`: JSON events with keys and headers, 8 partitions, several
//!   processes, large backlogs.
//!
//! ```text
//! cargo run --release -p mojay-bench
//! cargo run --release -p mojay-bench -- --suite realistic --csv results.csv
//! cargo run --release -p mojay-bench -- --only produce --seconds 5
//! ```

mod histogram;
mod measurement;
mod micro;
mod payload;
mod realistic;
mod worker;

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

use measurement::{Measurement, Scenario, Settings};
use worker::WorkerCommand;

#[derive(Debug, Parser)]
#[command(about = "Measure Mojay's throughput and latency")]
struct Args {
    /// Which scenarios to run.
    #[arg(long, value_enum, default_value_t = Suite::Micro)]
    suite: Suite,
    /// How long each time-boxed scenario runs, in seconds.
    #[arg(long, default_value_t = 2.0)]
    seconds: f64,
    /// Only run the scenarios whose name contains this text.
    #[arg(long)]
    only: Option<String>,
    /// How long the soak scenario runs, in seconds.
    #[arg(long, default_value_t = 30.0)]
    soak_seconds: f64,
    /// Data written before the catch-up and reopen scenarios, in MiB.
    #[arg(long, default_value_t = 1024)]
    backlog_mib: u64,
    /// Pause before reading the backlog, to drop the page cache by hand.
    #[arg(long)]
    cold: bool,
    /// Also write the results to this CSV file, to compare runs.
    #[arg(long)]
    csv: Option<PathBuf>,
    #[command(subcommand)]
    worker: Option<WorkerCommand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Suite {
    Micro,
    Realistic,
    All,
}

const HEADERS: [&str; 10] = [
    "scenario", "messages", "msg/s", "MiB/s", "on disk", "latency", "p50", "p99", "p99.9", "max",
];

fn main() {
    let args = Args::parse();
    if let Some(command) = args.worker {
        worker::run(command);
        return;
    }
    if cfg!(debug_assertions) {
        eprintln!("warning: debug build, numbers are not meaningful (use --release)");
    }
    let settings = Settings {
        duration: Duration::from_secs_f64(args.seconds),
        soak: Duration::from_secs_f64(args.soak_seconds),
        backlog_bytes: args.backlog_mib * 1024 * 1024,
        cold: args.cold,
    };
    let mut csv = args.csv.as_ref().map(|path| {
        let mut file = File::create(path).expect("CSV file");
        writeln!(file, "{}", CSV_HEADER).expect("write CSV");
        file
    });

    println!("{}", table_row(&HEADERS.map(String::from)));
    println!("{}", table_separator());
    for scenario in selected(args.suite, args.only.as_deref()) {
        for measurement in (scenario.run)(&settings) {
            println!("{}", table_row(&cells(&measurement)));
            if let Some(file) = csv.as_mut() {
                writeln!(file, "{}", csv_row(&measurement)).expect("write CSV");
            }
        }
    }
}

fn selected(suite: Suite, only: Option<&str>) -> Vec<Scenario> {
    let mut scenarios = Vec::new();
    if suite != Suite::Realistic {
        scenarios.extend(micro::all());
    }
    if suite != Suite::Micro {
        scenarios.extend(realistic::all());
    }
    scenarios.retain(|s| only.is_none_or(|only| s.name.contains(only)));
    scenarios
}

/// Name width: the longest scenario names fit.
const NAME_WIDTH: usize = 72;
const LEFT_ALIGNED: [bool; 10] = [
    true, false, false, false, false, true, false, false, false, false,
];
const WIDTHS: [usize; 10] = [NAME_WIDTH, 10, 9, 7, 9, 9, 9, 9, 9, 9];

fn table_row(cells: &[String; 10]) -> String {
    let columns: Vec<String> = cells
        .iter()
        .zip(WIDTHS.iter().zip(LEFT_ALIGNED))
        .map(|(cell, (width, left))| {
            if left {
                format!("{cell:<width$}")
            } else {
                format!("{cell:>width$}")
            }
        })
        .collect();
    format!("| {} |", columns.join(" | "))
}

fn table_separator() -> String {
    let columns: Vec<String> = WIDTHS
        .iter()
        .zip(LEFT_ALIGNED)
        .map(|(width, left)| {
            let dashes = "-".repeat(width - 1);
            if left {
                format!("{dashes}-")
            } else {
                format!("{dashes}:")
            }
        })
        .collect();
    format!("| {} |", columns.join(" | "))
}

fn cells(m: &Measurement) -> [String; 10] {
    let seconds = m.elapsed.as_secs_f64();
    let rate = m.messages as f64 / seconds;
    let mib = m.bytes as f64 / seconds / (1024.0 * 1024.0);
    let disk = m.disk_bytes.map_or(String::new(), format_bytes);
    let mut latency = [const { String::new() }; 5];
    if let Some((label, histogram)) = m.latency.as_ref().filter(|(_, h)| !h.is_empty()) {
        latency = [
            label.to_string(),
            format_duration(histogram.percentile(0.50)),
            format_duration(histogram.percentile(0.99)),
            format_duration(histogram.percentile(0.999)),
            format_duration(histogram.max()),
        ];
    }
    let [label, p50, p99, p999, max] = latency;
    [
        m.name.clone(),
        format_count(m.messages),
        format_rate(rate),
        format!("{mib:.1}"),
        disk,
        label,
        p50,
        p99,
        p999,
        max,
    ]
}

const CSV_HEADER: &str = "scenario,messages,seconds,msg_per_s,payload_mib_per_s,disk_bytes,latency,p50_us,p99_us,p999_us,max_us";

fn csv_row(m: &Measurement) -> String {
    let seconds = m.elapsed.as_secs_f64();
    let micros = |d: Duration| format!("{:.1}", d.as_secs_f64() * 1e6);
    let (label, percentiles) = match m.latency.as_ref().filter(|(_, h)| !h.is_empty()) {
        Some((label, h)) => (
            *label,
            [0.5, 0.99, 0.999]
                .map(|f| micros(h.percentile(f)))
                .into_iter()
                .chain([micros(h.max())])
                .collect::<Vec<_>>()
                .join(","),
        ),
        None => ("", ",,,".to_string()),
    };
    format!(
        "\"{}\",{},{:.3},{:.1},{:.2},{},{},{}",
        m.name.replace('"', "'"),
        m.messages,
        seconds,
        m.messages as f64 / seconds,
        m.bytes as f64 / seconds / (1024.0 * 1024.0),
        m.disk_bytes.map_or(String::new(), |b| b.to_string()),
        label,
        percentiles
    )
}

fn format_count(count: u64) -> String {
    let digits = count.to_string();
    let mut grouped = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.2} M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1} k", rate / 1_000.0)
    } else {
        format!("{rate:.0}")
    }
}

fn format_duration(d: Duration) -> String {
    let micros = d.as_secs_f64() * 1e6;
    if micros >= 1_000_000.0 {
        format!("{:.2} s", micros / 1_000_000.0)
    } else if micros >= 1000.0 {
        format!("{:.1} ms", micros / 1000.0)
    } else {
        format!("{micros:.0} µs")
    }
}

fn format_bytes(bytes: u64) -> String {
    let kib = bytes as f64 / 1024.0;
    if kib >= 1024.0 * 1024.0 {
        format!("{:.1} GiB", kib / 1024.0 / 1024.0)
    } else if kib >= 1024.0 {
        format!("{:.0} MiB", kib / 1024.0)
    } else {
        format!("{kib:.0} KiB")
    }
}
