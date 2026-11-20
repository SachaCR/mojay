//! Mojay benchmarks: throughput and latency of producing, reading and
//! consuming, for each fsync policy. Run in release mode:
//!
//! ```text
//! cargo run --release -p mojay-bench
//! cargo run --release -p mojay-bench -- --only produce --seconds 5
//! ```

mod histogram;
mod scenarios;

use std::time::Duration;

use clap::Parser;

use scenarios::Measurement;

#[derive(Debug, Parser)]
#[command(about = "Measure Mojay's throughput and latency")]
struct Args {
    /// How long each time-boxed scenario runs, in seconds.
    #[arg(long, default_value_t = 2.0)]
    seconds: f64,
    /// Only run the scenarios whose name contains this text.
    #[arg(long)]
    only: Option<String>,
}

fn main() {
    let args = Args::parse();
    let duration = Duration::from_secs_f64(args.seconds);
    if cfg!(debug_assertions) {
        eprintln!("warning: debug build, numbers are not meaningful (use --release)");
    }

    println!(
        "| {:<52} | {:>9} | {:>11} | {:>9} | {:<9} | {:>9} | {:>9} |",
        "scenario", "messages", "msg/s", "MiB/s", "latency", "p50", "p99"
    );
    println!(
        "| {:-<52} | {:->9} | {:->11} | {:->9} | {:-<9} | {:->9} | {:->9} |",
        "", "", "", "", "", "", ""
    );
    for scenario in scenarios::all() {
        if args
            .only
            .as_ref()
            .is_some_and(|only| !scenario.name.contains(only.as_str()))
        {
            continue;
        }
        let measurement = (scenario.run)(duration);
        println!("{}", row(&measurement));
    }
}

fn row(m: &Measurement) -> String {
    let seconds = m.elapsed.as_secs_f64();
    let rate = m.messages as f64 / seconds;
    let mib = m.bytes as f64 / seconds / (1024.0 * 1024.0);
    let (label, p50, p99) = match &m.latency {
        Some((label, histogram)) if !histogram.is_empty() => (
            *label,
            format_duration(histogram.percentile(0.50)),
            format_duration(histogram.percentile(0.99)),
        ),
        _ => ("", String::new(), String::new()),
    };
    format!(
        "| {:<52} | {:>9} | {:>11} | {:>9.1} | {:<9} | {:>9} | {:>9} |",
        m.name,
        m.messages,
        format_rate(rate),
        mib,
        label,
        p50,
        p99
    )
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
    if micros >= 1000.0 {
        format!("{:.1} ms", micros / 1000.0)
    } else {
        format!("{micros:.0} µs")
    }
}
