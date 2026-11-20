//! `mojay`: inspect and manipulate a Mojay data directory from the command line.

mod commands;
mod display;
mod parse;

use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use commands::{CliError, CliResult};

#[derive(Debug, Parser)]
#[command(
    name = "mojay",
    version,
    about = "Inspect and manipulate a Mojay data directory"
)]
struct Cli {
    /// The data directory.
    #[arg(
        long,
        short,
        env = "MOJAY_DATA",
        default_value = "./data",
        global = true
    )]
    data: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a topic (and the data directory if it does not exist).
    CreateTopic(commands::topics::CreateArgs),
    /// List topics.
    Topics,
    /// Describe a topic: configuration, partitions, segments, group offsets.
    Describe {
        /// The topic to describe.
        topic: String,
    },
    /// Send a message, or one message per line of standard input.
    Produce(commands::produce::Args),
    /// Show a topic's messages, without committing anything.
    Tail(commands::tail::Args),
    /// Show the raw contents of a segment (positions, offsets, unreadable tail), for debugging.
    Dump(commands::dump::Args),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut out = BufWriter::new(io::stdout().lock());
    let result = run(cli, &mut out).and_then(|()| Ok(out.flush()?));
    match result {
        Ok(()) | Err(CliError::OutputClosed) => ExitCode::SUCCESS,
        Err(error) => {
            drop(out);
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli, out: &mut impl Write) -> CliResult {
    let data = &cli.data;
    match cli.command {
        Command::CreateTopic(args) => commands::topics::create(data, args, out),
        Command::Topics => commands::topics::list(data, out),
        Command::Describe { topic } => commands::describe::run(data, &topic, out),
        Command::Produce(args) => commands::produce::run(data, args, io::stdin().lock(), out),
        Command::Tail(args) => commands::tail::run(data, args, out),
        Command::Dump(args) => commands::dump::run(data, args, out),
    }
}
