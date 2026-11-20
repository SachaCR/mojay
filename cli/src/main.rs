//! `mojay` : inspecter et manipuler un dossier de données Mojay en ligne de commande.

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
    about = "Inspecter et manipuler un dossier de données Mojay"
)]
struct Cli {
    /// Le dossier de données.
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
    /// Crée un topic (et le dossier de données s'il n'existe pas).
    CreateTopic(commands::topics::CreateArgs),
    /// Liste les topics.
    Topics,
    /// Décrit un topic : configuration, partitions, segments, offsets des groupes.
    Describe { topic: String },
    /// Envoie un message, ou une ligne de l'entrée standard par message.
    Produce(commands::produce::Args),
    /// Affiche les messages d'un topic, sans rien commiter.
    Tail(commands::tail::Args),
    /// Affiche le contenu brut d'un segment (positions, offsets, fin illisible), pour déboguer.
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
            eprintln!("erreur : {error}");
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
