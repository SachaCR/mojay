//! `mojay produce` : envoie un message, ou une ligne de l'entrée standard par message.

use std::io::{BufRead, Write};
use std::path::Path;

use mojay::{Header, Message, Producer};

use super::{CliResult, open_existing};
use crate::parse;

#[derive(Debug, clap::Args)]
pub struct Args {
    pub topic: String,
    /// Valeur du message. Sans valeur, chaque ligne de l'entrée standard est un message.
    pub value: Option<String>,
    /// Clé du message : elle détermine la partition (murmur2, comme Kafka).
    #[arg(long, short)]
    pub key: Option<String>,
    /// Partition imposée, au lieu de celle choisie d'après la clé.
    #[arg(long, short)]
    pub partition: Option<u32>,
    /// Header « clé=valeur ». Répétable.
    #[arg(long = "header", short = 'H', value_parser = parse::header)]
    pub headers: Vec<(String, String)>,
    /// N'affiche pas la partition et l'offset de chaque message.
    #[arg(long, short)]
    pub quiet: bool,
}

pub fn run(data: &Path, args: Args, input: impl BufRead, out: &mut impl Write) -> CliResult {
    let db = open_existing(data)?;
    let mut producer = db.producer(&args.topic)?;
    match &args.value {
        Some(value) => send(&mut producer, &args, value, out),
        None => {
            for line in input.lines() {
                send(&mut producer, &args, &line?, out)?;
            }
            Ok(())
        }
    }
}

fn send(producer: &mut Producer, args: &Args, value: &str, out: &mut impl Write) -> CliResult {
    let message = Message {
        key: args.key.as_ref().map(|k| k.clone().into_bytes()),
        value: Some(value.as_bytes().to_vec()),
        headers: args
            .headers
            .iter()
            .map(|(key, value)| Header {
                key: key.clone(),
                value: value.clone().into_bytes(),
            })
            .collect(),
        timestamp: None,
    };
    let (partition, offset) = match args.partition {
        Some(partition) => (partition, producer.send_to(partition, &message)?),
        None => producer.send(&message)?,
    };
    if !args.quiet {
        writeln!(out, "{partition}:{offset}")?;
    }
    Ok(())
}
