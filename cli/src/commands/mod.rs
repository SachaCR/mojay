//! CLI commands: one per file.

pub mod describe;
pub mod dump;
pub mod produce;
pub mod tail;
pub mod topics;

use std::fmt;
use std::io;
use std::path::Path;

use mojay::Db;

/// An error to show to the user, then exit with code 1.
#[derive(Debug)]
pub enum CliError {
    /// The output was closed (`mojay tail … | head`): this is not a failure.
    OutputClosed,
    Message(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputClosed => f.write_str("output closed"),
            Self::Message(message) => f.write_str(message),
        }
    }
}

impl From<mojay::Error> for CliError {
    fn from(error: mojay::Error) -> Self {
        match error {
            mojay::Error::Io(e) => e.into(),
            other => Self::Message(other.to_string()),
        }
    }
}

impl From<io::Error> for CliError {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::BrokenPipe {
            Self::OutputClosed
        } else {
            Self::Message(format!("I/O error: {error}"))
        }
    }
}

impl From<String> for CliError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

pub type CliResult = Result<(), CliError>;

/// Open a data directory that must already exist. Only `create-topic`
/// creates one: a typo must not create an empty directory elsewhere.
pub fn open_existing(data: &Path) -> Result<Db, CliError> {
    if !data.join("mojay.toml").is_file() {
        return Err(CliError::Message(format!(
            "{} is not a Mojay data directory (use --data or MOJAY_DATA)",
            data.display()
        )));
    }
    Ok(Db::open(data)?)
}
