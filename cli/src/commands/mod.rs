//! Les commandes de la CLI : une par fichier.

pub mod describe;
pub mod dump;
pub mod produce;
pub mod tail;
pub mod topics;

use std::fmt;
use std::io;
use std::path::Path;

use mojay::Db;

/// Une erreur à afficher à l'utilisateur, puis sortie avec le code 1.
#[derive(Debug)]
pub enum CliError {
    /// La sortie a été fermée (`mojay tail … | head`) : ce n'est pas un échec.
    OutputClosed,
    Message(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputClosed => f.write_str("sortie fermée"),
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
            Self::Message(format!("erreur d'entrée/sortie : {error}"))
        }
    }
}

impl From<String> for CliError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

pub type CliResult = Result<(), CliError>;

/// Ouvre un dossier de données qui doit déjà exister. Seule `create-topic`
/// crée un dossier : une faute de frappe ne doit pas en créer un vide ailleurs.
pub fn open_existing(data: &Path) -> Result<Db, CliError> {
    if !data.join("mojay.toml").is_file() {
        return Err(CliError::Message(format!(
            "{} n'est pas un dossier de données Mojay (option --data ou variable MOJAY_DATA)",
            data.display()
        )));
    }
    Ok(Db::open(data)?)
}
