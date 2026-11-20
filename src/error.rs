use std::io;
use std::path::PathBuf;

/// Erreurs renvoyées par Mojay.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("erreur d'entrée/sortie : {0}")]
    Io(#[from] io::Error),

    /// Données invalides sur le disque qu'on ne peut pas réparer
    /// automatiquement (en-tête de segment illisible, segment scellé abîmé…).
    #[error("données corrompues dans {path} : {reason}")]
    Corrupt { path: PathBuf, reason: String },

    /// L'offset demandé n'existe pas (encore, ou plus) dans la partition.
    #[error("offset {offset} hors limites (disponibles : {start}..{end})")]
    OffsetOutOfRange { offset: u64, start: u64, end: u64 },

    #[error("enregistrement trop gros : {size} octets (maximum {max})")]
    RecordTooLarge { size: usize, max: usize },

    #[error("enregistrement invalide : {0}")]
    InvalidRecord(String),

    #[error("configuration invalide : {0}")]
    InvalidConfig(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
