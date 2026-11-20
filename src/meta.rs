//! Fichiers de métadonnées `clé = valeur` (`mojay.toml`, `meta.toml`).
//!
//! Le format est un petit sous-ensemble de TOML, valide pour tout outil TOML :
//! une paire par ligne, des valeurs entières ou des chaînes entre guillemets,
//! des commentaires `#` en début de ligne. Ce sous-ensemble suffit et évite
//! une dépendance : la lecture est stricte, toute clé inconnue est une erreur.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result, corrupt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Value {
    Int(u64),
    Str(String),
}

/// Le contenu d'un fichier de métadonnées.
#[derive(Debug)]
pub(crate) struct Meta {
    path: PathBuf,
    values: BTreeMap<String, Value>,
}

impl Meta {
    /// Décode `text`, lu depuis `path` (utilisé dans les messages d'erreur).
    pub(crate) fn parse(path: &Path, text: &str) -> Result<Self> {
        let mut values = BTreeMap::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let invalid = |reason: &str| corrupt(path, format!("ligne {} : {reason}", n + 1));
            let (key, raw) = line
                .split_once('=')
                .ok_or_else(|| invalid("« clé = valeur » attendu"))?;
            let key = key.trim();
            if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                return Err(invalid("clé invalide"));
            }
            let value = parse_value(raw.trim()).ok_or_else(|| invalid("valeur invalide"))?;
            if values.insert(key.to_string(), value).is_some() {
                return Err(invalid("clé en double"));
            }
        }
        Ok(Self {
            path: path.to_path_buf(),
            values,
        })
    }

    pub(crate) fn int(&self, key: &str) -> Result<u64> {
        match self.values.get(key) {
            Some(Value::Int(n)) => Ok(*n),
            Some(Value::Str(_)) => Err(self.error(format!("« {key} » doit être un entier"))),
            None => Err(self.error(format!("clé « {key} » manquante"))),
        }
    }

    /// Comme [`int`](Self::int), pour une clé facultative.
    pub(crate) fn opt_int(&self, key: &str) -> Result<Option<u64>> {
        match self.values.get(key) {
            None => Ok(None),
            Some(_) => self.int(key).map(Some),
        }
    }

    pub(crate) fn str(&self, key: &str) -> Result<&str> {
        match self.values.get(key) {
            Some(Value::Str(s)) => Ok(s),
            Some(Value::Int(_)) => Err(self.error(format!("« {key} » doit être une chaîne"))),
            None => Err(self.error(format!("clé « {key} » manquante"))),
        }
    }

    /// Refuse toute clé hors de `allowed` : une faute de frappe ne doit pas
    /// être ignorée en silence.
    pub(crate) fn only(&self, allowed: &[&str]) -> Result<()> {
        match self.values.keys().find(|k| !allowed.contains(&k.as_str())) {
            Some(unknown) => Err(self.error(format!("clé inconnue « {unknown} »"))),
            None => Ok(()),
        }
    }

    fn error(&self, reason: String) -> Error {
        corrupt(&self.path, reason)
    }
}

fn parse_value(raw: &str) -> Option<Value> {
    if let Some(inner) = raw.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        // Pas d'échappement : les chaînes Mojay sont de simples mots.
        return (!inner.contains(['"', '\\'])).then(|| Value::Str(inner.to_string()));
    }
    raw.parse().ok().map(Value::Int)
}

/// Encode des paires, précédées d'un commentaire d'en-tête.
pub(crate) fn format(header: &str, entries: &[(&str, Value)]) -> String {
    let mut out = format!("# {header}\n");
    for (key, value) in entries {
        // Écrire dans une String ne peut pas échouer.
        let _ = match value {
            Value::Int(n) => writeln!(out, "{key} = {n}"),
            Value::Str(s) => writeln!(out, "{key} = \"{s}\""),
        };
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Meta> {
        Meta::parse(Path::new("meta.toml"), text)
    }

    #[test]
    fn format_then_parse_roundtrips() {
        let text = format(
            "test",
            &[
                ("partitions", Value::Int(4)),
                ("fsync", Value::Str("always".into())),
            ],
        );
        let meta = parse(&text).unwrap();
        assert_eq!(meta.int("partitions").unwrap(), 4);
        assert_eq!(meta.str("fsync").unwrap(), "always");
        meta.only(&["partitions", "fsync"]).unwrap();
    }

    #[test]
    fn errors_are_reported_not_ignored() {
        assert!(parse("partitions 4").is_err());
        assert!(parse("partitions = quatre").is_err());
        assert!(parse("a = 1\na = 2").is_err());
        let meta = parse("partitions = 4\npartition = 5").unwrap();
        assert!(meta.only(&["partitions"]).is_err());
        assert!(meta.int("missing").is_err());
        assert!(meta.str("partitions").is_err());
    }
}
