//! `key = value` metadata files (`mojay.toml`, `meta.toml`).
//!
//! The format is a small subset of TOML, valid for any TOML tool:
//! one pair per line, integer values or double-quoted strings,
//! `#` comments at the start of a line. This subset is enough and avoids
//! a dependency: parsing is strict, any unknown key is an error.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result, corrupt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Value {
    Int(u64),
    Str(String),
}

/// The contents of a metadata file.
#[derive(Debug)]
pub(crate) struct Meta {
    path: PathBuf,
    values: BTreeMap<String, Value>,
}

impl Meta {
    /// Decodes `text`, read from `path` (used in error messages).
    pub(crate) fn parse(path: &Path, text: &str) -> Result<Self> {
        let mut values = BTreeMap::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let invalid = |reason: &str| corrupt(path, format!("line {}: {reason}", n + 1));
            let (key, raw) = line
                .split_once('=')
                .ok_or_else(|| invalid("expected \"key = value\""))?;
            let key = key.trim();
            if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                return Err(invalid("invalid key"));
            }
            let value = parse_value(raw.trim()).ok_or_else(|| invalid("invalid value"))?;
            if values.insert(key.to_string(), value).is_some() {
                return Err(invalid("duplicate key"));
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
            Some(Value::Str(_)) => Err(self.error(format!("\"{key}\" must be an integer"))),
            None => Err(self.error(format!("missing key \"{key}\""))),
        }
    }

    /// Like [`int`](Self::int), for an optional key.
    pub(crate) fn opt_int(&self, key: &str) -> Result<Option<u64>> {
        match self.values.get(key) {
            None => Ok(None),
            Some(_) => self.int(key).map(Some),
        }
    }

    pub(crate) fn str(&self, key: &str) -> Result<&str> {
        match self.values.get(key) {
            Some(Value::Str(s)) => Ok(s),
            Some(Value::Int(_)) => Err(self.error(format!("\"{key}\" must be a string"))),
            None => Err(self.error(format!("missing key \"{key}\""))),
        }
    }

    /// Rejects any key not in `allowed`: a typo must not be
    /// silently ignored.
    pub(crate) fn only(&self, allowed: &[&str]) -> Result<()> {
        match self.values.keys().find(|k| !allowed.contains(&k.as_str())) {
            Some(unknown) => Err(self.error(format!("unknown key \"{unknown}\""))),
            None => Ok(()),
        }
    }

    fn error(&self, reason: String) -> Error {
        corrupt(&self.path, reason)
    }
}

fn parse_value(raw: &str) -> Option<Value> {
    if let Some(inner) = raw.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        // No escaping: Mojay strings are plain words.
        return (!inner.contains(['"', '\\'])).then(|| Value::Str(inner.to_string()));
    }
    raw.parse().ok().map(Value::Int)
}

/// Encodes pairs, preceded by a header comment.
pub(crate) fn format(header: &str, entries: &[(&str, Value)]) -> String {
    let mut out = format!("# {header}\n");
    for (key, value) in entries {
        // Writing to a String cannot fail.
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
        assert!(parse("partitions = four").is_err());
        assert!(parse("a = 1\na = 2").is_err());
        let meta = parse("partitions = 4\npartition = 5").unwrap();
        assert!(meta.only(&["partitions"]).is_err());
        assert!(meta.int("missing").is_err());
        assert!(meta.str("partitions").is_err());
    }
}
