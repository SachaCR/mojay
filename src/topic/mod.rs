//! Les topics : un dossier par topic, avec ses partitions, ses métadonnées,
//! le verrou de son writer et ses limites de lecture.
//!
//! ```text
//! topics/<topic>/
//!   meta.toml      # nombre de partitions et configuration du log
//!   writer.lock    # flock exclusif = le writer du topic
//!   read_limits    # une limite de lecture par partition, partagée en mmap
//!   p0/ p1/ ...    # un PartitionLog par partition
//! ```
//!
//! - [`Producer`] : le writer, unique par topic grâce au verrou.
//! - [`TopicReader`] : un lecteur, sans verrou, autant qu'on veut.

mod background_sync;
mod producer;
mod reader;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use producer::Producer;
pub use reader::TopicReader;

use crate::durable::{sync_dir, write_atomic};
use crate::error::{Error, Result, corrupt};
use crate::log::{FsyncPolicy, LogConfig, Retention};
use crate::meta::{self, Meta, Value};
use crate::read_limits;

const META_FILE: &str = "meta.toml";
const WRITER_LOCK_FILE: &str = "writer.lock";
const READ_LIMITS_FILE: &str = "read_limits";
const META_FORMAT_VERSION: u64 = 1;
const MAX_PARTITIONS: u32 = 10_000;
/// Comme Kafka : au-delà, certains systèmes de fichiers refusent le nom.
const MAX_NAME_LEN: usize = 249;

/// Configuration d'un topic, fixée à sa création.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    /// Nombre de partitions. Fixe : en ajouter changerait la partition de chaque clé.
    pub partitions: u32,
    /// Configuration du log de chaque partition.
    pub log: LogConfig,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            partitions: 1,
            log: LogConfig::default(),
        }
    }
}

impl TopicConfig {
    fn validate(&self) -> Result<()> {
        if !(1..=MAX_PARTITIONS).contains(&self.partitions) {
            return Err(Error::InvalidConfig(format!(
                "le nombre de partitions doit être compris entre 1 et {MAX_PARTITIONS}"
            )));
        }
        self.log.validate()
    }
}

fn partition_dir(topic_dir: &Path, partition: u32) -> PathBuf {
    topic_dir.join(format!("p{partition}"))
}

/// Vérifie qu'un nom de topic ou de groupe est utilisable comme nom de dossier, partout.
pub(crate) fn validate_name(name: &str) -> Result<()> {
    let invalid = |reason: &str| Error::InvalidName {
        name: name.to_string(),
        reason: reason.to_string(),
    };
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(invalid("entre 1 et 249 caractères"));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(invalid(
            "seuls a-z, A-Z, 0-9, « . », « _ » et « - » sont permis",
        ));
    }
    if name.starts_with('.') {
        // Les noms en « . » sont réservés aux dossiers temporaires de création.
        return Err(invalid("ne peut pas commencer par « . »"));
    }
    Ok(())
}

/// Crée un topic de façon atomique : il apparaît complet, ou pas du tout.
///
/// Tout est préparé dans un dossier temporaire, rendu durable, puis renommé.
pub(crate) fn create(topics_dir: &Path, name: &str, config: &TopicConfig) -> Result<()> {
    validate_name(name)?;
    config.validate()?;
    let final_dir = topics_dir.join(name);
    if final_dir.exists() {
        return Err(Error::TopicExists(name.to_string()));
    }

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let tmp = topics_dir.join(format!(".creating-{name}-{}-{nanos}", std::process::id()));
    let result = prepare(&tmp, config).and_then(|()| match fs::rename(&tmp, &final_dir) {
        Ok(()) => sync_dir(topics_dir),
        // Un autre processus a créé le même topic entre-temps.
        Err(e) if final_dir.exists() || e.kind() == io::ErrorKind::DirectoryNotEmpty => {
            Err(Error::TopicExists(name.to_string()))
        }
        Err(e) => Err(e.into()),
    });
    if result.is_err() {
        // Nettoyage au mieux : un reste en « . » est ignoré par la liste des topics.
        let _ = fs::remove_dir_all(&tmp);
    }
    result
}

/// Remplit le dossier temporaire d'un nouveau topic.
fn prepare(dir: &Path, config: &TopicConfig) -> Result<()> {
    fs::create_dir(dir)?;
    write_atomic(&dir.join(META_FILE), encode_config(config).as_bytes())?;
    read_limits::create(&dir.join(READ_LIMITS_FILE), config.partitions)?;
    for partition in 0..config.partitions {
        fs::create_dir(partition_dir(dir, partition))?;
    }
    sync_dir(dir)
}

fn encode_config(config: &TopicConfig) -> String {
    let (fsync, interval_ms) = match config.log.fsync {
        FsyncPolicy::Always => ("always", None),
        FsyncPolicy::Interval(d) => ("interval", Some(d.as_millis() as u64)),
        FsyncPolicy::Never => ("never", None),
    };
    let mut entries = vec![
        ("format_version", Value::Int(META_FORMAT_VERSION)),
        ("partitions", Value::Int(u64::from(config.partitions))),
        (
            "segment_max_bytes",
            Value::Int(config.log.segment_max_bytes),
        ),
        (
            "index_interval_bytes",
            Value::Int(config.log.index_interval_bytes),
        ),
        ("fsync", Value::Str(fsync.to_string())),
    ];
    if let Some(ms) = interval_ms {
        entries.push(("fsync_interval_ms", Value::Int(ms)));
    }
    // Clés facultatives : un meta.toml sans elles signifie « aucune rétention ».
    if let Some(bytes) = config.log.retention.max_bytes {
        entries.push(("retention_bytes", Value::Int(bytes)));
    }
    if let Some(age) = config.log.retention.max_age {
        entries.push(("retention_ms", Value::Int(age.as_millis() as u64)));
    }
    meta::format(
        "Métadonnées d'un topic Mojay. Ne pas modifier à la main.",
        &entries,
    )
}

/// Lit la configuration d'un topic existant.
fn load_config(topic_dir: &Path) -> Result<TopicConfig> {
    let path = topic_dir.join(META_FILE);
    let text = fs::read_to_string(&path)?;
    let meta = Meta::parse(&path, &text)?;
    meta.only(&[
        "format_version",
        "partitions",
        "segment_max_bytes",
        "index_interval_bytes",
        "fsync",
        "fsync_interval_ms",
        "retention_bytes",
        "retention_ms",
    ])?;
    let version = meta.int("format_version")?;
    if version != META_FORMAT_VERSION {
        return Err(corrupt(
            &path,
            format!("version de format {version} non supportée"),
        ));
    }
    let fsync = match meta.str("fsync")? {
        "always" => FsyncPolicy::Always,
        "never" => FsyncPolicy::Never,
        "interval" => FsyncPolicy::Interval(Duration::from_millis(meta.int("fsync_interval_ms")?)),
        other => {
            return Err(corrupt(
                &path,
                format!("politique fsync inconnue « {other} »"),
            ));
        }
    };
    let partitions = u32::try_from(meta.int("partitions")?)
        .map_err(|_| corrupt(&path, "nombre de partitions trop grand"))?;
    let config = TopicConfig {
        partitions,
        log: LogConfig {
            segment_max_bytes: meta.int("segment_max_bytes")?,
            index_interval_bytes: meta.int("index_interval_bytes")?,
            fsync,
            retention: Retention {
                max_bytes: meta.opt_int("retention_bytes")?,
                max_age: meta.opt_int("retention_ms")?.map(Duration::from_millis),
            },
        },
    };
    config
        .validate()
        .map_err(|e| corrupt(&path, e.to_string()))?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_survives_a_roundtrip_through_meta_toml() {
        let dir = tempfile::TempDir::new().unwrap();
        for fsync in [
            FsyncPolicy::Always,
            FsyncPolicy::Never,
            FsyncPolicy::Interval(Duration::from_millis(250)),
        ] {
            let config = TopicConfig {
                partitions: 7,
                log: LogConfig {
                    segment_max_bytes: 1 << 20,
                    index_interval_bytes: 512,
                    fsync,
                    retention: Retention {
                        max_bytes: Some(1 << 30),
                        max_age: Some(Duration::from_secs(3600)),
                    },
                },
            };
            fs::write(dir.path().join(META_FILE), encode_config(&config)).unwrap();
            assert_eq!(load_config(dir.path()).unwrap(), config);
        }
    }

    #[test]
    fn names_are_checked() {
        for ok in ["orders", "orders.v2", "a_b-c", "X1"] {
            validate_name(ok).unwrap();
        }
        let too_long = "a".repeat(250);
        for bad in ["", ".hidden", "..", "a/b", "é", "a b", too_long.as_str()] {
            assert!(validate_name(bad).is_err(), "{bad:?} devrait être refusé");
        }
    }
}
