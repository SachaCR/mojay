//! Le point d'entrée : un dossier de données Mojay.
//!
//! ```text
//! data/
//!   mojay.toml     # version du format
//!   topics/<topic>/...
//!   groups/<group>/...
//! ```

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::durable::write_atomic;
use crate::error::{Error, Result, corrupt};
use crate::group::{self, CommittedOffset, Consumer, ConsumerConfig};
use crate::meta::{self, Meta, Value};
use crate::topic::{self, Producer, TopicConfig, TopicDescription, TopicReader};

const META_FILE: &str = "mojay.toml";
const TOPICS_DIR: &str = "topics";
const GROUPS_DIR: &str = "groups";
const FORMAT_VERSION: u64 = 1;

/// Un dossier de données Mojay, l'équivalent du fichier d'une base SQLite.
///
/// Plusieurs processus peuvent ouvrir le même dossier en même temps.
#[derive(Debug, Clone)]
pub struct Db {
    root: PathBuf,
}

impl Db {
    /// Ouvre le dossier de données `path`, en le créant au besoin.
    ///
    /// Renvoie [`Error::Corrupt`] si le dossier a été créé par une version de
    /// Mojay dont le format n'est pas supporté.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join(TOPICS_DIR))?;
        fs::create_dir_all(root.join(GROUPS_DIR))?;
        let meta_path = root.join(META_FILE);
        match fs::read_to_string(&meta_path) {
            Ok(text) => check_format(&meta_path, &text)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let entries = [("format_version", Value::Int(FORMAT_VERSION))];
                let text = meta::format("Dossier de données Mojay.", &entries);
                write_atomic(&meta_path, text.as_bytes())?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(Self { root })
    }

    /// Crée un topic. Renvoie [`Error::TopicExists`] s'il existe déjà.
    pub fn create_topic(&self, name: &str, config: TopicConfig) -> Result<()> {
        topic::create(&self.topics_dir(), name, &config)
    }

    /// Les noms des topics, triés.
    pub fn list_topics(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(self.topics_dir())? {
            let entry = entry?;
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            // Les dossiers en « . » sont des créations en cours ou interrompues.
            if !name.starts_with('.') && entry.file_type()?.is_dir() {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }

    /// Ouvre le writer d'un topic. Renvoie [`Error::Busy`] si un autre
    /// producteur, dans ce processus ou un autre, est déjà ouvert.
    pub fn producer(&self, topic: &str) -> Result<Producer> {
        Producer::open(&self.existing_topic_dir(topic)?, topic)
    }

    /// Ouvre un lecteur d'un topic. Il n'y a pas de limite au nombre de lecteurs.
    pub fn reader(&self, topic: &str) -> Result<TopicReader> {
        TopicReader::open(&self.existing_topic_dir(topic)?, topic)
    }

    /// Ouvre un consumer du groupe `group`, abonné à `topics`.
    ///
    /// Un consumer seul est l'unique membre d'un groupe à son nom. Il reprend
    /// chaque partition à l'offset commité par le groupe, ou selon
    /// [`ConsumerConfig::start_from`] s'il n'y en a pas.
    pub fn consumer(
        &self,
        group: &str,
        topics: &[&str],
        config: ConsumerConfig,
    ) -> Result<Consumer> {
        let readers = topics
            .iter()
            .map(|topic| self.reader(topic))
            .collect::<Result<_>>()?;
        Consumer::open(&self.root.join(GROUPS_DIR), group, readers, config)
    }

    /// Décrit un topic : configuration, offsets lisibles et segments de chaque
    /// partition. Ne modifie rien.
    pub fn describe_topic(&self, topic: &str) -> Result<TopicDescription> {
        topic::describe(&self.existing_topic_dir(topic)?, topic)
    }

    /// Les offsets commités pour `topic` par tous les groupes. Ne modifie rien.
    pub fn committed_offsets(&self, topic: &str) -> Result<Vec<CommittedOffset>> {
        let partitions = self.describe_topic(topic)?.config.partitions;
        group::committed_offsets(&self.root.join(GROUPS_DIR), topic, partitions)
    }

    fn topics_dir(&self) -> PathBuf {
        self.root.join(TOPICS_DIR)
    }

    fn existing_topic_dir(&self, name: &str) -> Result<PathBuf> {
        topic::validate_name(name)?;
        let dir = self.topics_dir().join(name);
        if !dir.is_dir() {
            return Err(Error::UnknownTopic(name.to_string()));
        }
        Ok(dir)
    }
}

fn check_format(path: &Path, text: &str) -> Result<()> {
    let meta = Meta::parse(path, text)?;
    meta.only(&["format_version"])?;
    let version = meta.int("format_version")?;
    if version != FORMAT_VERSION {
        return Err(corrupt(
            path,
            format!("version de format {version} non supportée (attendue : {FORMAT_VERSION})"),
        ));
    }
    Ok(())
}
