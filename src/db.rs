//! The entry point: a Mojay data directory.
//!
//! ```text
//! data/
//!   mojay.toml     # format version
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

/// A Mojay data directory, the equivalent of an SQLite database file.
///
/// Several processes can open the same directory at the same time.
#[derive(Debug, Clone)]
pub struct Db {
    root: PathBuf,
}

impl Db {
    /// Opens the data directory `path`, creating it if needed.
    ///
    /// Returns [`Error::Corrupt`] if the directory was created by a version of
    /// Mojay whose format is not supported.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join(TOPICS_DIR))?;
        fs::create_dir_all(root.join(GROUPS_DIR))?;
        let meta_path = root.join(META_FILE);
        match fs::read_to_string(&meta_path) {
            Ok(text) => check_format(&meta_path, &text)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let entries = [("format_version", Value::Int(FORMAT_VERSION))];
                let text = meta::format("Mojay data directory.", &entries);
                write_atomic(&meta_path, text.as_bytes())?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(Self { root })
    }

    /// Creates a topic. Returns [`Error::TopicExists`] if it already exists.
    pub fn create_topic(&self, name: &str, config: TopicConfig) -> Result<()> {
        topic::create(&self.topics_dir(), name, &config)
    }

    /// The topic names, sorted.
    pub fn list_topics(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(self.topics_dir())? {
            let entry = entry?;
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            // Directories starting with "." are in-progress or interrupted creations.
            if !name.starts_with('.') && entry.file_type()?.is_dir() {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }

    /// Opens a writer on a topic. Any number of producers, in this process
    /// or others, can write to the same topic: they take turns on each
    /// partition, one batch at a time.
    pub fn producer(&self, topic: &str) -> Result<Producer> {
        Producer::open(&self.existing_topic_dir(topic)?, topic)
    }

    /// Opens a reader on a topic. There is no limit on the number of readers.
    pub fn reader(&self, topic: &str) -> Result<TopicReader> {
        TopicReader::open(&self.existing_topic_dir(topic)?, topic)
    }

    /// Opens a consumer in group `group`, subscribed to `topics`.
    ///
    /// A standalone consumer is the only member of a group named after it. It resumes
    /// each partition at the offset committed by the group, or according to
    /// [`ConsumerConfig::start_from`] if there is none.
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

    /// Describes a topic: configuration, readable offsets and segments of each
    /// partition. Modifies nothing.
    pub fn describe_topic(&self, topic: &str) -> Result<TopicDescription> {
        topic::describe(&self.existing_topic_dir(topic)?, topic)
    }

    /// The offsets committed for `topic` by all groups. Modifies nothing.
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
            format!("unsupported format version {version} (expected {FORMAT_VERSION})"),
        ));
    }
    Ok(())
}
