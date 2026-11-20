//! `Db`: a Mojay data directory.

use std::time::Duration;

use napi::Env;
use napi::bindgen_prelude::Either;
use napi_derive::napi;

use crate::consumer::{Consumer, ConsumerOptions};
use crate::convert::{non_negative, offset};
use crate::error::{BindingError, to_js};
use crate::producer::Producer;

/// A topic's configuration, fixed when it is created. Everything is optional.
#[napi(object)]
#[derive(Default)]
pub struct TopicOptions {
    /// Number of partitions (1 by default). Fixed: adding some would change the partition of every key.
    pub partitions: Option<u32>,
    /// Maximum size of a segment, in bytes (64 MiB by default).
    pub segment_bytes: Option<i64>,
    /// `'always'` (default): each message is on disk before it becomes readable.
    /// `'never'`: left to the OS. A number: one fsync every N milliseconds.
    #[napi(ts_type = "'always' | 'never' | number")]
    pub fsync: Option<Either<String, u32>>,
    /// Maximum size of a partition, in bytes; beyond it, the oldest segments are deleted.
    pub retention_bytes: Option<i64>,
    /// Maximum age of a segment, in milliseconds since its last write.
    pub retention_ms: Option<i64>,
}

/// The state of a topic on disk.
#[napi(object)]
pub struct TopicDescription {
    pub name: String,
    pub partitions: Vec<PartitionDescription>,
}

#[napi(object)]
pub struct PartitionDescription {
    pub partition: u32,
    /// First available offset.
    pub start_offset: i64,
    /// First offset not yet readable: `startOffset..readLimit` can be read.
    pub read_limit: i64,
    pub segments: u32,
    pub size_bytes: i64,
}

/// A Mojay data directory, the equivalent of a SQLite database file.
/// Several processes can open the same directory at the same time.
#[napi]
pub struct Db {
    inner: mojay::Db,
}

#[napi]
impl Db {
    /// Opens the data directory `path`, creating it if needed.
    #[napi(factory)]
    pub fn open(env: Env, path: String) -> napi::Result<Self> {
        let inner = to_js(&env, mojay::Db::open(path))?;
        Ok(Db { inner })
    }

    /// Creates a topic. Fails with `MOJAY_TOPIC_EXISTS` if it already exists.
    #[napi]
    pub fn create_topic(
        &self,
        env: Env,
        name: String,
        options: Option<TopicOptions>,
    ) -> napi::Result<()> {
        let config = to_js(&env, topic_config(options.unwrap_or_default()))?;
        to_js(&env, self.inner.create_topic(&name, config))
    }

    /// The topic names, sorted.
    #[napi]
    pub fn list_topics(&self, env: Env) -> napi::Result<Vec<String>> {
        to_js(&env, self.inner.list_topics())
    }

    /// The readable offsets and the size of each partition.
    #[napi]
    pub fn describe_topic(&self, env: Env, name: String) -> napi::Result<TopicDescription> {
        let description = to_js(&env, self.inner.describe_topic(&name))?;
        Ok(TopicDescription {
            name: description.name,
            partitions: description
                .partitions
                .iter()
                .map(|p| PartitionDescription {
                    partition: p.partition,
                    start_offset: offset(p.start_offset),
                    read_limit: offset(p.read_limit),
                    segments: p.segments.len() as u32,
                    size_bytes: offset(p.size_bytes()),
                })
                .collect(),
        })
    }

    /// Opens the writer of a topic. Fails with `MOJAY_BUSY` if another producer,
    /// in this process or another, is already open on this topic.
    #[napi]
    pub fn producer(&self, env: Env, topic: String) -> napi::Result<Producer> {
        let producer = to_js(&env, self.inner.producer(&topic))?;
        to_js(&env, Producer::start(topic, producer))
    }

    /// Opens a member of the group `group`, subscribed to `topics`. Members of
    /// the same group share the partitions.
    #[napi]
    pub fn consumer(
        &self,
        env: Env,
        group: String,
        topics: Vec<String>,
        options: Option<ConsumerOptions>,
    ) -> napi::Result<Consumer> {
        let config = to_js(&env, options.unwrap_or_default().into_config())?;
        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        let consumer = to_js(&env, self.inner.consumer(&group, &names, config))?;
        to_js(&env, Consumer::start(group, consumer))
    }
}

fn topic_config(options: TopicOptions) -> Result<mojay::TopicConfig, BindingError> {
    let defaults = mojay::LogConfig::default();
    let fsync = match options.fsync {
        None => defaults.fsync,
        Some(Either::A(policy)) => match policy.as_str() {
            "always" => mojay::FsyncPolicy::Always,
            "never" => mojay::FsyncPolicy::Never,
            other => {
                return Err(BindingError::InvalidArgument(format!(
                    "fsync: unknown value \"{other}\", expected 'always', 'never' or a number of milliseconds"
                )));
            }
        },
        Some(Either::B(ms)) => mojay::FsyncPolicy::Interval(Duration::from_millis(u64::from(ms))),
    };
    let optional =
        |name: &str, value: Option<i64>| value.map(|v| non_negative(name, v)).transpose();
    Ok(mojay::TopicConfig {
        partitions: options.partitions.unwrap_or(1),
        log: mojay::LogConfig {
            segment_max_bytes: optional("segmentBytes", options.segment_bytes)?
                .unwrap_or(defaults.segment_max_bytes),
            fsync,
            retention: mojay::Retention {
                max_bytes: optional("retentionBytes", options.retention_bytes)?,
                max_age: optional("retentionMs", options.retention_ms)?.map(Duration::from_millis),
            },
            ..defaults
        },
    })
}
