//! `Consumer`: a member of a consumer group.

use std::collections::HashMap;
use std::time::Duration;

use napi::Env;
use napi::bindgen_prelude::{Buffer, Object};
use napi_derive::napi;

use crate::convert::offset;
use crate::error::BindingError;
use crate::worker::Worker;

/// Consumer options. Everything is optional.
#[napi(object)]
#[derive(Default)]
pub struct ConsumerOptions {
    /// Where to start when there is no committed offset: `'earliest'` (default) or `'latest'`.
    #[napi(ts_type = "'earliest' | 'latest'")]
    pub start_from: Option<String>,
    /// Automatic commit, at most every N milliseconds, at the start of each
    /// `poll` and on close. Without this option: manual `commit()`.
    pub auto_commit_ms: Option<u32>,
    /// Approximate maximum size of the messages returned by one `poll`.
    pub max_poll_bytes: Option<u32>,
}

impl ConsumerOptions {
    pub fn into_config(self) -> Result<mojay::ConsumerConfig, BindingError> {
        let defaults = mojay::ConsumerConfig::default();
        let start_from = match self.start_from.as_deref() {
            None | Some("earliest") => mojay::StartFrom::Earliest,
            Some("latest") => mojay::StartFrom::Latest,
            Some(other) => {
                return Err(BindingError::InvalidArgument(format!(
                    "startFrom: unknown value \"{other}\", expected 'earliest' or 'latest'"
                )));
            }
        };
        Ok(mojay::ConsumerConfig {
            start_from,
            auto_commit: self
                .auto_commit_ms
                .map(|ms| Duration::from_millis(u64::from(ms))),
            max_poll_bytes: self
                .max_poll_bytes
                .map_or(defaults.max_poll_bytes, |b| b as usize),
        })
    }
}

/// A message that was read.
#[napi(object)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: u32,
    pub offset: i64,
    /// Milliseconds since the epoch.
    pub timestamp: i64,
    pub key: Option<Buffer>,
    pub value: Option<Buffer>,
    pub headers: HashMap<String, Buffer>,
}

#[napi(object)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

/// A partition assigned or revoked during a group rebalance.
#[napi(object)]
pub struct RebalanceEvent {
    #[napi(ts_type = "'assigned' | 'revoked'")]
    pub kind: String,
    pub topic: String,
    pub partition: u32,
}

/// A member of a consumer group. The live members of a group share its
/// partitions, whether in this process or in others.
///
/// Calls run in order, on a dedicated thread: a waiting `poll` never
/// blocks Node.
#[napi]
pub struct Consumer {
    group: String,
    worker: Worker<mojay::Consumer>,
}

impl Consumer {
    pub fn start(group: String, consumer: mojay::Consumer) -> Result<Self, BindingError> {
        Ok(Self {
            worker: Worker::spawn(&format!("mojay-consumer-{group}"), consumer)?,
            group,
        })
    }
}

#[napi]
impl Consumer {
    #[napi(getter)]
    pub fn group(&self) -> String {
        self.group.clone()
    }

    /// Waits at most `timeoutMs` for messages, and returns them as soon as there are any.
    /// The Promise resolves to an empty list if nothing arrived in time.
    ///
    /// `poll` is also when this member rebalances with the rest of the group
    /// (at most every 200 ms): a consumer that stops calling `poll` keeps its
    /// partitions, and a new member gets none of them, until it polls again
    /// or is closed. Keep polling, or `close()` a consumer you no longer read.
    #[napi(ts_return_type = "Promise<ConsumerRecord[]>")]
    pub fn poll<'env>(&self, env: &'env Env, timeout_ms: u32) -> napi::Result<Object<'env>> {
        let timeout = Duration::from_millis(u64::from(timeout_ms));
        self.worker.call(
            env,
            move |consumer| Ok(consumer.poll(timeout)?),
            |_, records| Ok(records.into_iter().map(to_js_record).collect::<Vec<_>>()),
        )
    }

    /// Commits the position of each owned partition: the messages already
    /// returned by `poll` are considered processed.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn commit<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.worker
            .call(env, |consumer| Ok(consumer.commit()?), |_, ()| Ok(()))
    }

    /// The partitions owned by this member.
    #[napi(ts_return_type = "Promise<TopicPartition[]>")]
    pub fn assignment<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.worker.call(
            env,
            |consumer| Ok(consumer.assignment()),
            |_, owned| {
                Ok(owned
                    .into_iter()
                    .map(|(topic, partition)| TopicPartition { topic, partition })
                    .collect::<Vec<_>>())
            },
        )
    }

    /// The partitions assigned or revoked since the last call, in order.
    #[napi(ts_return_type = "Promise<RebalanceEvent[]>")]
    pub fn rebalance_events<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.worker.call(
            env,
            |consumer| Ok(consumer.take_rebalance_events()),
            |_, events| Ok(events.into_iter().map(to_js_event).collect::<Vec<_>>()),
        )
    }

    /// Leaves the group: commits if auto-commit is enabled, then hands the
    /// partitions back to the other members.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn close<'env>(&mut self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.worker.close(env)
    }
}

fn to_js_record(record: mojay::ConsumerRecord) -> ConsumerRecord {
    ConsumerRecord {
        topic: record.topic,
        partition: record.partition,
        offset: offset(record.offset),
        timestamp: record.timestamp,
        key: record.key.map(Buffer::from),
        value: record.value.map(Buffer::from),
        headers: record
            .headers
            .into_iter()
            .map(|h| (h.key, Buffer::from(h.value)))
            .collect(),
    }
}

fn to_js_event(event: mojay::RebalanceEvent) -> RebalanceEvent {
    let (kind, topic, partition) = match event {
        mojay::RebalanceEvent::Assigned { topic, partition } => ("assigned", topic, partition),
        mojay::RebalanceEvent::Revoked { topic, partition } => ("revoked", topic, partition),
    };
    RebalanceEvent {
        kind: kind.to_string(),
        topic,
        partition,
    }
}
