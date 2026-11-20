//! `Producer`: the writer of a topic.

use std::collections::HashMap;

use napi::Env;
use napi::bindgen_prelude::Object;
use napi_derive::napi;

use crate::convert::{JsBytes, bytes, offset};
use crate::error::BindingError;
use crate::worker::Worker;

/// A message to send.
#[napi(object)]
pub struct Message {
    /// The key chooses the partition (murmur2, like Kafka): messages with the
    /// same key stay together, in order.
    #[napi(ts_type = "string | Buffer")]
    pub key: Option<JsBytes>,
    /// Without a value, the message is a "tombstone" (null value).
    #[napi(ts_type = "string | Buffer")]
    pub value: Option<JsBytes>,
    #[napi(ts_type = "Record<string, string | Buffer>")]
    pub headers: Option<HashMap<String, JsBytes>>,
    /// Explicit partition, instead of the one chosen from the key.
    pub partition: Option<u32>,
    /// Milliseconds since the epoch; defaults to the time of sending.
    pub timestamp: Option<i64>,
}

/// Where a message was written.
#[napi(object)]
pub struct SendResult {
    pub partition: u32,
    pub offset: i64,
}

/// A writer on a topic. Other producers, in this process or others, can write to the same topic at the same time.
///
/// Sends run in call order, even without awaiting each Promise. With
/// `fsync: 'always'`, a message is on disk when its Promise resolves.
#[napi]
pub struct Producer {
    topic: String,
    worker: Worker<mojay::Producer>,
}

impl Producer {
    pub fn start(topic: String, producer: mojay::Producer) -> Result<Self, BindingError> {
        Ok(Self {
            worker: Worker::spawn(&format!("mojay-producer-{topic}"), producer)?,
            topic,
        })
    }
}

#[napi]
impl Producer {
    #[napi(getter)]
    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    /// Sends a message. The Promise resolves to its partition and offset.
    #[napi(ts_return_type = "Promise<SendResult>")]
    pub fn send<'env>(&self, env: &'env Env, message: Message) -> napi::Result<Object<'env>> {
        let partition = message.partition;
        let message = mojay::Message {
            key: message.key.map(bytes),
            value: message.value.map(bytes),
            headers: message
                .headers
                .unwrap_or_default()
                .into_iter()
                .map(|(key, value)| mojay::Header {
                    key,
                    value: bytes(value),
                })
                .collect(),
            timestamp: message.timestamp,
        };
        self.worker.call(
            env,
            move |producer| {
                Ok(match partition {
                    Some(p) => (p, producer.send_to(p, &message)?),
                    None => producer.send(&message)?,
                })
            },
            |_, (partition, sent)| {
                Ok(SendResult {
                    partition,
                    offset: offset(sent),
                })
            },
        )
    }

    /// Forces everything sent so far to be written to disk.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn flush<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.worker
            .call(env, |producer| Ok(producer.flush()?), |_, ()| Ok(()))
    }

    /// Finishes the pending sends, then closes the producer.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn close<'env>(&mut self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.worker.close(env)
    }
}
