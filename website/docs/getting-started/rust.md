---
sidebar_position: 1
title: Rust
description: Add the mojay crate, create a topic, produce messages and consume them in a group.
---

This page builds a small program that creates a topic, writes messages to it
and reads them back as a member of a consumer group.

## Install

```bash
cargo add mojay
```

Mojay targets macOS and Linux, and needs Rust 1.88 or later. The API
reference is on [docs.rs](https://docs.rs/mojay).

## Open a data folder

Everything starts with a `Db`: a handle on a data folder, the equivalent of an
SQLite database file. `Db::open` creates the folder
if it does not exist.

```rust
use mojay::Db;

let db = Db::open("./data")?;
```

Opening is cheap and `Db` is `Clone`. Several processes can open the same
folder at the same time.

## Create a topic

A topic's configuration is fixed at creation. `TopicConfig` has two fields:
`partitions` and `log` (segment size, `fsync` policy, retention), and its
`Default` gives one partition, 64 MiB segments, `fsync` on every write and no
retention.

```rust
use mojay::TopicConfig;

db.create_topic("orders", TopicConfig { partitions: 3, ..Default::default() })?;
```

`create_topic` returns `Error::TopicExists` if the topic already exists. To
create it only the first time, match on that error:

```rust
match db.create_topic("orders", TopicConfig { partitions: 3, ..Default::default() }) {
    Ok(()) | Err(mojay::Error::TopicExists(_)) => {}
    Err(error) => return Err(error.into()),
}
```

All the options are described in [Topics and partitions](../concepts/topics-and-partitions.md#topic-configuration).

## Produce messages

`db.producer(topic)` opens a writer. A `Message` has an optional key, an
optional value, headers and an optional timestamp:

```rust
use mojay::Message;

let mut producer = db.producer("orders")?;

// With a key: the partition is chosen from the key (murmur2, like Kafka).
// All messages for "client-42" land in the same partition, in order.
let (partition, offset) = producer.send(&Message::new("client-42", r#"{"total": 129.9}"#))?;

// Without a key: partitions are used in turn (round-robin).
producer.send(&Message::with_value("hello"))?;

// A header, and an explicit partition.
let offset = producer.send_to(0, &Message::with_value("ping").header("source", "cli"))?;
```

`send` returns the partition and the offset of the message, `send_to` only the
offset.

With the default `fsync` policy, each send waits until the message is on disk.
That is safe, but it costs one disk flush per call. To write many messages,
send them as a batch: one lock, one write and one flush for the whole batch.

```rust
let batch: Vec<Message> = (0..100)
    .map(|i| Message::with_value(format!("event {i}")))
    .collect();
let offsets = producer.send_batch_to(1, &batch)?; // a Range<u64>, e.g. 0..100
```

Any number of producers, in this process or in others, can write to the same
topic at the same time. See [Producers](../concepts/producers.md).

## Consume messages

`db.consumer(group, topics, config)` opens a member of a consumer group. A
consumer always belongs to a group: when it is the only process using that
group name, it gets every partition.

```rust
use std::time::Duration;
use mojay::ConsumerConfig;

let mut consumer = db.consumer("billing", &["orders"], ConsumerConfig::default())?;

for record in consumer.poll(Duration::from_millis(500))? {
    println!("{}/{} @ {}: {:?}", record.topic, record.partition, record.offset, record.value);
}
consumer.commit()?;
```

- `poll(timeout)` waits at most `timeout` and returns the messages as soon as
  there are some. It returns an empty list if nothing arrived in time.
- `commit()` records, for the group, that everything returned by `poll` so far
  is processed. The next member to read these partitions, or this program when
  it restarts, resumes from there.

By default, the commit is manual and a group with nothing committed starts at
the oldest message. Both are set in `ConsumerConfig`:

```rust
use mojay::{ConsumerConfig, StartFrom};

let config = ConsumerConfig {
    start_from: StartFrom::Latest,             // only new messages, when nothing is committed
    auto_commit: Some(Duration::from_secs(1)), // commit automatically, at most once per second
    ..Default::default()
};
```

See [Consumers and groups](../concepts/consumers-and-groups.md) and
[Offsets and commits](../concepts/offsets-and-commits.md).

## Close

There is no `close` method: producers and consumers are closed when they are
dropped.

- Dropping a `Producer` releases its files. With an `Interval` `fsync` policy, it
  also flushes what is still pending. Call `producer.flush()?` before dropping it
  if you want to see a flush error.
- Dropping a `Consumer` with automatic commit commits its positions, then hands
  its partitions back to the other members of the group. With manual commit,
  call `commit()` first.

## The complete program

```rust title="src/main.rs"
use std::error::Error;
use std::time::Duration;

use mojay::{ConsumerConfig, Db, Message, TopicConfig};

fn main() -> Result<(), Box<dyn Error>> {
    let db = Db::open("./data")?;

    // Create the topic the first time only.
    match db.create_topic("orders", TopicConfig { partitions: 3, ..Default::default() }) {
        Ok(()) | Err(mojay::Error::TopicExists(_)) => {}
        Err(error) => return Err(error.into()),
    }

    // Produce: one keyed message, then a batch on partition 0.
    let mut producer = db.producer("orders")?;
    let (partition, offset) = producer.send(&Message::new("client-42", r#"{"total": 129.9}"#))?;
    println!("sent to partition {partition}, offset {offset}");

    let batch: Vec<Message> = (0..10)
        .map(|i| Message::with_value(format!("event {i}")).header("source", "example"))
        .collect();
    let offsets = producer.send_batch_to(0, &batch)?;
    println!("batch written at offsets {offsets:?}");
    drop(producer);

    // Consume as a member of the "billing" group, until nothing new arrives.
    let mut consumer = db.consumer("billing", &["orders"], ConsumerConfig::default())?;
    loop {
        let records = consumer.poll(Duration::from_millis(500))?;
        if records.is_empty() {
            break;
        }
        for record in &records {
            let value = record.value.as_deref().map(String::from_utf8_lossy);
            println!("{}/{} @ {}: {:?}", record.topic, record.partition, record.offset, value);
        }
        // Everything returned so far is processed: the group moves past it.
        consumer.commit()?;
    }
    Ok(())
}
```

Run it twice: the second run sends new messages, and the consumer only prints
those, because the `billing` group committed its position the first time.

To look at the data meanwhile, use the [command line](./cli.md):

```bash
mojay describe orders   # partitions, read limits, and the lag of the billing group
mojay tail orders       # the messages, without committing anything
```

## Next steps

- [Topics and partitions](../concepts/topics-and-partitions.md)
- [Durability](../concepts/durability.md): choose an `fsync` policy
- [Errors](../concepts/errors.md): what each `mojay::Error` variant means
