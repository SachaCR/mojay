---
slug: introducing-mojay
title: Introducing Mojay
authors: [sacha]
tags: [release, rust, nodejs]
---

Today I am publishing Mojay, a Rust library that gives you Kafka's model (topics, partitions, offsets and consumer groups) in a local folder, with no server to run. I like to describe it as "SQLite for Kafka": you open a folder, you produce, you consume. It comes with a Node.js binding and a small command-line tool.

{/* truncate */}

## Why "SQLite for Kafka"

I like Kafka's model a lot. An append-only log split into partitions, offsets that consumers commit, groups that share the work and pick it up again after a crash: it is a simple and robust way to pass work between programs. But Kafka is a distributed system. Using it means running brokers, keeping a cluster healthy, and paying for all of that even when everything happens on one machine.

Many applications live on one machine: a desktop app with a few background workers, a service and its batch jobs, an edge device, a CLI tool, a test suite. They want the model, not the cluster. SQLite showed long ago that a database can be a library and a file. Mojay tries to do the same for a log.

## What Mojay does

- **Topics and partitions** in a data folder. Messages have an optional key, a value, headers and a timestamp. Messages with the same key go to the same partition, using the same murmur2 hash as Kafka.
- **Several processes** on the same machine open the same folder at the same time. Any number of producers can write to the same topic and even the same partition; any number of readers can follow along.
- **Consumer groups without a coordinator.** Members share partitions and take over those of a member that stops or crashes. A lone consumer is simply a group of one. Committed offsets give at-least-once delivery.
- **No server, no daemon.** All the coordination goes through the file system, with `flock` locks and a small memory-mapped file.

## A quick look

In Rust:

```rust
use std::time::Duration;
use mojay::{ConsumerConfig, Db, Message, TopicConfig};

let db = Db::open("./data")?;
db.create_topic("orders", TopicConfig { partitions: 4, ..Default::default() })?;

let mut producer = db.producer("orders")?;
let (partition, offset) = producer.send(&Message::new("client-42", "order #1"))?;

let mut consumer = db.consumer("billing", &["orders"], ConsumerConfig::default())?;
for record in consumer.poll(Duration::from_millis(500))? {
    println!("{} / {}: {:?}", record.partition, record.offset, record.value);
}
consumer.commit()?;
```

The same in Node.js. Sends, polls and commits return Promises and run on a thread of their own for each producer and consumer, so they never block the event loop:

```ts
import { Db } from "mojay";

const db = Db.open("./data");
db.createTopic("orders", { partitions: 4 });

const producer = db.producer("orders");
const { partition, offset } = await producer.send({ key: "client-42", value: "order #1" });

const consumer = db.consumer("billing", ["orders"], { autoCommitMs: 1000 });
for (const record of await consumer.poll(500)) {
  console.log(record.partition, record.offset, record.value?.toString());
}
await Promise.all([producer.close(), consumer.close()]);
```

Run the consumer in two processes with the same group name and they split the partitions between them. Kill one, and the other takes over from the last commit.

## What makes it reliable

Mojay stores other people's data, so reliability came first, readability second and performance third.

- **Crash recovery.** Every record carries its length and a CRC32C. After a crash, only the end of the active segment, where an interrupted write can be, is truncated. A damaged sealed segment is reported as corrupt, never silently "repaired". The index is only a cache, rebuilt from the log when it is missing or wrong. Readers never modify anything on disk.
- **`fsync` by default.** With the default policy, a send returns, and its message becomes readable, only once it is on disk. A consumer can never see a message that a power loss would take back. If you can trade the last moments of data for speed, `interval` and `never` are one option away.
- **Group commit.** One `fsync` makes durable everything written before it, by any writer. So concurrent writers share their flushes instead of queuing behind each other.

The tests follow the same idea: the log is cut at every byte of its end and reopened, and the multi-process behavior is tested with real processes, killed in the middle of their writes.

## A few numbers

On my machine (an Apple M5 with its internal SSD):

- With `fsync` on every send, about **300 messages per second** per writer: each flush takes 3 to 5 ms. Batches of 100 messages reach **27,000 messages per second**.
- Group commit took **4 writers on one partition from 168 to 580 messages per second**, and their p99 latency from **805 ms to 11.5 ms**.
- With `interval` or `never`, **250,000 to 450,000 single sends per second**, and **5 to 6 million per second** in batches.
- Reading runs at **5 to 15 million messages per second**.

The [benchmarks page](/docs/benchmarks) has the full table and the method.

## What's next

Mojay targets macOS and Linux, on a local file system. It does not do replication, transactions, compaction or compression, and it does not speak Kafka's network protocol.

The Node.js binding, built with napi-rs, came first. Next, depending on demand, come a C ABI for other languages (Go, C++ and others) and a Python binding. Further out, I am thinking about a time index to seek by timestamp, compression, compaction by key, event-based notification instead of polling, and maybe a Kafka protocol layer on top of the library.

## Try it

Start with the [introduction](/docs/intro), then the [Rust getting started guide](/docs/getting-started/rust). If you want to know how it works inside, the [How it works](/docs/how-it-works/on-disk-layout) section walks through the on-disk format, crash recovery, group commit and consumer groups.

I would love to hear what you build with it, and what breaks.
