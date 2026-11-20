---
sidebar_position: 1
title: Introduction
description: Mojay gives you Kafka's topics, partitions, offsets and consumer groups in a local folder, with no server.
slug: /intro
---

Mojay is a Rust library that gives you topics, partitions, offsets and consumer
groups in a plain local folder. There is no server, no broker and no Docker
container to run: you open a folder, you produce, you consume.

## SQLite for Kafka

SQLite showed that a lot of applications need a real database but not a
database server. Mojay applies the same idea to event logs. It keeps the
vocabulary and the guarantees of Kafka that matter on one machine, and replaces
the broker with files and file locks:

- The data folder plays the role of the SQLite database file. All the state
  lives inside it, nothing lives anywhere else.
- Several processes on the same machine can open the same folder at the same
  time. They coordinate through `flock` locks on files in that folder, the way
  SQLite processes coordinate through locks on the database file.
- Your application links the library and calls it directly. Nothing to start,
  monitor or upgrade separately.

## What you get

| Feature | In short |
| --- | --- |
| Topics and partitions | A topic is a name and a number of partitions, fixed at creation. Each partition is an append-only, ordered log. |
| Keyed ordering | Messages with the same key go to the same partition (murmur2 hash, the same as Kafka's default partitioner), so they are read in the order they were written. |
| Offsets | Each message gets an offset in its partition: a `u64` that starts at 0 and only grows. |
| Consumer groups | Consumers with the same group name share the partitions of the topics they read, and commit their positions. Members join, leave or crash, and the others rebalance automatically. |
| Several producers | Any number of producers, in any number of processes, can write to the same topic and even the same partition. They take turns, one batch at a time. |
| Durability you choose | Three `fsync` policies: on every write (the default), on an interval, or left to the operating system. |
| Retention | Optional limits per partition, by size and by age. By default, everything is kept. |
| Crash recovery | A write interrupted by a crash is detected and repaired by the next writer. Readers never see a partial message. |

Delivery to consumers is *at-least-once*: after a crash, the messages processed
since the last commit are read again.

## Three ways to use it

- **The Rust crate** `mojay`: the library itself, with a synchronous API. See
  [Getting started with Rust](./getting-started/rust.md).
- **The Node.js package** `mojay`: a native binding, with Promises that never
  block the event loop. See [Getting started with Node.js](./getting-started/nodejs.md).
- **The `mojay` command line**: create topics, send messages, follow a topic,
  check group lag and inspect segment files. See [Command line](./getting-started/cli.md).

All three read and write the same on-disk format, so you can, for example,
produce from a Node.js service and watch the topic with `mojay tail --follow`.

## When to use Mojay

Mojay fits when the producers and the consumers run on **one machine** and you
want Kafka's model without operating Kafka:

- **A local event log**: an append-only history of what happened in an
  application, replayable from any offset.
- **Job queues between processes**: a web process produces jobs, a pool of
  worker processes shares them as a consumer group, and a crashed worker's
  partitions are picked up by the others.
- **CLI tools and desktop applications** that need a durable queue or log but
  cannot ask their users to install a broker.
- **Tests and local development** of code that is designed around topics,
  partitions and consumer groups.
- **Edge devices and single servers** that buffer events locally, then ship
  them elsewhere when they can.

## When not to use it

- **Several machines.** Mojay coordinates through file locks on a local file
  system. It does not work across machines, and it must not be used on a
  network file system (NFS, SMB) or a cloud-synced folder.
- **Replication.** There is one copy of the data, on one disk. Mojay has no
  cluster, no replicas and no failover.
- **The Kafka wire protocol.** Mojay does not speak it: existing Kafka clients,
  Kafka Connect or Kafka Streams cannot connect to a Mojay folder. You use
  Mojay's own API.
- **Transactions or exactly-once delivery**, key compaction and compression are
  not part of version 1.

If you need any of these, use Kafka or another distributed log. If your whole
system fits on one machine, Mojay gives you the same programming model with a
folder instead of a cluster.

## Next steps

- [Getting started with Rust](./getting-started/rust.md)
- [Getting started with Node.js](./getting-started/nodejs.md)
- [Getting started with the command line](./getting-started/cli.md)
- [Topics and partitions](./concepts/topics-and-partitions.md), to understand the model
