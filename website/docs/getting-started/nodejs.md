---
sidebar_position: 2
title: Node.js
description: Use Mojay from Node.js, with Promises that never block the event loop.
---

The `@mojay-dev/node` npm package is a native binding to the Rust library,
built with [napi-rs](https://napi.rs). It calls the library directly: there is
still no server or service to run. TypeScript definitions are included.

## Install

```bash
npm install @mojay-dev/node
```

You need Node.js 18 or later, on macOS or Linux (x64 or arm64). The package
ships prebuilt binaries, so installing it needs neither Rust nor a compiler.

## Import it

The package works with both module systems.

```js title="ESM"
import { Db } from '@mojay-dev/node'
```

```js title="CommonJS"
const { Db } = require('@mojay-dev/node')
```

## A first program

```js title="quickstart.mjs"
import { Db } from '@mojay-dev/node'

const db = Db.open('./data')

// Create the topic the first time only.
try {
  db.createTopic('orders', { partitions: 3 })
} catch (error) {
  if (error.code !== 'MOJAY_TOPIC_EXISTS') throw error
}

// Produce: the key picks the partition, so a customer's orders stay in order.
const producer = db.producer('orders')
for (const customer of ['client-1', 'client-2', 'client-1']) {
  const { partition, offset } = await producer.send({
    key: customer,
    value: JSON.stringify({ customer, total: 129.9 }),
    headers: { source: 'quickstart' },
  })
  console.log(`sent ${customer} -> partition ${partition}, offset ${offset}`)
}
await producer.close()

// Consume as a member of the "billing" group, with automatic commit.
const consumer = db.consumer('billing', ['orders'], { autoCommitMs: 1000 })
for (const record of await consumer.poll(1000)) {
  const order = JSON.parse(record.value.toString())
  console.log(`p${record.partition}:${record.offset}`, order)
}
await consumer.close() // commits, then hands the partitions back to the group
```

Run it again: the consumer only prints the new messages, because the `billing`
group committed its position when it closed.

## Synchronous and asynchronous calls

Opening things is synchronous; everything that may wait for the disk or for
messages returns a Promise.

| Synchronous | Returns a Promise |
| --- | --- |
| `Db.open`, `db.createTopic`, `db.listTopics`, `db.describeTopic`, `db.producer`, `db.consumer` | `producer.send`, `producer.flush`, `producer.close`, `consumer.poll`, `consumer.commit`, `consumer.assignment`, `consumer.rebalanceEvents`, `consumer.close` |

Each producer and each consumer has its own thread and its own queue of calls:

- **Nothing blocks Node.js.** A `poll` that waits, or an `fsync`, runs on that
  thread. Neither the event loop nor Node's thread pool (used by `fs` and
  `crypto`) is occupied.
- **Calls run in order.** Calls on the same producer or consumer run one at a
  time, in the order they were made, even if you do not await each Promise.
  `Promise.all(messages.map((m) => producer.send(m)))` writes the messages in
  array order.

## API

### `Db`

| Method | Purpose |
| --- | --- |
| `Db.open(path)` | Opens a data folder, creating it if needed. Several processes can open the same one. |
| `db.createTopic(name, options?)` | Creates a topic. Throws `MOJAY_TOPIC_EXISTS` if it already exists. |
| `db.listTopics()` | The topic names, sorted. |
| `db.describeTopic(name)` | `{ name, partitions }`, with for each partition `partition`, `startOffset`, `readLimit`, `segments` (a count) and `sizeBytes`. |
| `db.producer(topic)` | A writer on the topic. |
| `db.consumer(group, topics, options?)` | A member of the group `group`, subscribed to `topics`. |

`createTopic` options, all optional:

| Option | Default | Effect |
| --- | --- | --- |
| `partitions` | `1` | Number of partitions, fixed at creation. |
| `segmentBytes` | 64 MiB | Size of a segment file before moving to the next one. |
| `fsync` | `'always'` | `'always'`: each message is on disk before it becomes readable. `'never'`: left to the OS. A number: one `fsync` every N milliseconds. See [Durability](../concepts/durability.md). |
| `retentionBytes` | none | Maximum size of **one partition**, in bytes. Beyond it, the oldest segments are deleted. |
| `retentionMs` | none | Maximum age of a segment, in milliseconds since its last write. |

### `Producer`

| Member | Purpose |
| --- | --- |
| `producer.topic` | The topic name. |
| `send(message)` | Sends a message. Resolves to `{ partition, offset }`. |
| `flush()` | Forces everything sent so far to disk, whatever the `fsync` policy. |
| `close()` | Finishes the pending sends, then closes the producer. |

A message is `{ key?, value?, headers?, partition?, timestamp? }`:

- `key`, `value` and header values accept a `string` (encoded as UTF-8) or a
  `Buffer`.
- `key` picks the partition (murmur2, like Kafka). Without a key and without a
  `partition`, messages are spread round-robin.
- `partition` forces a partition instead of the one chosen from the key.
- Without a `value`, the message is a tombstone (null value).
- `timestamp` is in milliseconds since the epoch, and defaults to the time of
  sending.

:::tip[Throughput]
The binding sends one message per `send` call. With `fsync: 'always'`, each
call waits for the disk, which limits a producer to a few hundred messages per
second on a typical laptop SSD. If you need more, create the topic with an
interval policy such as `fsync: 100`. See [Durability](../concepts/durability.md).
:::

### `Consumer`

| Member | Purpose |
| --- | --- |
| `consumer.group` | The group name. |
| `poll(timeoutMs)` | Waits at most `timeoutMs` for messages. Resolves to a `ConsumerRecord[]`, empty if nothing arrived in time. |
| `commit()` | Commits the position of each owned partition: everything returned by `poll` so far is processed. |
| `assignment()` | The partitions this member owns: `[{ topic, partition }]`. |
| `rebalanceEvents()` | The partitions received or given up since the last call: `[{ kind: 'assigned' \| 'revoked', topic, partition }]`. |
| `close()` | Leaves the group: commits if auto-commit is on, then hands the partitions back to the other members. |

Consumer options, all optional:

| Option | Default | Effect |
| --- | --- | --- |
| `startFrom` | `'earliest'` | Where to start when the group has committed nothing for a partition: `'earliest'` or `'latest'`. |
| `autoCommitMs` | none | Commit automatically, at most every N milliseconds, at the start of each `poll` and on `close`. Without it, call `commit()` yourself. |
| `maxPollBytes` | 1 MiB | Approximate maximum size of the messages returned by one `poll`. |

A `ConsumerRecord` has `topic`, `partition`, `offset`, `timestamp` (ms), and
`key`, `value` and `headers` as `Buffer`s (`key` and `value` are absent when the
message has none).

### Offsets are numbers

Offsets are JavaScript `number`s. They are exact up to 2<sup>53</sup> messages
per partition.

## Close explicitly

`close()` releases locks and files immediately. A producer or consumer that you
forget is closed when the garbage collector collects it, with no guaranteed
delay: until then, a consumer keeps its partitions and the rest of its group
cannot read them.

```js
await Promise.all([producer.close(), consumer.close()])
```

Any method called after `close()` fails with `MOJAY_CLOSED`.

## Errors

Every error is a regular `Error` with a stable `code` property, like Node's own
errors:

```js
try {
  db.producer('orders')
} catch (error) {
  if (error.code === 'MOJAY_UNKNOWN_TOPIC') console.log('create the topic first')
  else throw error
}
```

Synchronous methods throw; asynchronous ones reject their Promise with the same
kind of error. Rely on `error.code` in your code: `error.message` is meant for
humans and may change. The full list of codes is in [Errors](../concepts/errors.md#nodejs-error-codes).

## Next steps

- [Topics and partitions](../concepts/topics-and-partitions.md)
- [Consumers and groups](../concepts/consumers-and-groups.md)
- [Command line](./cli.md), to look at the data your program writes
