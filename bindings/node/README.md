# Mojay for Node.js

Topics, partitions and consumer groups in a local folder, with no server:
the SQLite of Kafka, from Node.js.

```js
const { Db } = require('mojay')

const db = Db.open('./data')
db.createTopic('orders', { partitions: 3 })

const producer = db.producer('orders')
await producer.send({ key: 'client-42', value: JSON.stringify({ total: 129.9 }) })

const consumer = db.consumer('billing', ['orders'], { autoCommitMs: 1000 })
for (const record of await consumer.poll(1000)) {
  console.log(record.partition, record.offset, JSON.parse(record.value.toString()))
}
await Promise.all([producer.close(), consumer.close()])
```

The binding is written with [napi-rs](https://napi.rs): it calls the Rust
library directly, with no server or service to run. The TypeScript definitions
(`index.d.ts`) are generated from the Rust code.

## Building

You need Rust (`rustup`) and Node.js 18 or later. From `bindings/node`:

```bash
npm install
```

```bash
npm run build
```

`npm run build` compiles for **the architecture of the Node that runs the
command**. An x64 Node on an arm64 Mac (under Rosetta, common with nvm) needs
the matching Rust target; the script detects it and prints the command to run:

```bash
rustup target add x86_64-apple-darwin
```

To test: `npm run build:debug`, then `npm test`. For a complete example:
`node examples/quickstart.mjs`.

## API

### `Db`

| Method | Purpose |
| --- | --- |
| `Db.open(path)` | Opens a data folder, creating it if needed. Several processes can open the same one. |
| `db.createTopic(name, options?)` | Creates a topic (see below). |
| `db.listTopics()` | Topic names, sorted. |
| `db.describeTopic(name)` | For each partition: `startOffset`, `readLimit`, `segments`, `sizeBytes`. |
| `db.producer(topic)` | A writer on the topic. Any number of producers, in any process, can write to the same topic and partition. |
| `db.consumer(group, topics, options?)` | A member of the `group` group. |

`createTopic` options, all optional:

| Option | Default | Effect |
| --- | --- | --- |
| `partitions` | `1` | Number of partitions, fixed at creation. |
| `segmentBytes` | 64 MiB | Size of a segment before moving to the next one. |
| `fsync` | `'always'` | `'always'`: each message is on disk before it becomes readable. `'never'`: left to the OS. A number: one fsync every N ms. |
| `retentionBytes` | none | Maximum size of **one partition**; beyond it, the oldest segments are deleted. |
| `retentionMs` | none | Maximum age of a segment, since its last write. |

### `Producer`

| Method | Purpose |
| --- | --- |
| `send(message)` | Sends a message. `Promise<{ partition, offset }>`. |
| `flush()` | Forces everything sent so far onto disk. |
| `close()` | Finishes pending sends, then closes the producer's files. |

A message is `{ key?, value?, headers?, partition?, timestamp? }`. Key, value
and headers accept a `string` (encoded as UTF-8) or a `Buffer`. The key picks
the partition (murmur2, like Kafka): messages with the same key stay together,
in order. Without a `value`, the message is a tombstone.

### `Consumer`

| Method | Purpose |
| --- | --- |
| `poll(timeoutMs)` | Waits at most `timeoutMs` for messages. `Promise<ConsumerRecord[]>`, empty if nothing arrived in time. |
| `commit()` | Commits the position of each owned partition. |
| `assignment()` | The owned partitions: `[{ topic, partition }]`. |
| `rebalanceEvents()` | Partitions received (`'assigned'`) or given up (`'revoked'`) since the last call. |
| `close()` | Leaves the group: commits if auto-commit is on, then hands the partitions back. |

Options: `startFrom` (`'earliest'` by default, or `'latest'`), `autoCommitMs`
(automatic commit at most every N ms; without it, call `commit()` yourself),
`maxPollBytes`.

A read message (`ConsumerRecord`) has `topic`, `partition`, `offset`,
`timestamp` (ms), and `key`, `value`, `headers` as `Buffer`s.

The live members of a group share its partitions, whether they run in the same
process or in several. Delivery is *at-least-once*: after a crash, messages
since the last commit are read again.

## Behavior

- **Nothing blocks Node.** Each producer and each consumer has its own thread.
  `poll` waits and `fsync` calls happen there, without occupying the event loop
  or Node's thread pool (the one used by `fs` and `crypto`).
- **Call order is preserved.** Calls on the same producer or consumer run one at
  a time, in order, even without awaiting each Promise:
  `Promise.all(messages.map((m) => producer.send(m)))` writes the messages in
  array order.
- **Several writers.** Producers in the same process or in several take turns
  on each partition, one message (or batch) at a time: every message gets its
  own offset, and each producer's messages keep their order. Messages from
  different producers interleave in time.
- **Offsets are `number`s.** Exact up to 2^53 messages per partition.
- **Close explicitly.** `close()` releases locks and files immediately. A
  forgotten object is closed when the garbage collector collects it, but with
  no guaranteed delay: a topic would stay locked until then.

## Errors

Every error is an `Error` with a stable `code`, like Node's own errors:

```js
try {
  db.producer('orders')
} catch (error) {
  if (error.code === 'MOJAY_UNKNOWN_TOPIC') console.log('create the topic first')
}
```

| Code | Cause |
| --- | --- |
| `MOJAY_TOPIC_EXISTS` | `createTopic` on a topic that already exists. |
| `MOJAY_UNKNOWN_TOPIC` | No such topic. |
| `MOJAY_UNKNOWN_PARTITION` | No such partition. |
| `MOJAY_INVALID_NAME` | Invalid topic or group name (letters, digits, `.`, `_`, `-`). |
| `MOJAY_INVALID_CONFIG` | Configuration refused (0 partitions, segment too small…). |
| `MOJAY_INVALID_ARGUMENT` | Unknown option or invalid value (`fsync: 'sometimes'`, negative number…). |
| `MOJAY_RECORD_TOO_LARGE` | Message larger than 16 MiB. |
| `MOJAY_INVALID_RECORD` | Message that cannot be encoded (header key too long…). |
| `MOJAY_OFFSET_OUT_OF_RANGE` | Offset outside the partition's bounds. |
| `MOJAY_CORRUPT` | Unreadable data on disk. |
| `MOJAY_IO` | File system error. |
| `MOJAY_CLOSED` | Method called after `close()`. |

Rely on `error.code` in your code: messages (`error.message`) are meant for
humans and may change.

## Platforms

macOS and Linux, x64 and arm64, like the library. Windows is not supported in V1.
