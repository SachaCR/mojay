---
sidebar_position: 2
title: Producers
description: Sending messages one by one or in batches, flushing, and running several producers in one or several processes.
---

A **producer** appends messages to a topic. Open one with `db.producer(topic)`;
it stays usable until you drop it (Rust) or `close()` it (Node.js).

## Sending

| Rust | Node.js | Returns |
| --- | --- | --- |
| `send(&message)` | `send(message)` | The partition and offset of the message. The partition comes from the key, or round-robin. |
| `send_to(partition, &message)` | `send({ ...message, partition })` | The offset of the message. |
| `send_batch_to(partition, &messages)` | — | The range of offsets given to the messages, in order. |
| `flush()` | `flush()` | Nothing: forces everything this producer appended to disk. |

```rust
use mojay::Message;

let mut producer = db.producer("orders")?;

let (partition, offset) = producer.send(&Message::new("client-42", "created"))?;
let offset = producer.send_to(2, &Message::with_value("ping"))?;

let batch = vec![Message::with_value("a"), Message::with_value("b"), Message::with_value("c")];
let range = producer.send_batch_to(0, &batch)?; // for example 10..13
```

What happens when a call returns depends on the topic's `fsync` policy:

- With `Always` (the default), the messages are on disk, and readable by
  consumers.
- With `Interval` or `Never`, the messages are written to the operating system
  and readable by consumers, but may not be on disk yet.

See [Durability](./durability.md) for what each policy survives.

## Batches

`send_batch_to` appends several messages to one partition in a single
operation: one lock, one write and, with `Always`, one disk flush for the whole
batch. With `Always`, this is by far the most effective way to raise
throughput: on the reference machine, batches of 100 reach about 27,000 messages
per second, against about 300 for single sends.

A batch is all or nothing on validation: if one message is too large or invalid,
none of them is written, and the call returns `Error::RecordTooLarge` or
`Error::InvalidRecord`.

Messages in a batch without their own timestamp all get the same one, taken
when the batch is appended.

The Node.js binding has no batch method yet: each `send` is its own write.

## Flushing

`flush()` forces everything this producer has appended to disk, whatever the
topic's policy. Use it with `Interval` or `Never` when you reach a point where
the data must survive a power loss, for example before acknowledging an
external request. With `Always`, every send is already on disk.

When a producer with an `Interval` policy is dropped, it flushes one last time.
An error at that point can no longer be reported, so call `flush()` yourself
before dropping it if you need to know.

## Several producers

Any number of producers can write to the same topic, and to the same partition,
from the same process or from several processes. There is nothing to configure.

They take turns on each partition, like SQLite writers take turns on a
database: each partition has a write lock (a `flock` on a file), taken for one
batch at a time and held only for the few microseconds the write takes. Disk
flushes happen outside that lock, and concurrent writers share them (*group
commit*): one `fsync` covers the messages of every writer that was waiting.

What this guarantees:

- **One total order per partition.** Every message gets exactly one offset, and
  offsets follow the order in which writers took the lock.
- **Each producer's own messages keep their call order.**
- **Messages from different producers interleave** in time. If you need a strict
  order between two kinds of messages, send them from the same producer, or give
  them the same key.

A partition's throughput stays that of one writer at a time. With `Always`,
group commit lets several writers share the cost of flushes: on the reference
machine, four writers on one partition reach about 580 messages per second,
against about 300 for a single writer sending one message at a time. See
[Benchmarks](../benchmarks.md).

The details are in [Write path and group commit](../how-it-works/write-path-and-group-commit.md).

### When a producer crashes

If a process dies while it is writing, the operating system releases its locks.
The next writer on that partition detects the interrupted write and repairs the
end of the partition before appending. A message that was completely written
before the crash is kept, even if its `send` never returned: delivery is
*at-least-once*, so a producer that retries after a crash may write a message
twice. See [Recovery](../how-it-works/recovery.md).

## Retention is applied by producers

Mojay has no background server, so [retention](./retention.md) is applied by
producers:

- when a producer opens the topic;
- when a partition moves on to a new segment, or exceeds its size limit;
- for the age limit, at most once per second while sending.

A producer that sends nothing for a long time can call
`enforce_retention()` regularly to apply the age limit anyway. It returns the
number of segments deleted.

```rust
let deleted = producer.enforce_retention()?;
```

If no producer is open on a topic, retention is not applied.

## Errors when sending

| Error | Cause |
| --- | --- |
| `Error::UnknownPartition` | `send_to` or `send_batch_to` with a partition the topic does not have. |
| `Error::RecordTooLarge` | An encoded message larger than 16 MiB. Nothing was written. |
| `Error::InvalidRecord` | A message that cannot be encoded, such as more than 65,535 headers. Nothing was written. |
| `Error::Io` | A file system error, for example a full disk. With `Interval`, an `fsync` failure in the background is returned by the next call. |

See [Errors](./errors.md) for all of them.
