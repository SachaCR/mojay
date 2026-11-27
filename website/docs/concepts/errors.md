---
sidebar_position: 8
title: Errors
description: The variants of mojay::Error and the Node.js error codes, what they mean and what to do.
---

Every fallible function in the Rust crate returns `mojay::Result<T>`, an alias
for `Result<T, mojay::Error>`. `mojay::Error` implements `std::error::Error`, so
`?` works in functions that return `Box<dyn std::error::Error>` too. In Node.js,
the same errors are thrown (or reject the Promise) as `Error` objects with a
stable `code`.

## Rust: `mojay::Error`

| Variant | Node.js code | Meaning | What to do |
| --- | --- | --- | --- |
| `TopicExists(name)` | `MOJAY_TOPIC_EXISTS` | `create_topic` on a topic that already exists. | Ignore it if you create topics at start-up: the topic is there. Its configuration is the one it was created with. |
| `UnknownTopic(name)` | `MOJAY_UNKNOWN_TOPIC` | The topic does not exist (`producer`, `reader`, `consumer`, `describe_topic`…). | Create it first, or wait for the process that creates it. |
| `UnknownPartition { partition, partitions }` | `MOJAY_UNKNOWN_PARTITION` | A partition number the topic does not have. | Use a number below `partitions`. |
| `InvalidName { name, reason }` | `MOJAY_INVALID_NAME` | A topic or group name that is empty, longer than 249 characters, starts with `.`, or has characters other than ASCII letters, digits, `.`, `_` and `-`. | Fix the name. |
| `InvalidConfig(reason)` | `MOJAY_INVALID_CONFIG` | A configuration Mojay refuses: partitions outside 1 to 10,000, a segment size out of range, an `fsync` interval of zero, a consumer with no topic or the same topic twice. | Fix the configuration. The message says what is wrong. |
| `RecordTooLarge { size, max }` | `MOJAY_RECORD_TOO_LARGE` | An encoded message larger than 16 MiB (`MAX_RECORD_SIZE`). Nothing was written. | Split the message, or store large payloads elsewhere and send a reference. |
| `InvalidRecord(reason)` | `MOJAY_INVALID_RECORD` | A message that cannot be encoded: more than 65,535 headers, or a header key longer than 65,535 bytes. Nothing was written. | Fix the message. |
| `OffsetOutOfRange { offset, start, end }` | `MOJAY_OFFSET_OUT_OF_RANGE` | `TopicReader::read` at an offset before the start of the partition (deleted by retention) or beyond its read limit. | Read from `start`, or wait for new messages if you are at `end`. Consumers handle this case themselves. |
| `Corrupt { path, reason }` | `MOJAY_CORRUPT` | Data on disk that Mojay cannot use and must not repair on its own. See below. | Inspect the file named in `path`. Do not delete files by hand before you understand the cause. |
| `Io(io::Error)` | `MOJAY_IO` | An error from the file system: full disk, permissions, too many open files… | Check the underlying error. With the `Interval` policy, a background `fsync` failure is returned by the producer's next call. |
| `WorkerStopped` | — | Rust async API only (`tokio` feature): the helper thread of an `AsyncProducer` or `AsyncConsumer` stopped after a panic, a bug in Mojay. | Drop it and open a new one, and please report the bug. |

### `Corrupt` in more detail

Mojay repairs on its own only what an interrupted write can leave behind: the
incomplete end of a partition's active segment. Anything else that looks wrong
is reported as `Error::Corrupt` instead of being repaired silently, because
"repairing" it could mean throwing away data. It is raised, for example, for:

- a damaged **sealed** segment (a bad checksum, or an unreadable header);
- segments whose offsets do not follow each other;
- an unreadable record **before** the read limit;
- a damaged committed offset file in a group;
- a `mojay.toml` or `meta.toml` that cannot be read, or a format version this
  version of Mojay does not support. In particular, topics created by an older
  Mojay with format version 1 (single writer) are refused: recreate them.

`mojay dump` shows what a segment file contains, record by record, and where it
stops being readable, without modifying anything. See [Command line](../getting-started/cli.md#dump)
and [Recovery](../how-it-works/recovery.md).

### Matching on errors

```rust
use mojay::{Error, TopicConfig};

match db.create_topic("orders", TopicConfig::default()) {
    Ok(()) | Err(Error::TopicExists(_)) => {}
    Err(Error::InvalidName { name, reason }) => eprintln!("bad topic name {name}: {reason}"),
    Err(error) => return Err(error.into()),
}
```

## Node.js error codes

Every error has a `code` property. Use it in your code: `error.message` is meant
for humans and may change.

| Code | Cause |
| --- | --- |
| `MOJAY_TOPIC_EXISTS` | `createTopic` on a topic that already exists. |
| `MOJAY_UNKNOWN_TOPIC` | No such topic. |
| `MOJAY_UNKNOWN_PARTITION` | No such partition. |
| `MOJAY_INVALID_NAME` | Invalid topic or group name. |
| `MOJAY_INVALID_CONFIG` | Configuration refused by Mojay (0 partitions, segment too small, `fsync: 0`…). |
| `MOJAY_INVALID_ARGUMENT` | An option with a value the binding does not accept, such as `fsync: 'sometimes'`, `startFrom: 'middle'` or a negative `retentionBytes`. |
| `MOJAY_RECORD_TOO_LARGE` | Message larger than 16 MiB. |
| `MOJAY_INVALID_RECORD` | Message that cannot be encoded (header key too long…). |
| `MOJAY_OFFSET_OUT_OF_RANGE` | Offset outside the partition's bounds. |
| `MOJAY_CORRUPT` | Unreadable data on disk. |
| `MOJAY_IO` | File system error. |
| `MOJAY_CLOSED` | A method called on a producer or consumer after `close()`. |

`MOJAY_INVALID_ARGUMENT` and `MOJAY_CLOSED` only exist in Node.js; every other
code matches a `mojay::Error` variant above.

```js
try {
  db.createTopic('orders', { partitions: 3 })
} catch (error) {
  if (error.code !== 'MOJAY_TOPIC_EXISTS') throw error
}
```
