---
sidebar_position: 6
title: Retention
description: Limit how much disk a topic uses, by size and by age. By default Mojay keeps everything.
---

Retention deletes old messages to bound the disk space of a topic. **By
default, there is none: Mojay keeps everything.** Deleting data nobody asked to
delete would be a bad surprise.

## Limits

A topic can have a size limit, an age limit, or both. They are fixed at
creation, and apply to each partition separately.

| Limit | Rust (`log.retention`) | Node.js | CLI | Meaning |
| --- | --- | --- | --- | --- |
| Size | `max_bytes: Some(bytes)` | `retentionBytes` | `--retention-size 10GiB` | Maximum size of **one partition**, active segment included. |
| Age | `max_age: Some(duration)` | `retentionMs` | `--retention-time 7d` | Maximum age of a sealed segment, measured from its last write. |

```rust
use std::time::Duration;
use mojay::{LogConfig, Retention, TopicConfig};

let config = TopicConfig {
    partitions: 3,
    log: LogConfig {
        segment_max_bytes: 16 * 1024 * 1024,
        retention: Retention {
            max_bytes: Some(1024 * 1024 * 1024),               // 1 GiB per partition
            max_age: Some(Duration::from_secs(7 * 24 * 3600)), // 7 days
        },
        ..Default::default()
    },
};
db.create_topic("events", config)?;
```

The size limit is per partition: a topic with 3 partitions and
`max_bytes` of 1 GiB can use up to about 3 GiB.

## Whole segments only

Each partition is stored as a series of segment files. New messages go to the
**active segment**; when it reaches `segment_max_bytes` (64 MiB by default), it is
**sealed** and a new active segment starts.

Retention only deletes **whole sealed segments**, from the oldest to the most
recent, and never the active segment. As a consequence:

- the partition stays a continuous sequence of offsets, which simply starts
  further along: there is never a hole;
- the limits are approximate, to the size of a segment. A partition can exceed
  its size limit when its active segment alone is larger than the limit, and a
  message older than `max_age` stays until its whole segment is old enough and
  sealed;
- smaller segments make retention finer, at the cost of more files. With a size
  limit, keep segments several times smaller than the limit.

The age of a segment is the date of its last write on disk, not the timestamps of
its messages: those come from producers' clocks and are not reliable.

## When retention is applied

There is no background server, so producers apply retention:

- when a producer opens the topic;
- for the size limit, on every append that moves to a new segment or puts the
  partition over its limit;
- for the age limit, when a new segment starts, and at most once per second
  while sending.

A producer that stays open without sending can call `producer.enforce_retention()`
regularly to apply the age limit during quiet periods. It returns the number of
segments deleted.

:::note
Retention is applied only while a producer is open on the topic. If nothing
writes to a topic, its old segments stay on disk until a producer opens it.
:::

## Readers and deleted segments

On Linux and macOS, a file that is deleted while a process has it open keeps its
disk space until the process closes it. Readers and consumers that keep reading
notice deleted segments within 500 ms and close them, so the space is actually
freed.

## Consumers that fall behind

If a group's committed offset, or a consumer's current position, falls in a
deleted segment, the consumer does not fail. It applies its `start_from` policy:
with `Earliest` (the default), it continues from the new first offset of the
partition. The deleted messages are never processed by that group. See
[Offsets and commits](./offsets-and-commits.md#when-retention-deletes-a-consumers-position).

A `TopicReader` asked for a deleted offset returns `Error::OffsetOutOfRange`,
with the new start of the partition in the error.

Use `mojay describe <topic>` to watch the start offset of each partition and the
lag of each group.
