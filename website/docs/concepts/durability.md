---
sidebar_position: 5
title: Durability
description: The three fsync policies, what each one survives, what they cost, and how to choose.
---

Writing to a file does not put the data on disk: the operating system keeps it
in memory (the page cache) and writes it later. Only `fsync` forces it to the
disk. Each topic chooses when Mojay calls `fsync`, with its **fsync policy**,
set at creation.

## The three policies

| Policy | Rust | Node.js | CLI | When `fsync` happens |
| --- | --- | --- | --- | --- |
| Always (default) | `FsyncPolicy::Always` | `fsync: 'always'` | `--fsync always` | Before every send returns, and before its messages become readable. Shared between concurrent writers. |
| Interval | `FsyncPolicy::Interval(Duration)` | `fsync: 100` (ms) | `--fsync 100ms` | Periodically, on a background thread of each producer. |
| Never | `FsyncPolicy::Never` | `fsync: 'never'` | `--fsync never` | Left to the operating system. |

```rust
use std::time::Duration;
use mojay::{FsyncPolicy, LogConfig, TopicConfig};

let config = TopicConfig {
    partitions: 4,
    log: LogConfig { fsync: FsyncPolicy::Interval(Duration::from_millis(100)), ..Default::default() },
};
```

## What each policy survives

There are two kinds of failure to think about:

- **A process crash** (a panic, `kill -9`, an out-of-memory kill): the operating
  system keeps running, and its page cache with it. Everything the process
  passed to `write` survives, whatever the policy.
- **A power loss or a kernel crash**: the page cache is lost. Only what was
  flushed with `fsync` survives.

| Policy | Survives a process crash | Survives a power loss |
| --- | --- | --- |
| `Always` | Yes | Yes: a send that returned is on disk. |
| `Interval(d)` | Yes | Except the messages of the last interval `d`. |
| `Never` | Yes | Not guaranteed: whatever the OS had not written yet is lost. |

Whatever the policy, a segment is flushed when it is sealed (when the partition
moves on to the next segment). With `Interval` and `Never`, only the end of the
active segment is at risk.

With `Always`, a message only becomes readable once it is on disk, so a consumer
can never process a message that a power loss then takes back. With `Interval`
and `Never`, messages are readable as soon as they are written. After a power
loss, a consumer may have committed offsets beyond what survived; it then
resumes at the new [read limit](./offsets-and-commits.md#where-a-consumer-starts),
so that no new message is skipped.

## What it costs

`fsync` is the expensive part of a write. On macOS, a flush that really reaches
the disk uses `F_FULLFSYNC`, which takes about 3 to 5 ms on an internal SSD.
Measured on a Mac M5 with its internal SSD (see [Benchmarks](../benchmarks.md)):

| Case | Throughput |
| --- | --- |
| `Always`, one message per send, one writer | about 300 messages/s |
| `Always`, batches of 100 | about 27,000 messages/s |
| `Always`, 4 writers on one partition | about 580 messages/s |
| `Interval` or `Never`, one message per send | about 250,000 to 450,000 messages/s |
| `Interval` or `Never`, batches of 100 | about 5 to 6 million messages/s of 100 B, about 900,000/s of 1.25 KiB JSON (over 1 GiB/s) |

On other machines the numbers change with the disk and the file system, but the
shape stays the same: with `Always`, throughput is bounded by the number of
flushes, not by the amount of data.

### Group commit

With `Always`, writers share flushes. One `fsync` makes durable everything that
was written to the file before it, by any writer. So when several producers (in
one or several processes) write to the same partition, a writer that finds a
flush already in progress waits for it, and the next flush covers the messages
of every writer that was waiting. Four writers share flushes instead of queuing
them one after another. See
[Write path and group commit](../how-it-works/write-path-and-group-commit.md).

## Choosing a policy

- **Keep `Always`** when a lost message is a real problem (payments, orders,
  anything you acknowledge to someone else) and your rate is modest, or you can
  batch.
- **Batch with `send_batch_to`** when you produce many messages at once: you keep
  the guarantees of `Always` and divide the cost of flushes by the batch size.
- **Use `Interval`** when you need high throughput with single sends, and can
  accept losing the last fraction of a second on a power loss. 100 ms is a
  reasonable start. Call `producer.flush()` at the points where you need a
  guarantee.
- **Use `Never`** for data you can rebuild: caches, derived views, tests, local
  development.

Whatever you choose, Mojay never shows a half-written message, and a crash never
corrupts the partition: the next writer [repairs](../how-it-works/recovery.md)
the end of an interrupted write.
