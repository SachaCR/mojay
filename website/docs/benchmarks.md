---
sidebar_position: 5
title: Benchmarks
description: Throughput and latency of producing, reading and consuming with each fsync policy, measured on one machine.
---

These numbers give an order of magnitude, not a promise: they come from one machine, and the `fsync = always` results depend almost entirely on how fast the disk flushes.

## Machine and method

- **Machine**: Apple M5 (10 cores), 16 GB of memory, macOS 26.6.2, internal SSD (APFS).
- **Build**: release, with group commit.
- **Method**: each scenario runs for 2 seconds in its own temporary data folder, deleted at the end. Latencies go into a fixed-size histogram and reads are done in 1 MiB batches, so memory stays bounded: the whole run took 40 s and **9.8 MB of memory at most** (maximum resident set size).

The benchmark program lives in [`bench/`](https://github.com/SachaCR/mojay/tree/main/bench) and uses only the public API. To run it on your machine:

```bash
cargo run --release -p mojay-bench
```

```bash
# one scenario, for longer
cargo run --release -p mojay-bench -- --only "produce never" --seconds 5
```

## Results

| scenario | messages | msg/s | MiB/s | latency | p50 | p99 |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| produce always, 100 B, batch 1 | 615 | 307 | 0.0 | per send | 3.1 ms | 5.2 ms |
| produce always, 1024 B, batch 1 | 553 | 276 | 0.3 | per send | 3.9 ms | 5.2 ms |
| produce always, 100 B, batch 100 | 54,100 | 27.0 k | 2.6 | per batch | 4.2 ms | 5.8 ms |
| produce interval 100ms, 100 B, batch 1 | 783,387 | 391.7 k | 37.4 | per send | 2 µs | 6 µs |
| produce interval 100ms, 1024 B, batch 1 | 476,972 | 238.5 k | 232.9 | per send | 3 µs | 12 µs |
| produce interval 100ms, 100 B, batch 100 | 12,924,700 | 6.45 M | 615.0 | per batch | 9 µs | 49 µs |
| produce never, 100 B, batch 1 | 885,637 | 442.8 k | 42.2 | per send | 2 µs | 5 µs |
| produce never, 1024 B, batch 1 | 563,309 | 281.7 k | 275.1 | per send | 3 µs | 9 µs |
| produce never, 100 B, batch 100 | 11,433,400 | 5.69 M | 542.4 | per batch | 9 µs | 74 µs |
| 1 writer on 1 partition, always, 100 B | 553 | 276 | 0.0 | per send | 3.9 ms | 6.3 ms |
| 2 writers on 1 partition, always, 100 B | 608 | 304 | 0.0 | per send | 7.3 ms | 9.4 ms |
| 4 writers on 1 partition, always, 100 B | 1,162 | 580 | 0.1 | per send | 7.3 ms | 11.5 ms |
| 1 writer on 1 partition, never, 100 B | 839,886 | 419.9 k | 40.0 | per send | 2 µs | 6 µs |
| 2 writers on 1 partition, never, 100 B | 528,189 | 264.1 k | 25.2 | per send | 3 µs | 90 µs |
| 4 writers on 1 partition, never, 100 B | 554,996 | 277.4 k | 26.5 | per send | 3 µs | 360 µs |
| read raw (TopicReader), 100 B | 300,000 | 5.42 M | 516.7 | | | |
| read with a consumer group, 100 B | 300,000 | 4.86 M | 463.3 | | | |
| producer → consumer latency, always, 1,000 msg/s | 2,000 | 286 | 0.0 | delivery | 4.7 ms | 10.5 ms |
| producer → consumer latency, never, 1,000 msg/s | 2,000 | 1,000 | 0.0 | delivery | 721 µs | 2.6 ms |

The latency columns measure different things depending on the row:

- **per send** and **per batch**: the time a `send` call takes;
- **delivery**: the time from just before `send` until the message comes out of the consumer's `poll`.

:::note[Read numbers vary]
Read throughput varies a lot from run to run on the same build: 5 to 15 million messages per second raw, 5 to 9 million through a consumer group. It depends on the state of the page cache right after the topic is filled. Take the read rows as an order of magnitude.
:::

## Before group commit

The same machine, with the flush done under the partition's write lock (and the index flushed on every send):

| scenario | msg/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| produce always, 100 B, batch 1 | 205 | 5.2 ms | 9.4 ms |
| produce always, 100 B, batch 100 | 14.6 k | 7.3 ms | 10.5 ms |
| 1 writer on 1 partition, always, 100 B | 148 | 6.3 ms | 14.7 ms |
| 2 writers on 1 partition, always, 100 B | 148 | 6.3 ms | 335.5 ms |
| 4 writers on 1 partition, always, 100 B | 168 | 6.3 ms | 805.3 ms |

[Write path and group commit](./how-it-works/write-path-and-group-commit.md) explains the change.

## What the numbers say

- **`always` is bounded by the disk flush: about 300 messages per second** for a producer sending one message at a time. On macOS each `fsync` is an `F_FULLFSYNC`, which really empties the drive's cache, and takes about 3 to 5 ms. Batching amortizes it: batches of 100 messages reach 27,000 msg/s, with the same latency per call.
- **Group commit lets concurrent writers share flushes.** With 4 writers on one partition, `always` reaches about 580 msg/s, 3.5 times as much as before, and the p99 latency drops from 805 ms to 11.5 ms. The write lock is now held for microseconds, so the unfairness of `flock` no longer shows. Single writers got faster too: a send now flushes only the `.log`, not the index, which is a cache rebuilt from the log.
- **`interval` and `never` are about 1,000 times faster**: 250,000 to 450,000 single sends per second, 5 to 6 million per second in batches of 100, a few microseconds per send. With `never`, contention between writers costs about a third of the throughput and adds latency in the hundreds of microseconds.
- **Reading is fast**: 5 to 15 million messages per second raw, 5 to 9 million through a consumer group.
- **Delivery latency** from producer to consumer is under 1 ms at the median and under 3 ms at p99 with `never`. With `always`, it is dominated by the flush.

:::tip[Need more throughput with `always`?]
Each writer gets about 300 sends per second on this machine. To go beyond, send in batches, let several writers share flushes, or choose `fsync = interval` if losing the last interval on a power loss is acceptable. See [Durability](./concepts/durability.md).
:::
