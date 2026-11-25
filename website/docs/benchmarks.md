---
sidebar_position: 5
title: Benchmarks
description: Throughput and latency of producing, reading and consuming with each fsync policy, with synthetic and realistic JSON workloads, measured on one machine.
---

These numbers give an order of magnitude, not a promise: they come from one machine, and the `fsync = always` results depend almost entirely on how fast the disk flushes.

There are two suites:

- **[Micro](#micro-benchmarks)**: the cost of each primitive, with synthetic 100 B and 1 KiB values on one partition.
- **[Realistic](#realistic-workload)**: JSON events with keys and headers on 8 partitions, producer and consumer processes, a large backlog, a long run with retention.

## Machine and method

- **Machine**: Apple M5 (10 cores), 16 GB of memory, macOS 26.6.2, internal SSD (APFS).
- **Build**: release, with group commit.
- **Method**: each scenario runs for 2 seconds in its own temporary data folder, deleted at the end. Latencies go into a fixed-size histogram, so memory stays bounded whatever the number of messages.

The benchmark program lives in [`bench/`](https://github.com/SachaCR/mojay/tree/main/bench) and uses only the public API. To run it on your machine:

```bash
cargo run --release -p mojay-bench                       # micro suite, about 40 s
cargo run --release -p mojay-bench -- --suite realistic  # about 2 min, writes up to 2.5 GiB at a time
```

```bash
# one scenario, for longer, with the results also saved to compare runs
cargo run --release -p mojay-bench -- --only "produce never" --seconds 5 --csv run.csv
```

:::note[Measure Linux on Linux]
Numbers taken in a Docker container on a Mac say little about `fsync`: the container's disk lives in a virtual machine.
:::

## Micro benchmarks

The whole run took 40 s and **9.8 MB of memory at most** (maximum resident set size).

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

**messages** is how many messages the scenario sent or read in its run, not a rate: **msg/s** is the rate. A produce scenario runs for 2 seconds, so the 553 messages of one writer with `always` make 276 msg/s.

The latency columns measure different things depending on the row:

- **per send** and **per batch**: the time a `send` call takes;
- **delivery**: the time from just before `send` until the message comes out of the consumer's `poll`. In this suite the consumer reads one partition and never commits: see [Delivery](#delivery-producer--consumer-processes) below for a consumer that commits.

:::note[Read numbers vary]
Read throughput varies a lot from run to run on the same build: 5 to 15 million messages per second raw, 5 to 9 million through a consumer group. It depends on the state of the page cache right after the topic is filled. Take the read rows as an order of magnitude.
:::

### Before group commit

The same machine, with the flush done under the partition's write lock (and the index flushed on every send):

| scenario | msg/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| produce always, 100 B, batch 1 | 205 | 5.2 ms | 9.4 ms |
| produce always, 100 B, batch 100 | 14.6 k | 7.3 ms | 10.5 ms |
| 1 writer on 1 partition, always, 100 B | 148 | 6.3 ms | 14.7 ms |
| 2 writers on 1 partition, always, 100 B | 148 | 6.3 ms | 335.5 ms |
| 4 writers on 1 partition, always, 100 B | 168 | 6.3 ms | 805.3 ms |

[Write path and group commit](./how-it-works/write-path-and-group-commit.md) explains the change.

### What the numbers say

- **`always` is bounded by the disk flush: about 300 messages per second** for a producer sending one message at a time. On macOS each `fsync` is an `F_FULLFSYNC`, which really empties the drive's cache, and takes about 3 to 5 ms. Batching amortizes it: batches of 100 messages reach 27,000 msg/s, with the same latency per call.
- **Group commit lets concurrent writers share flushes.** With 4 writers on one partition, `always` reaches about 580 msg/s, 3.5 times as much as before, and the p99 latency drops from 805 ms to 11.5 ms. The write lock is now held for microseconds, so the unfairness of `flock` no longer shows. Single writers got faster too: a send now flushes only the `.log`, not the index, which is a cache rebuilt from the log.
- **`interval` and `never` are about 1,000 times faster**: 250,000 to 450,000 single sends per second, 5 to 6 million per second in batches of 100 messages of 100 B, a few microseconds per send. With `never`, contention between writers costs about a third of the throughput and adds latency in the hundreds of microseconds.
- **Reading is fast**: 5 to 15 million messages per second raw, 5 to 9 million through a consumer group.
- **Delivery latency** from producer to consumer is under 1 ms at the median and under 3 ms at p99 with `never`, for a consumer that does not commit. With `always`, it is dominated by the flush.

:::tip[Need more throughput with `always`?]
A single writer gets about 300 sends per second on this machine. To go beyond, send in batches, let several writers share flushes, or choose `fsync = interval` if losing the last interval on a power loss is acceptable. See [Durability](./concepts/durability.md).
:::

## Realistic workload

The same machine. The whole run took 108 s and **44 MB of memory at most**.

### Messages

Generated from a seed, so every run writes the same bytes. Each event has a key (the user, customer or document: keys repeat, and the partitioner spreads them over 8 partitions) and two headers, `event-type` and `trace-id`. Sizes count key, value and headers.

| shape | average | what it is |
| --- | ---: | --- |
| `click` | 475 B | page view: user, session, URL, user agent, viewport |
| `order` | 1.3 KiB | order with 3 to 10 line items, two addresses, payment |
| `document` | 35 KiB | document with 24 to 40 sections of text |
| `mixed` | 1.25 KiB | 80 % clicks, 18 % orders, 2 % documents |

### Ingestion

One producer, keyed `send` (one message at a time) or `send_batch_to` (100 messages, partitions in turn). *Fan-in*: several producer **processes** writing to the same topic. Latency is the time a `send` or a batch takes.

| scenario | msg/s | MiB/s | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: |
| click, always, keyed send | 273 | 0.1 | 4.1 ms | 4.1 ms | 4.1 ms |
| document, always, keyed send | 274 | 9.1 | 4.2 ms | 4.2 ms | 4.3 ms |
| mixed, always, keyed send | 270 | 0.4 | 4.2 ms | 4.2 ms | 7.0 ms |
| mixed, always, batch 100 | 27.9 k | 33.7 | 4.2 ms | 4.2 ms | 5.6 ms |
| click, interval 100ms, keyed send | 231.5 k | 105.0 | 2 µs | 6 µs | 23 µs |
| document, interval 100ms, keyed send | 35.8 k | 1190.4 | 11 µs | 41 µs | 590 µs |
| mixed, interval 100ms, keyed send | 179.1 k | 214.4 | 3 µs | 13 µs | 33 µs |
| mixed, interval 100ms, batch 100 | 892.5 k | 1079.0 | 31 µs | 246 µs | 18.9 ms |
| mixed, never, keyed send | 312.9 k | 374.4 | 2 µs | 9 µs | 15 µs |
| mixed, never, batch 100 | 819.8 k | 991.2 | 29 µs | 229 µs | 25.2 ms |
| fan-in, always, 1 process | 310 | 0.4 | 3.1 ms | 4.2 ms | 7.2 ms |
| fan-in, always, 4 processes | 370 | 0.4 | 11.5 ms | 21.0 ms | 24.0 ms |
| fan-in, never, 1 process | 238.2 k | 307.2 | 3 µs | 20 µs | 82 µs |
| fan-in, never, 4 processes | 192.7 k | 238.5 | 11 µs | 123 µs | 459 µs |

On disk, segments take about 4 % more than the payload (record headers and CRCs).

### Delivery: producer → consumer processes

A producer at a fixed rate; consumers in other processes, with automatic commit every second. Latency runs from the time each message was **due** to be sent, so a producer that falls behind counts too.

| scenario | msg/s | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| 1 consumer, always | 100 | 6.8 ms | 15.7 ms | 35.2 ms |
| 1 consumer, interval 100ms | 1,000 | 1.4 ms | 75.5 ms | 75.5 ms |
| 1 consumer, never | 1,000 | 852 µs | 6.8 ms | 21.0 ms |
| 1 consumer, never | 10,000 | 786 µs | 1.6 ms | 7.9 ms |
| 1 consumer, never | 50,000 | 147 µs | 5.8 ms | 15.7 ms |
| work queue, 4 consumer processes, never | 10,000 | 721 µs | 4.7 ms | 13.6 ms |
| 2 consumers, a 3rd joins, the 1st is killed, never | 10,000 | 852 µs | 369.1 ms | 416.9 ms |
| 3 groups reading the same topic, never | 10,000 | 786 µs | 7.9 ms | 21.0 ms |

### Backlog, long run, reopen

| scenario | result |
| --- | --- |
| a group reads a 1 GiB backlog (790,300 messages, warm cache) | 850 k msg/s, 1.1 GiB/s |
| 30 s at 20,000 msg/s, interval 100ms, 8 MiB segments, retention 32 MiB per partition | delivery p50 1.6 to 5.8 ms and p99 76 to 92 ms in every 5 s window; disk size flat at 223 to 232 MiB |
| open a producer on 1 GiB of active segments, index intact | 4.9 ms |
| the same with every index deleted, as after losing them | 573 ms: the log is scanned at 1.8 GiB/s |

### Before one offsets file per topic

Until data folder format 2, a group's committed offsets were one file per partition, each replaced with two `fsync`. A consumer committing 8 partitions spent 40 to 70 ms in `poll` once per second, and that set the p99:

| scenario | msg/s | p99 before | p99 now |
| --- | ---: | ---: | ---: |
| 1 consumer, always | 100 | 75.5 ms | 15.7 ms |
| 1 consumer, never | 10,000 | 41.9 ms | 1.6 ms |
| work queue, 4 consumer processes, never | 10,000 | 15.7 ms | 4.7 ms |
| 3 groups reading the same topic, never | 10,000 | 151.0 ms | 7.9 ms |

[Consumer groups without a server](./how-it-works/consumer-groups-without-a-server.md#committed-offsets) describes the file.

### What the numbers say

- **Real messages do not change the write picture.** `always` stays at about 270 sends per second whatever the size, from 475 B to 35 KiB: the flush costs the same. With `never`, a keyed send takes 2 µs, and batches go beyond 1 GiB/s.
- **Several producer processes** cost about 20 % of the throughput with `never`, and raise the p99 from 20 to 123 µs: they take turns on each partition.
- **Delivery stays in milliseconds with commits**: p99 1.6 ms at 10,000 msg/s with `never`. What remains above the median is mostly the commit, two flushes once per second.
- **`interval` has a p99 of about 75 ms**, in delivery as in the long run. Single sends with `interval` also show maximums of 60 to 75 ms: the 100 ms background flush seems to hold sends up while it runs. This is being looked at.
- **A crashed member stalls its partitions for about 0.4 s**: the others take them over at their next rebalance (every 200 ms), and the messages since its last commit are delivered again.
- **Long runs are stable**: rate, latency and disk size stay flat while retention deletes segments.
- **Losing an index is cheap**: rebuilding it scans 1 GiB in about 0.6 s.

## Node.js binding

`npm run bench` from `bindings/node`, after `npm run build`. Same messages and 8 partitions; producer and consumer in the same process.

| scenario | msg/s | MiB/s | p50 | p99 |
| --- | ---: | ---: | ---: | ---: |
| mixed, always, await each send | 313 | 0.3 | 3.0 ms | 6.0 ms |
| mixed, always, 100 sends in flight | 279 | 0.3 | 178.7 ms | 358.6 ms |
| mixed, never, await each send | 66.9 k | 78.1 | 13 µs | 53 µs |
| mixed, never, 100 sends in flight | 161.7 k | 188.8 | 340 µs | 581 µs |
| delivery, always, 100 msg/s | 97 | 0.1 | 5.4 ms | 14.5 ms |
| delivery, never, 1,000 msg/s | 980 | 1.1 | 638 µs | 8.2 ms |
| delivery, never, 10,000 msg/s | 9.6 k | 11.3 | 1.2 ms | 14.4 ms |
| read a 128 MiB backlog with a group | 231.8 k | 270.5 | | |
| the same, with `JSON.parse` of each value | 190.1 k | 221.8 | | |

- **With `always`, sends in flight do not help yet**: each one is still flushed on its own, so 100 in flight only add waiting. Await each send, or use `fsync: 100`.
- **The binding costs about 5 times the throughput of Rust** for single sends with `never` (67 k against 313 k per second): crossing to the producer's thread and back outweighs a 2 µs append. Keeping sends in flight halves the gap.
- **Delivery p99 varies from 14 to 18 ms** from run to run at 10,000 msg/s: producer and consumer share one event loop here, so any pause of the loop shows.
