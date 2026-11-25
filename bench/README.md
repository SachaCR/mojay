# Mojay benchmarks

Two suites:

- **micro** (default): the cost of each primitive, for each fsync policy, with
  synthetic 100 B and 1 KiB values on one partition.
- **realistic**: JSON events with keys and headers on 8 partitions, several
  processes, large backlogs, a soak with retention.

Each scenario runs in its own temporary data folder, deleted at the end;
memory stays bounded (latencies go into a fixed-size histogram, messages come
from a pool of at most 16 MiB that is reused in a loop).

```bash
cargo run --release -p mojay-bench                                      # micro
cargo run --release -p mojay-bench -- --suite realistic --csv run.csv  # about 2 min
cargo run --release -p mojay-bench -- --suite all --only delivery --seconds 5
```

Options: `--seconds` (each time-boxed scenario, 2 by default),
`--soak-seconds` (30), `--backlog-mib` (data written before the catch-up and
reopen scenarios, 1024), `--cold` (pause after writing the backlog so you can
drop the page cache: `sudo purge` on macOS), `--csv <file>` (to compare runs
between commits). The ingestion scenarios write up to about 2.5 GiB to the
temporary folder each.

Numbers taken in a Docker container on a Mac do not tell you much about
`fsync`: the container's disk lives in a VM. Measure Linux on a real Linux
machine.

## Micro suite

Apple M5 (10 cores), 16 GB, macOS 26.6.2, internal SSD (APFS). Release build,
2 seconds per time-boxed scenario, with group commit. Whole run: 40 s, **9.8 MB
of memory at most** (maximum resident set size). `messages` counts what a
scenario sent or read in its run; `msg/s` is the rate.

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

Latency columns: *per send* / *per batch* is the time a `send` call takes;
*delivery* is the time from just before `send` to the message reaching the
consumer's `poll`.

Read numbers vary a lot from run to run (5 to 15 million messages per second
raw, 5 to 9 million through a group, on the same build): they depend on the
state of the page cache right after the prefill.

#### Before group commit

The same machine, with the flush done under the partition's write lock (and
the index flushed on every send):

| scenario | msg/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| produce always, 100 B, batch 1 | 205 | 5.2 ms | 9.4 ms |
| produce always, 100 B, batch 100 | 14.6 k | 7.3 ms | 10.5 ms |
| 1 writer on 1 partition, always, 100 B | 148 | 6.3 ms | 14.7 ms |
| 2 writers on 1 partition, always, 100 B | 148 | 6.3 ms | 335.5 ms |
| 4 writers on 1 partition, always, 100 B | 168 | 6.3 ms | 805.3 ms |

### What the numbers say

- **`always` is bounded by the disk flush: about 300 messages per second**
  for a producer sending one message at a time. On macOS each `fsync` is an
  `F_FULLFSYNC` (the drive's cache is really flushed), about 3 to 5 ms.
  Batching amortizes it: 100 messages per batch give 27,000 msg/s, with the
  same per-call latency.
- **Group commit lets concurrent writers share flushes**: with 4 writers on
  one partition, `always` reaches about 580 msg/s (3.5 times as before), and
  the p99 latency drops from 805 ms to 11.5 ms. The write lock is now held for
  microseconds, so `flock`'s unfairness no longer shows. Single writers got
  faster too: a send now flushes only the `.log`, not the index, which is a
  cache rebuilt from the log.
- **`interval` and `never` are about 1,000 times faster**: 250,000 to
  450,000 single sends per second, 5 to 6 million per second in batches of
  100, a few microseconds per send. With `never`, contention between writers
  costs about a third of the throughput and adds latency in the hundreds of
  microseconds.
- **Reading is fast**: 5 to 15 million messages per second raw, 5 to 9
  million through a consumer group.
- **Delivery latency** from producer to consumer is under 1 ms at the median
  and 3 ms at p99 with `never`; with `always` it is dominated by the flush.

### Possible improvements

1. **Automatic batching in the Node binding**: sends waiting in a producer's
   queue would be written as one batch, with a single flush, which multiplies
   `always` throughput for code that does not await each send.
2. **Documentation**: say plainly that `always` means about 300 sends per
   second for a single writer on macOS, and point to batches, several writers or
   `interval` for throughput.

## Realistic suite

Same machine. Release build, defaults (2 s per scenario, 30 s soak, 1 GiB
backlog). Whole run: 108 s, **44 MB of memory at most**. The delivery
and soak scenarios were measured again after the move to one offsets file
per topic (data folder format 2); the others do not commit.

### Messages

Generated from a seed (`src/payload.rs`), so every run writes the same bytes.
Each event has a key (the user, customer or document: keys repeat, and the
partitioner spreads them over the 8 partitions) and two headers
(`event-type`, `trace-id`). Sizes below count key, value and headers.

| shape | average | what it is |
| --- | ---: | --- |
| `click` | 475 B | page view: user, session, URL, user agent, viewport |
| `order` | 1.3 KiB | order with 3 to 10 line items, two addresses, payment |
| `document` | 35 KiB | document with 24 to 40 sections of text |
| `mixed` | 1.25 KiB | 80 % clicks, 18 % orders, 2 % documents |

### Ingestion

One producer in the process, keyed `send` (one message at a time) or
`send_batch_to` (100 messages, partitions in turn). *Fan-in*: N producer
**processes** on the same topic, each sending keyed `mixed` messages.

| scenario | msg/s | MiB/s | p50 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| click, always, keyed send | 273 | 0.1 | 4.1 ms | 4.1 ms | 4.1 ms | 4.1 ms |
| document, always, keyed send | 274 | 9.1 | 4.2 ms | 4.2 ms | 4.3 ms | 4.3 ms |
| mixed, always, keyed send | 270 | 0.4 | 4.2 ms | 4.2 ms | 7.0 ms | 7.0 ms |
| mixed, always, batch 100 | 27.9 k | 33.7 | 4.2 ms | 4.2 ms | 5.6 ms | 5.6 ms |
| click, interval 100ms, keyed send | 231.5 k | 105.0 | 2 µs | 6 µs | 23 µs | 76.5 ms |
| order, interval 100ms, keyed send | 192.9 k | 233.2 | 3 µs | 8 µs | 31 µs | 63.9 ms |
| document, interval 100ms, keyed send | 35.8 k | 1190.4 | 11 µs | 41 µs | 590 µs | 69.2 ms |
| mixed, interval 100ms, keyed send | 179.1 k | 214.4 | 3 µs | 13 µs | 33 µs | 65.4 ms |
| mixed, interval 100ms, batch 100 | 892.5 k | 1079.0 | 31 µs | 246 µs | 18.9 ms | 215.2 ms |
| mixed, never, keyed send | 312.9 k | 374.4 | 2 µs | 9 µs | 15 µs | 28.7 ms |
| mixed, never, batch 100 | 819.8 k | 991.2 | 29 µs | 229 µs | 25.2 ms | 193.5 ms |
| fan-in, always, 1 process | 310 | 0.4 | 3.1 ms | 4.2 ms | 7.2 ms | 7.2 ms |
| fan-in, always, 4 processes | 370 | 0.4 | 11.5 ms | 21.0 ms | 24.0 ms | 24.0 ms |
| fan-in, never, 1 process | 238.2 k | 307.2 | 3 µs | 20 µs | 82 µs | 84.4 ms |
| fan-in, never, 4 processes | 192.7 k | 238.5 | 11 µs | 123 µs | 459 µs | 23.7 ms |

Latency is the time a `send` (or a batch) takes. On disk, segments take
about 4 % more than the payload (record headers and CRCs).

### Delivery: producer → consumer processes

A producer at a fixed rate in the bench process; consumers in child
processes, with auto-commit every second. Latency runs from the time each
message was **due** to be sent, so a producer that falls behind counts too
(no coordinated omission).

| scenario | msg/s | p50 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 consumer, always | 100 | 6.8 ms | 15.7 ms | 35.2 ms | 35.2 ms |
| 1 consumer, interval 100ms | 1,000 | 1.4 ms | 75.5 ms | 75.5 ms | 76.3 ms |
| 1 consumer, never | 1,000 | 852 µs | 6.8 ms | 21.0 ms | 21.6 ms |
| 1 consumer, never | 10,000 | 786 µs | 1.6 ms | 7.9 ms | 9.0 ms |
| 1 consumer, never | 50,000 | 147 µs | 5.8 ms | 15.7 ms | 15.8 ms |
| work queue, 4 consumers, always | 100 | 12.6 ms | 41.3 ms | 41.3 ms | 41.3 ms |
| work queue, 4 consumers, never | 10,000 | 721 µs | 4.7 ms | 13.6 ms | 18.5 ms |
| 2 consumers, a 3rd joins, the 1st is killed, never | 10,000 | 852 µs | 369.1 ms | 416.9 ms | 416.9 ms |
| fan-out, 3 groups of 1, never | 10,000 | 786 µs | 7.9 ms | 21.0 ms | 23.6 ms |

#### Before one offsets file per topic

With format 1, a commit wrote one file per partition, each with a file and a
folder `fsync`: 16 full flushes for 8 partitions, inside `poll`, once per
second.

| scenario | msg/s | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| 1 consumer, always | 100 | 7.3 ms | 75.5 ms | 94.2 ms |
| 1 consumer, interval 100ms | 1,000 | 1.4 ms | 100.7 ms | 109.1 ms |
| 1 consumer, never | 1,000 | 852 µs | 41.9 ms | 58.7 ms |
| 1 consumer, never | 10,000 | 786 µs | 41.9 ms | 58.7 ms |
| 1 consumer, never | 50,000 | 115 µs | 46.1 ms | 62.9 ms |
| work queue, 4 consumers, never | 10,000 | 721 µs | 15.7 ms | 31.5 ms |
| fan-out, 3 groups of 1, never | 10,000 | 852 µs | 151.0 ms | 167.8 ms |

### Backlog, soak, reopen

| scenario | messages | result |
| --- | ---: | --- |
| catch-up: a group reads a 1 GiB backlog (warm cache) | 790,300 | 850 k msg/s, 1.1 GiB/s |
| soak 30 s, interval 100ms, 20,000 msg/s, 8 MiB segments, retention 32 MiB/partition | 600,000 | delivery p50 1.6 to 5.8 ms, p99 76 to 92 ms in every 5 s window (before format 2: p50 4.7 to 12.6 ms, p99 101 to 109 ms); disk flat at 223 to 232 MiB from the 2nd window |
| reopen 1 GiB of active segments, index intact | 790,300 | 4.9 ms |
| reopen 1 GiB of active segments, index deleted | 790,300 | 573 ms (full scan, 1.8 GiB/s) |

### What the numbers say

- **Real messages do not change the write picture.** `always` stays at about
  270 sends per second whatever the size (from 475 B to 35 KiB): the flush
  costs the same. With `never`, a keyed `mixed` send takes 2 µs (313 k msg/s),
  and batches go beyond 1 GiB/s.
- **Commits used to set the delivery p99.** With one offsets file per
  partition, p99 was 40 to 100 ms in every scenario with consumers, and
  151 ms with 3 groups. One file per topic (two `fsync` per commit) brings
  `never` at 10,000 msg/s from 41.9 ms to 1.6 ms, and 3 groups from 151 ms
  to 7.9 ms. A commit still costs two flushes (about 8 ms on macOS) once per
  second, which is most of what remains above the median.
- **`interval` keeps a p99 of about 75 ms**, in delivery as in the soak, and
  single sends with `interval` have a max of 60 to 75 ms: the 100 ms
  background `fsync` seems to hold up sends while it runs. Not investigated
  yet.
- **A rebalance stalls delivery for about 0.4 s** when a member crashes: its
  partitions wait for the next rebalance of the others (every 200 ms), and
  the messages since its last commit are delivered again.
- **Soak is stable**: rate, latency and disk size stay flat over 30 s while
  retention deletes segments continuously.
- **4 writer processes with `never`** lose about 20 % of throughput and p99
  goes from 20 to 123 µs: the cost of taking turns on each partition.
- **Losing the index is cheap**: rebuilding it scans 1 GiB in about 0.6 s.

### Possible improvements

1. **`interval` without stalling sends**: check whether the background
   `fsync` holds the partition logs' lock while it flushes.
2. **Commit off the `poll` path**: an auto-commit that does not block the
   records that are ready (the remaining two flushes).

## Node.js binding

`npm run bench` from `bindings/node`, after `npm run build` (release). Same
messages (`bindings/node/bench/payload.mjs` is a port of `src/payload.rs` and
writes the same bytes), same 8 partitions; producer and consumer in the same
process.

| scenario | msg/s | MiB/s | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: |
| mixed, always, await each send | 313 | 0.3 | 3.0 ms | 6.0 ms | 13.2 ms |
| mixed, always, 100 sends in flight | 279 | 0.3 | 178.7 ms | 358.6 ms | 371.3 ms |
| mixed, never, await each send | 66.9 k | 78.1 | 13 µs | 53 µs | 103 µs |
| mixed, never, 100 sends in flight | 161.7 k | 188.8 | 340 µs | 581 µs | 871 µs |
| delivery, always, 100 msg/s | 97 | 0.1 | 5.4 ms | 14.5 ms | 30.2 ms |
| delivery, never, 1,000 msg/s | 980 | 1.1 | 638 µs | 8.2 ms | 25.2 ms |
| delivery, never, 10,000 msg/s | 9.6 k | 11.3 | 1.2 ms | 14.4 ms | 32.4 ms |
| catch-up, read 128 MiB | 231.8 k | 270.5 | | | |
| catch-up, read 128 MiB + `JSON.parse` | 190.1 k | 221.8 | | | |

- **Sends in flight do not help `always` today**: each one is still flushed
  on its own, so 100 in flight only add queueing (p50 179 ms). This is the
  measured case for automatic batching in the binding (micro suite,
  improvement 1).
- **The binding costs about 5 times the throughput of Rust** for single sends
  with `never` (67 k vs 313 k msg/s): crossing to the producer thread and back
  dominates a 2 µs append. Pipelining halves the gap.
- Reading is 3.5 times slower than in Rust, and `JSON.parse` adds 20 %.
- Delivery p99 at 10,000 msg/s varies between 14 and 18 ms from run to run
  (one run gave 103 ms): producer and consumer share one event loop here, so
  any pause of the loop shows. Before one offsets file per topic: 55 ms.
