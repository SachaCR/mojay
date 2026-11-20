# Mojay benchmarks

Throughput and latency of producing, reading and consuming, for each fsync
policy. Each scenario runs in its own temporary data folder, deleted at the
end; memory stays bounded (latencies go into a fixed-size histogram, reads
are done in 1 MiB batches).

```bash
cargo run --release -p mojay-bench
```

```bash
cargo run --release -p mojay-bench -- --only "produce never" --seconds 5
```

## Results

Apple M5 (10 cores), 16 GB, macOS 26.6.2, internal SSD (APFS). Release build,
2 seconds per time-boxed scenario, with group commit. Whole run: 40 s, **9.8 MB
of memory at most** (maximum resident set size).

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

### Before group commit

The same machine, with the flush done under the partition's write lock (and
the index flushed on every send):

| scenario | msg/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| produce always, 100 B, batch 1 | 205 | 5.2 ms | 9.4 ms |
| produce always, 100 B, batch 100 | 14.6 k | 7.3 ms | 10.5 ms |
| 1 writer on 1 partition, always, 100 B | 148 | 6.3 ms | 14.7 ms |
| 2 writers on 1 partition, always, 100 B | 148 | 6.3 ms | 335.5 ms |
| 4 writers on 1 partition, always, 100 B | 168 | 6.3 ms | 805.3 ms |

## What the numbers say

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

## Possible improvements

1. **Automatic batching in the Node binding**: sends waiting in a producer's
   queue would be written as one batch, with a single flush, which multiplies
   `always` throughput for code that does not await each send.
2. **Documentation**: say plainly that `always` means about 300 sends per
   second per writer on macOS, and point to batches, several writers or
   `interval` for throughput.
