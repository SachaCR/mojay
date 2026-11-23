# Changelog

All notable changes to Mojay are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until 1.0, a minor version (0.x)
may change the API or the disk format; a changed disk format is refused with a
clear error, never misread.

## [0.1.0] - 2026-11-23

The first version: Kafka's model in a local folder, with no server.

### Library (`mojay` crate)

- Topics with a fixed number of partitions; a message goes to an explicit
  partition, to the partition of its key (murmur2, like Kafka), or round-robin.
- Append-only segments with a sparse index, rotated by size, recovered after a
  crash: only an interrupted write at the end of the active segment is cut;
  damage anywhere else is reported as `Error::Corrupt`.
- `fsync` policies: `Always` (default), `Interval` and `Never`. With `Always`,
  concurrent writers share flushes (group commit).
- Any number of producers, in any process on the machine, on the same topic
  and partition, coordinated with `flock`.
- Consumer groups without a coordinator: members share the partitions,
  rebalance when one joins, leaves or crashes, and commit offsets. A lone
  consumer is a group of one.
- Retention by size and by age, deleting whole sealed segments.
- `TopicReader` for raw reads without a group.
- macOS and Linux; Rust 1.88 or later.

### Command line (`mojay-cli` crate)

- `mojay create-topic`, `topics`, `describe`, `produce`, `tail` and `dump`.

### Node.js (`mojay` npm package)

Not yet on npm: it will be published separately, from this same version.

- The library from Node.js, with Promises and TypeScript definitions; each
  producer and consumer runs on its own thread.
- Prebuilt binaries for macOS (arm64, x64) and Linux with glibc 2.35 or later
  (x64, arm64).

[0.1.0]: https://github.com/SachaCR/mojay/releases/tag/v0.1.0
