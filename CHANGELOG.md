# Changelog

All notable changes to Mojay are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until 1.0, a minor version (0.x)
may change the API or the disk format; a changed disk format is refused with a
clear error, never misread.

Note each visible change under "Unreleased" as it lands: the Release workflow
turns that section into the section of the new version.

## [Unreleased]

## [0.1.2] - 2026-11-24

### Library (`mojay` crate)

- **Changed disk format (data folder format 2)**: a group's committed offsets
  are now one file per topic, `groups/<g>/offsets/<topic>`, instead of one file
  per partition. A commit costs two `fsync` per topic whatever the number of
  partitions: with 8 partitions on macOS, delivery p99 drops from about 42 ms
  to under 2 ms (`never`, 10,000 msg/s, auto-commit every second). A data
  folder created by 0.1.x is refused with `Error::Corrupt`; recreate it.

### Benchmarks

- A realistic suite (`--suite realistic`): JSON events with keys and headers,
  8 partitions, producer and consumer processes, backlog catch-up, soak with
  retention, reopen after a crash. CSV output with `--csv`. A matching
  Node.js benchmark: `npm run bench` in `bindings/node`.

## [0.1.1] - 2026-11-23

### Node.js (`@mojay-dev/node` npm package)

- Published by the release workflow through npm trusted publishing: each
  version now carries a provenance attestation linking it to the GitHub
  Actions run and the commit it was built from.

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

### Node.js (`@mojay-dev/node` npm package)

Published on npm shortly after the crates, from the same code. npm refused the
plain name `mojay`, hence the scope.

- The library from Node.js, with Promises and TypeScript definitions; each
  producer and consumer runs on its own thread.
- Prebuilt binaries for macOS (arm64, x64) and Linux with glibc 2.35 or later
  (x64, arm64).

[0.1.2]: https://github.com/SachaCR/mojay/releases/tag/v0.1.2
[0.1.1]: https://github.com/SachaCR/mojay/releases/tag/v0.1.1
[0.1.0]: https://github.com/SachaCR/mojay/releases/tag/v0.1.0
