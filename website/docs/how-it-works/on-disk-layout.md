---
sidebar_position: 1
title: On-disk layout
description: Every file Mojay writes in a data folder, what it is for, and how its format is versioned.
---

:::note
These pages describe Mojay's internals. You do not need them to use the library, but they explain where its guarantees come from. The reference is the design note [`docs/conception.md`](https://github.com/SachaCR/mojay/blob/main/docs/conception.md) in the repository (in French), versioned with the code.
:::

A Mojay data folder is the equivalent of an SQLite database file: all the state lives inside it, and nothing lives anywhere else. There is no server process and no hidden cache. Several processes on the same machine open the same folder and coordinate through the files themselves.

## The tree

```text
data/
  mojay.toml                        # data folder format version
  topics/<topic>/
    meta.toml                       # partitions, segment size, fsync policy, retention
    partitions.state                # shared state of each partition, memory-mapped
    p0/
      write.lock                    # flock held by a writer while it writes a batch
      flush.lock                    # flock held while flushing to disk (group commit)
      00000000000000000000.log      # a segment, named after its first offset
      00000000000000000000.idx      # its sparse index: offset -> position
      00000000000000052417.log      # the next segment starts at offset 52417
      00000000000000052417.idx
    p1/ ...
  groups/<group>/                   # a lone consumer is a group named after it
    members/<member_id>.lock        # locked for as long as the member lives
    partitions/<topic>-<p>.lock     # locked = partition owned by a member
    offsets.lock                    # locked while a member writes a commit
    offsets/<topic>                 # the group's committed offsets for that topic
```

A topic is a folder with one sub-folder per partition. Each consumer group has its own folder, including the group of a lone consumer, so two groups never write the same file.

## What each file is for

| File | Written by | Purpose |
| --- | --- | --- |
| `mojay.toml` | `Db::open`, once | Marks the folder as a Mojay data folder and records its format version. |
| `topics/<t>/meta.toml` | Topic creation, once | The topic configuration, fixed at creation. |
| `topics/<t>/partitions.state` | Writers | One 64-byte slot per partition: read limit, end of the active segment, next offset, and so on. Mapped in memory by every process. See [Shared state and locks](./shared-state-and-locks.md). |
| `p<N>/write.lock` | Writers | Taken with `flock` for the duration of one batch. Writers take turns on a partition. |
| `p<N>/flush.lock` | Writers with `fsync = always` | Taken while one writer flushes the active segment for everyone waiting. See [Write path and group commit](./write-path-and-group-commit.md). |
| `p<N>/<base>.log` | Writers | A segment: a 16-byte header, then records one after another. Append-only. |
| `p<N>/<base>.idx` | Writers | The sparse index of that segment. A cache, rebuilt from the `.log` when missing or wrong. |
| `groups/<g>/members/<id>.lock` | Each consumer | A member is alive as long as it holds the lock on its file. |
| `groups/<g>/partitions/<t>-<p>.lock` | Consumers | A member owns a partition as long as it holds this lock. |
| `groups/<g>/offsets.lock` | Consumers | Taken with `flock` while a member rewrites an offsets file, so that it keeps the other members' entries. |
| `groups/<g>/offsets/<t>` | Consumers, each for the partitions it owns | The number of partitions (`u32`), one committed offset per partition (`u64`, `u64::MAX` if none), then a CRC32C. |

The lock files are empty. Only the `flock` held on them matters, and the operating system releases it when the process dies, which is what lets Mojay detect crashes without heartbeats.

### Segment names

A segment is named after its base offset, the offset of its first record, on 20 digits with leading zeros: `00000000000000052417.log`. Sorting the names sorts the segments. The last one is the *active* segment, the only one that receives writes; the others are *sealed*. Files whose names do not match this pattern are ignored.

### Metadata files

`mojay.toml` and `meta.toml` use a small subset of TOML: one `key = value` pair per line, integers or double-quoted strings, `#` comments. Any TOML tool can read them. Mojay parses them strictly: an unknown key, a duplicate key or a malformed line is an error, never silently ignored.

```toml title="data/topics/orders/meta.toml"
# Mojay topic metadata. Do not edit by hand.
format_version = 2
partitions = 1
segment_max_bytes = 67108864
index_interval_bytes = 4096
fsync = "always"
```

A topic with `fsync = "interval"` also has `fsync_interval_ms`. The retention keys, `retention_bytes` and `retention_ms`, are optional: without them, the topic keeps everything.

## How files are created and replaced

- **Small files are replaced atomically.** `meta.toml`, `mojay.toml` and committed offsets are written to a temporary file, flushed with `fsync`, renamed over the old file, and then the folder itself is flushed so the rename survives a power loss. A reader sees the old content or the new one, never a mix.
- **Topics appear complete or not at all.** A new topic is prepared in a temporary folder whose name starts with `.` (`meta.toml`, `partitions.state`, the partition folders), made durable, then renamed to its final name. Names starting with `.` are ignored by the topic listing, so an interrupted creation leaves no half-made topic. This is also why topic and group names cannot start with `.`.
- **Names** of topics and groups are 1 to 249 characters among `a-z`, `A-Z`, `0-9`, `.`, `_` and `-`, so they are valid folder names everywhere.
- **Partition files appear on first write.** A new partition folder is empty; the first producer to open it creates segment `0`, its index and the lock files.

## Format versions

Each file format carries its own version, so an incompatible change fails clearly instead of being misread.

| What | Where the version lives | Current version |
| --- | --- | --- |
| Data folder | `format_version` in `mojay.toml` | 2 |
| Topic | `format_version` in `meta.toml` | 2 |
| Segment | `u16` in the 16-byte header of each `.log` | 1 |

Data folder format 2 replaced the committed offsets file of each partition with one file per group and topic. A data folder in format 1 is refused with `Error::Corrupt`: there is no migration, so recreate the folder.

Topic format 2 introduced several writers per partition and the `partitions.state` file. A topic created in format 1 (single writer) is refused with an error saying to recreate it: there is no migration. Any other unknown version is reported as `Error::Corrupt`, with the file path and the version found.

## Local file systems only

Coordination relies on `flock`, which is not reliable on network file systems (NFS, SMB) or on folders synchronized by a cloud client. SQLite has the same restriction. Keep the data folder on a local disk. Mojay targets macOS and Linux.
