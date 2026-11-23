---
sidebar_position: 7
title: Multiple processes
description: What several processes can safely do with the same data folder, how crashes are handled, and where Mojay must not be used.
---

A Mojay data folder is meant to be shared by several processes on the same
machine, the way an SQLite database file is. Processes coordinate only through
the file system: locks on files, and a small shared state file mapped in memory.
No process plays a special role, and none has to be started first.

## What is safe

Any combination of the following can run at the same time, in one process or in
many, written in Rust, Node.js, or using the `mojay` command:

| Role | How many | Coordination |
| --- | --- | --- |
| Producers on a topic, even on the same partition | Any number | They take turns on each partition, one batch at a time, through a write lock. Disk flushes are shared. |
| Members of a consumer group | Any number | Each partition is owned by one member at a time, through a lock per partition. |
| Readers (`TopicReader`, `mojay tail`, `mojay describe`) | Any number | None needed: they take no lock and never modify anything. |
| Different consumer groups on the same topic | Any number | Independent: each group has its own folder and its own offsets. |

Several consumers of the same group can also live in the same process: each one
is a separate member.

## How it works, in short

- **Locks are `flock` locks** on files in the data folder. A producer holds a
  partition's write lock for the few microseconds of a write; a group member
  holds a lock per partition it owns, and one on its own membership file.
- **The read limit and the writers' state** of each partition live in a small
  file, `partitions.state`, that every process maps in memory. Readers check it
  without a system call; writers use it to catch up with what other writers
  appended.
- **Reads use positional I/O** (`pread`): no process shares a file cursor with
  another.

See [Shared state and locks](../how-it-works/shared-state-and-locks.md) for the
details.

## When a process crashes

When a process dies, for whatever reason, the operating system releases all its
locks immediately. There is no timeout to wait for and nothing to clean up by
hand:

- **A producer that crashed in the middle of a write** leaves its partition
  marked as being modified. The next writer on that partition notices it,
  reopens the partition from disk and cuts off the incomplete end before
  appending. A message that was completely written is kept. Readers never saw
  the incomplete part, because the read limit had not moved past it. See
  [Recovery](../how-it-works/recovery.md).
- **A consumer that crashed** loses its membership and partition locks at once.
  The other members of its group take its partitions on their next rebalance
  (within about 200 ms) and resume from the last committed offsets.
- **A reader that crashed** held nothing: nothing happens.

A machine restart is handled the same way: the first producer to open a topic
repairs each partition before writing.

## Same machine, local file system

`flock` locks only coordinate processes that share one kernel and one local file
system. This sets the limits of Mojay:

:::warning[Not on network or synced folders]
Do not put a data folder on a network file system (NFS, SMB) or in a folder
synchronized by a cloud service. `flock` is not reliable there, and two
processes could both believe they own the same partition. SQLite has the same
restriction.
:::

- **One machine.** Processes on different machines cannot share a data folder,
  even through a network file system. Mojay has no replication.
- **macOS and Linux.** Windows is not supported in version 1.

## Things to keep in mind

- **Close what you open.** A consumer keeps its partitions for as long as it
  lives, even if it stops calling `poll`. In Node.js, call `close()` rather than
  waiting for the garbage collector.
- **A crash means at-least-once.** A crashed producer's last message may be
  written even though its `send` never returned, and a crashed consumer's
  partitions are replayed from the last commit. Design for duplicates.
- **The CLI is safe to use while your application runs.** `mojay tail`,
  `describe` and `dump` are read-only, and `mojay produce` takes turns with your
  producers like any other writer.
