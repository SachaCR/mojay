---
sidebar_position: 4
title: Shared state and locks
description: The memory-mapped partitions.state file, its memory ordering, and why Mojay uses flock with an explicit unlock.
---

Processes that share a Mojay folder coordinate through two mechanisms only: a small memory-mapped file per topic, `partitions.state`, and `flock` locks on empty files. There is no daemon and no socket.

## `partitions.state`

The file holds one 64-byte slot per partition: eight `u64` fields, six of them used and two reserved. Every process that opens the topic, writer or reader, maps it into memory with a shared `mmap`. Reading or publishing a field is then a single atomic operation on memory, with no system call.

| Field | Meaning | Written by | Read by |
| --- | --- | --- | --- |
| `read_limit` | First offset that readers may not read yet. Everything below it is fully written and, with `fsync = always`, on disk. | Writers, after the flush | Readers and consumers; writers during group commit |
| `end_position` | Number of valid bytes in the active segment, header included | Writers, under the write lock | The next writer |
| `active_base` | Base offset of the active segment | Writers, under the write lock | The next writer; group commit |
| `start_offset` | First available offset (base of the oldest segment) | Writers, under the write lock | The next writer |
| `dirty` | Non-zero while a writer is modifying the partition's files | Writers, under the write lock | The next writer |
| `next_offset` | Offset the next message will get. Everything below it is written to the files, not necessarily flushed. | Writers, under the write lock | The next writer; group commit |

Readers only ever look at `read_limit`. The other fields are the writers' shared memory: a writer that takes the lock reads what the previous holder published and catches up with its appends, usually with a few comparisons and one `fstat`.

The file is created with the topic: every slot starts at zero, except `end_position`, which is 16 (an empty segment holds only its header). Fields are stored in the machine's native byte order, since the file is only ever shared between processes of the same machine. When opening, Mojay checks that the file size matches the number of partitions; otherwise it reports `Error::Corrupt`.

### Two limits: `next_offset` and `read_limit`

Kafka calls the read limit the *high watermark*. Mojay keeps it separate from `next_offset` because of [group commit](./write-path-and-group-commit.md): messages are written, and `next_offset` published, under the write lock, but they only become readable after a flush that happens once the lock is released and may cover several writers at once. So `read_limit` trails `next_offset`, and only moves forward.

### Memory ordering

The fields are atomics, and the ordering of the operations is what makes the protocol correct:

- **Publishing** (writer, under the write lock): `end_position`, `active_base` and `start_offset` are stored, then `dirty` is cleared, then `next_offset` is stored **last**, all with *Release* ordering. Whoever sees a given `next_offset` also sees the active segment it belongs to.
- **Making readable**: `read_limit` is advanced with `fetch_max` and *Release* ordering. `fetch_max` means an older flush finishing late can never move the limit back.
- **Reading**: readers load `read_limit` with *Acquire* ordering, then read the segment with `pread`. Everything written to the log before the limit was advanced is visible to them.
- **Marking dirty**: `dirty` is set before the writer touches any file. If the writer dies before publishing, the flag stays set, and the next writer reopens the partition from disk and repairs it.

The read limit moves back in one case only: when a producer opens a topic, repairs a partition and finds fewer messages than the shared state claimed, for example after a power loss with `fsync = never`. See [Crash recovery](./recovery.md).

### Waiting for new messages

A consumer waiting for messages polls `read_limit` in shared memory, with an adaptive delay that doubles from 1 ms to 50 ms while nothing arrives. The cost is negligible: it is a memory read, not a system call. Event-based notification would need a separate channel, because `inotify` and `kqueue` do not report writes made through `mmap`.

`partitions.state` is the only place in Mojay that uses `mmap`, and the only module allowed to contain `unsafe` code. Segments are read with `pread`.

## Locks: `flock`, never `fcntl`

Unix offers two kinds of advisory file locks, and Mojay uses only `flock`:

- **POSIX `fcntl` locks belong to the process**, and are released as soon as the process closes *any* descriptor on the file, even one opened by an unrelated part of the program. A library cannot protect itself against that.
- **`flock` locks belong to the open file description.** They are released when that descriptor is closed, or by the operating system when the process dies, whatever the reason.

That last property is the basis of crash detection in Mojay: a dead process cannot keep a lock. No heartbeat, no lease, no timeout.

### Explicit unlock, because of `fork`

A `flock` lock is shared with every child created by `fork`, until that child calls `exec`. If another thread of the program spawns a subprocess at the wrong moment, closing the file is not enough: the child's copy of the descriptor keeps the lock alive. Mojay therefore always releases a lock with an explicit `flock(LOCK_UN)`, which releases it for every copy at once, before closing the file. This case was found by a test that failed about one time in eight.

### The lock files

| Lock | Mode | Held | Purpose |
| --- | --- | --- | --- |
| `p<N>/write.lock` | Blocking, exclusive | Microseconds: one batch | Writers take turns appending to the partition |
| `p<N>/flush.lock` | Blocking, exclusive | Milliseconds: one `fsync` | One writer flushes for all those waiting (`fsync = always` only) |
| `groups/<g>/members/<id>.lock` | Non-blocking, exclusive | The member's lifetime | Liveness of a group member |
| `groups/<g>/partitions/<t>-<p>.lock` | Non-blocking, exclusive | While the member owns the partition | Ownership of a partition within a group |

Each producer opens its own descriptor on each lock file, so two producers in the same process exclude each other exactly like two producers in different processes. A blocking wait interrupted by a signal is retried.

## Readers are lock-free

A `TopicReader` takes no lock at all and never writes a byte to disk. You can open as many as you like, in any process, while producers write. This is safe because:

- the log is append-only: bytes below the read limit never change;
- readers never go beyond the read limit, and the record checksums let them tell a write in progress from real damage;
- reads are positional (`pread`), so there is no shared file cursor that another thread or process could move.

Consumers in a group do take locks, but only on their group's files, never on the topic's: reading a partition is the same lock-free path.

:::warning[Local file systems only]
`flock` is not reliable on NFS, SMB or folders synchronized by a cloud client, and shared `mmap` needs processes on the same machine. Keep the data folder on a local disk.
:::
