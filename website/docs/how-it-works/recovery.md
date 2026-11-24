---
sidebar_position: 3
title: Crash recovery
description: What Mojay repairs after a crash, what it refuses to repair, and how to look at a segment yourself.
---

A process can die at any instruction: killed, out of memory, or taken down with the machine. Mojay's recovery rests on one rule: **only an interrupted write is repaired, and an interrupted write can only be at the end of the active segment.** Anything else that looks wrong is reported, never silently fixed.

## What a crash can leave behind

Records are only ever appended to the active segment, and a sealed segment was flushed to disk before the next one was created. So after a crash:

- **Sealed segments** are complete. If one of them is damaged, it was not damaged by Mojay's own writes: a disk problem, a file edited by hand, a bad copy.
- **The active segment** may end with a partial record, or with garbage if the file was extended but its content never reached the disk.
- **The index** may be missing entries, or have entries pointing past the end of the log: the `.log` and `.idx` are separate files, and the disk does not promise to persist them in order.
- **`partitions.state`** may say the partition is being modified (the `dirty` flag), or describe a log that no longer matches the files.

## Who repairs, and when

Only writers repair. A partition is recovered from disk, under its write lock:

- when a **producer opens** the topic: each partition is checked, whether or not anything looks wrong;
- when a writer, under the lock, finds that the previous one **left the partition dirty** (it died in the middle of a change), or that the files do not match the published state (another active segment, or an active segment whose length differs from the published one).

Recovering a partition means reopening its log:

1. **List the segments** in the partition folder. The last one is the active segment.
2. **Check each header.** An active segment shorter than 16 bytes was created just before a crash: it is recreated empty. A sealed segment without a valid header is corrupt.
3. **Load the index**, keeping its longest consistent prefix (positions inside the log, offsets and positions strictly increasing). Then step back over the last entries until one points to a valid record carrying the expected offset.
4. **Scan the log** from that entry to the end, checking every record: length, CRC, structure, and an offset exactly one more than the previous record's. Missing index entries are added along the way. Without a usable index, the scan covers the whole segment; with one, it usually reads only the last few KiB.
5. **Decide what to do with the end.** If the scan stops before the end of the file:
   - in the **active segment**, the rest is the remains of an interrupted write: the file is truncated to the last valid record, and the truncation is flushed;
   - in a **sealed segment**, the anomaly is reported as `Error::Corrupt`, with the file path and the position. Nothing is truncated.
6. **Rewrite the index** if it was missing, damaged or incomplete.
7. **Check continuity**: each segment must end exactly at the offset where the next one begins. A missing segment in the middle is reported as `Error::Corrupt`.
8. **Clean up** index files whose `.log` no longer exists: retention deletes the `.log` first, then the index, so a crash in between leaves an orphan index.

After a producer has repaired a partition, it publishes the real state of the log in `partitions.state`: end of the active segment, next offset, and a cleared `dirty` flag. With `fsync = always`, it flushes the active segment first. It then sets the read limit to the end of the log. This is the only time the read limit may move back: when the repair removed messages that the shared state still counted, for example after a power loss with `fsync = never`.

:::tip[What is kept]
A record that was completely written before the crash passes every check, so it is kept, even if the `send` that wrote it never returned. Its producer may send it again after restarting: delivery is at least once, not exactly once. If a writer died between writing and flushing, the next flush by any writer, or the next producer to open the topic, makes its messages durable before they become readable.
:::

## Readers never modify anything

A reader (`TopicReader`, or a consumer) takes no lock and never writes to a segment or an index. It cannot repair, so it relies on the read limit instead:

- **Before the read limit**, every record is complete and, with `fsync = always`, on disk. An unreadable record there is an error (`Error::Corrupt`).
- **At or after the read limit**, a writer may be writing right now. An incomplete record there is normal, and the reader simply stops.

A missing, truncated or wrong index only slows a reader down. It keeps the consistent entries, checks that the entry it starts from points at the record it names, and otherwise steps back to the previous entry, down to the start of the segment. It never repairs the index file: only a writer does.

## Other files

| File | What happens when it is damaged |
| --- | --- |
| `mojay.toml`, `meta.toml` | Strict parsing: a malformed line, an unknown key or an unsupported version is `Error::Corrupt`. They are only ever replaced atomically, so a crash leaves the old or the new version. |
| `partitions.state` | Its size must match the number of partitions, otherwise `Error::Corrupt`. Its content is rebuilt from the logs whenever a producer opens the topic. |
| `offsets/<t>` | Ends with its own CRC32C. A file of the wrong size, with the wrong number of partitions or a bad CRC is `Error::Corrupt`, rather than resuming from a wrong position. Replaced atomically. |
| A topic folder being created | Prepared under a hidden name and renamed at the end: a crash leaves a hidden folder that the topic listing ignores. |

If a consumer finds a committed offset beyond the read limit (messages committed, then lost in a power loss with `fsync = never` or `interval`), it resumes at the read limit, so it skips none of the new messages written there.

## How this is tested

Recovery is tested exhaustively rather than by example: the log is cut at every byte of its end and reopened, every byte of a record is modified in turn to check that the damage is detected, indexes are deleted or filled with wrong entries, and sealed segments are damaged to check that they are reported, not repaired. Multi-process tests kill a writer in the middle of its writes and check that the other writers carry on and that the next one repairs the partition.

## Looking at a segment: `mojay dump`

`mojay dump` shows what is actually stored in a segment: position, offset, size, timestamp and content of each record, and the index with `--index`. Like a reader, it never repairs anything. Unlike a reader, it does not stop with an error on damaged data: it shows everything it could read, then where and why the rest is unreadable.

```bash
mojay --data ./data dump orders --index            # every segment of a topic
mojay --data ./data dump orders --partition 2      # one partition
mojay dump ./data/topics/orders/p0/00000000000000000000.log
```

Here is a segment whose last write was interrupted after 6 bytes:

```text
== data/topics/orders/p0/00000000000000000000.log (168 B, base 0)
valid header, format v1
  position     offset    size  timestamp                 contents
        16          0      49  2026-11-20T14:54:01.462Z  key="client-42" value="hello"
        65          1      49  2026-11-20T14:54:01.465Z  key="client-42" value="world"
       114          2      48  2026-11-20T14:54:01.487Z  key="client-7" value="third"
3 readable record(s)
!! unreadable tail at position 162: 6 B (incomplete record)
index: 0 entry(ies)
```

The next producer to open this topic truncates the file to 162 bytes. The same anomaly in a sealed segment would make opening fail with `Error::Corrupt`, and `mojay dump` is the tool to understand why. The same inspection is available from Rust as `mojay::inspect::dump_segment`.
