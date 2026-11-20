---
sidebar_position: 2
title: Record format
description: How a message is encoded on disk, how segments roll over, and how the sparse index finds an offset.
---

Each partition is a sequence of append-only segments. Every record carries its own length and checksum, so a reader can always tell a complete record from one cut short by a crash.

All integers are little-endian.

## Segment header

Every `.log` file starts with a 16-byte header:

| Bytes | Field | Value |
| --- | --- | --- |
| 0–5 | magic | `MOJAY\0` |
| 6–7 | format version | `u16`, currently `1` |
| 8–15 | reserved | zeros |

A file without this header is not a Mojay segment and is refused. The records follow immediately, one after another, with no padding.

## Record

| Field | Type | Size | Meaning |
| --- | --- | --- | --- |
| `length` | `u32` | 4 | Size of everything that follows, CRC included |
| `crc` | `u32` | 4 | CRC32C of everything that follows |
| `offset` | `u64` | 8 | Absolute offset in the partition |
| `timestamp` | `i64` | 8 | Milliseconds since the Unix epoch |
| `attributes` | `u8` | 1 | Reserved for compression and future flags, always `0` today |
| `key_len` | `i32` | 4 | Key length, or `-1` for no key |
| `key` | bytes | `key_len` | |
| `value_len` | `i32` | 4 | Value length, or `-1` for a null value (a tombstone) |
| `value` | bytes | `value_len` | |
| `headers_count` | `u16` | 2 | Number of headers |
| for each header | | | `u16` key length, the key (UTF-8), `u32` value length, the value |

A record with no key, no value and no headers takes 35 bytes, which is also the fixed overhead added to every message. A message with a 9-byte key and a 5-byte value takes 49 bytes on disk.

Some details worth knowing:

- **The CRC covers the whole body**: offset, timestamp, attributes, key, value and headers. Only the `length` field sits outside it, and a damaged length is caught too: the bytes it delimits no longer match the stored CRC.
- **The offset is stored in the record.** Recovery uses it to check that records follow each other without gaps (offset `n` is followed by `n + 1`), which catches a record written in the wrong place.
- **The timestamp** comes from the producer if it sets one, otherwise from the local clock when the batch is appended. Mojay never relies on it: retention uses the file's modification time instead, because producer clocks are not trustworthy.
- **Limits**: a record is at most 16 MiB (`MAX_RECORD_SIZE`), length prefix included; a message has at most 65,535 headers, and a header key at most 65,535 bytes. A message over these limits is rejected before anything is written: in a batch, one invalid message means none of them is written.

### Reading a record

Bytes read from disk are never trusted. A reader decodes a record in this order, and every check failure becomes a value, never a crash:

1. Read the 4-byte `length`. Zero bytes left means a clean end; fewer than 4 means the record is **incomplete**.
2. Check that the length is plausible: at least the 31 bytes of a minimal record after the length field, and at most 16 MiB. Otherwise the record is **corrupt**.
3. Read `length` bytes. If the file ends first, the record is **incomplete**.
4. Compute the CRC32C of the body and compare it with the stored one. A mismatch is **corrupt**.
5. Decode the fields. Lengths that overrun the body, leftover bytes, or a header key that is not UTF-8 are **corrupt**.

Whether an incomplete or corrupt record is an error depends on where it is: see [Crash recovery](./recovery.md).

## Segments and rotation

A partition writes only to its active segment, the last one. Before appending a record, the writer checks whether it still fits:

- If the active segment would exceed `segment_max_bytes` (64 MiB by default) with this record, the segment is **sealed** and a new one starts at the next offset. A record larger than the segment size still goes into a segment of its own: an empty segment always accepts its first record.
- Index positions are `u32`, so a segment can never exceed about 4 GiB (`u32::MAX` minus the maximum record size), and relative offsets are `u32` too, so a segment never holds more than `u32::MAX` records.

Sealing a segment is a durability point. The writer flushes the sealed segment's `.log` and `.idx` to disk, creates the new segment with its header, flushes it, then flushes the partition folder so the new file name is durable. A sealed segment is never modified again and never repaired: it must be complete on disk before the next one exists.

A batch can span several segments; its offsets stay contiguous across the boundary.

## The sparse index

Each segment has an `.idx` file: a flat array of 8-byte entries.

| Field | Type | Meaning |
| --- | --- | --- |
| relative offset | `u32` | Offset of a record, minus the segment's base offset |
| position | `u32` | Byte position of that record in the `.log` |

The index is **sparse**: the writer adds an entry each time 4 KiB of records have been written since the previous entry (`index_interval_bytes` in `meta.toml`, 4096 by default). The first 4 KiB of a segment need no entry: a lookup that finds nothing starts right after the header.

The `.log` is written before the index, so an entry never points to a record that was not written yet. The index is not flushed on every send, only when its segment is sealed: it is a cache.

### The index is a cache

Everything in the index can be rebuilt from the `.log`, so a missing or damaged index never loses data. When an index is loaded, only its longest consistent prefix is kept: positions must lie between the end of the header and the end of the log, and both offsets and positions must strictly increase. On recovery, the writer also checks that the last kept entries point to a real record carrying the expected offset, and rebuilds the rest by scanning the log. An entry can still be wrong yet look consistent (a damaged or stale index file): every read checks the entry it starts from, as described below. See [Crash recovery](./recovery.md).

## Finding an offset

To read from offset `o`:

1. **Pick the segment**: the last segment whose base offset is less than or equal to `o`. Segment names are base offsets, so this is a binary search on the sorted list.
2. **Find a starting position**: a binary search in the segment's index gives the last entry whose offset is less than or equal to `o`. Without such an entry, start right after the 16-byte header. The record found at the entry's position must be the one the entry names; if it is not, the entry is wrong, and the read starts again from the previous entry, down to the start of the segment. The reader then forgets the wrong entries until the index file changes. A wrong index only slows reading down: it never makes a read fail or skip a record.
3. **Scan forward**: read records with positional reads (`pread`) through a 64 KiB buffer, skip those below `o`, and collect records until the byte budget or the read limit is reached. Since entries are at most about 4 KiB apart, the scan is short.
4. **Continue into the next segment** if the read limit lies beyond the current one.

A read always returns at least one record when one is readable, even if that record is larger than the requested budget, so a large message can never block a reader. It never returns anything at or beyond the partition's [read limit](./shared-state-and-locks.md).

Readers never take a lock: the log is append-only, so reading while another process appends is safe. A reader re-reads the size of the active segment on each read, reloads the index when the index file changes size, and discovers new segments on its own.
