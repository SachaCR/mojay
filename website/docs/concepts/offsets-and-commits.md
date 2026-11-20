---
sidebar_position: 4
title: Offsets and commits
description: Offsets, the read limit, committed offsets per group, manual and automatic commit, and at-least-once delivery.
---

## Offsets

Each message in a partition has an **offset**: a `u64` that starts at 0 and
grows by one for each message. Offsets are never reused. When
[retention](./retention.md) deletes old segments, the partition simply starts at
a higher offset.

Each partition has three positions worth knowing:

| Position | Meaning |
| --- | --- |
| Start offset | The first offset still available. 0 until retention deletes something. |
| Read limit | The first offset not yet readable. Consumers read up to, but not including, it. |
| Committed offset | Per group: the next offset the group will read. Everything below it is processed. |

`mojay describe <topic>` shows all three, and the lag of each group (the read
limit minus the committed offset).

## The read limit

The **read limit** of a partition is the first offset that is not readable yet.
Kafka users know it as the *high watermark*. Readers never go beyond it, so
they never see a message that is half-written:

- with the `Always` `fsync` policy, the read limit only moves once the messages
  below it are on disk: a consumer never processes a message that a power loss
  could take back;
- with `Interval` or `Never`, it moves as soon as the messages are written to
  the operating system.

The read limit lives in a small file shared in memory by every process, so a
consumer sees new messages from other processes within milliseconds, without a
system call. See [Shared state and locks](../how-it-works/shared-state-and-locks.md).

## Committed offsets

A group stores one **committed offset** per partition: the offset of the next
message to process. When a member gets a partition, at start-up or after a
rebalance, it resumes from that offset. All members of a group share these
offsets, and each group has its own: two groups reading the same topic do not
affect each other.

A committed offset is written atomically and durably (to a temporary file,
flushed, then renamed), only by the member that owns the partition.

From Rust, `db.committed_offsets(topic)` lists the offsets committed by every
group for a topic, as `CommittedOffset { group, partition, offset }`. It
modifies nothing.

## Manual or automatic commit

### Manual commit (the default)

With `auto_commit: None`, you call `commit()` yourself. It commits, for each
partition the consumer owns, the position after the last message returned by
`poll`:

```rust
loop {
    let records = consumer.poll(Duration::from_millis(500))?;
    for record in &records {
        process(record)?;
    }
    consumer.commit()?; // everything returned so far is processed
}
```

Commit after processing, not before: if the process crashes in between, the
messages are read again rather than lost. Committing after each batch of `poll`
results, as above, is the usual pattern. A commit writes one small file per
partition that moved, so committing after every single message costs a disk
flush per message.

### Automatic commit

With `auto_commit: Some(interval)`, the consumer commits for you:

- at the start of a `poll`, if at least `interval` has passed since the last
  commit;
- when it hands a partition over to another member;
- when it is dropped (Rust) or closed (Node.js).

```rust
let config = ConsumerConfig {
    auto_commit: Some(Duration::from_secs(1)),
    ..Default::default()
};
```

In Node.js, the option is `autoCommitMs: 1000`.

The rule to remember: **the messages returned by a `poll` count as processed when
you call `poll` again, or close the consumer.** Process every message of a
`poll` before calling the next one. If you hand messages to another thread or
queue them for later, use manual commit instead.

## At-least-once delivery

Mojay delivers every message **at least once** to each group:

- a message is never skipped: positions only move past messages that `poll`
  returned;
- after a crash, the messages returned since the last commit are returned again,
  to the same member when it restarts or to another member of the group.

Design your processing so that seeing a message twice is harmless: make it
idempotent, or record the offsets you have processed alongside your results.
Exactly-once delivery and transactions are not part of version 1.

## Handing over a partition

When a partition moves from one member to another during a
[rebalance](./consumers-and-groups.md#rebalancing), the new owner always reads
the committed offset *after* taking the partition's lock. What it finds depends
on the old owner's commit mode:

- **Automatic commit**: the old owner commits its position before releasing the
  partition. The new owner resumes exactly where it stopped: a normal rebalance
  produces no duplicates. Only a crash replays messages since the last commit.
- **Manual commit**: the old owner releases the partition **without**
  committing. Committing on the application's behalf could mark as processed
  messages it still holds in memory, and lose them. The new owner resumes from
  the last commit: possible duplicates, no loss. To avoid these duplicates,
  commit before each `poll`, after processing the previous one.

`take_rebalance_events()` tells you which partitions were revoked, so you can
drop any state you keep for them.

## Where a consumer starts

When a member gets a partition, it picks its position like this:

| Situation | Position |
| --- | --- |
| The group committed an offset, still available | The committed offset. |
| The group never committed anything for this partition | `start_from`: `Earliest` is the oldest available message (the default); `Latest` is the read limit, so only new messages are read. |
| The committed offset was deleted by retention | `start_from` again: with `Earliest`, the new first offset of the partition. No error. |
| The committed offset is beyond the read limit | The read limit. |

The last case happens with the `Interval` or `Never` policies: a consumer
committed messages that a power loss then erased. Moving back to the read limit
means no new message is skipped when the partition fills up again.

`start_from` only applies when there is no usable committed offset. Once a group
has committed, restarting with `Latest` does not skip the backlog. To start over,
use a new group name.

## When retention deletes a consumer's position

A consumer that falls behind can find that retention deleted the messages it was
about to read. It does not fail: it applies `start_from` and continues. With
`Earliest`, it jumps to the new first offset of the partition; with `Latest`, to
the read limit. Either way, the deleted messages are never processed by that
group. Size your retention so that your
consumers' usual lag stays well inside it. See [Retention](./retention.md).
