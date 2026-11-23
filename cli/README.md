# `mojay`: the Mojay command line

`mojay` creates topics, sends and reads messages, and shows what is actually
stored in a Mojay data folder. It is the tool for debugging and exploring;
your application itself uses the library.

```bash
mojay create-topic orders --partitions 3
echo '{"order": 1}' | mojay produce orders --key client-42
mojay tail orders --follow
```

## Installation

```bash
cargo install mojay-cli
```

The `mojay` binary is then in `~/.cargo/bin`. From a clone of the repository,
`cargo install --path cli` installs the current version, and
`cargo run -p mojay-cli -- <command>` runs it without installing.

## The data folder

Every command works on a data folder, chosen in this order of priority:

1. the `--data <folder>` option (or `-d`), placed before or after the command;
2. the `MOJAY_DATA` environment variable;
3. by default, `./data`.

Only `create-topic` creates the folder if it does not exist. The other commands
refuse a folder that is not a Mojay folder, so a typo never creates an empty
folder somewhere else.

```console
$ mojay -d elsewhere topics
error: elsewhere is not a Mojay data directory (use --data or MOJAY_DATA)
```

## Commands

| Command | Purpose | Changes data? |
| --- | --- | --- |
| [`create-topic`](#create-topic) | Creates a topic | Yes |
| [`topics`](#topics) | Lists the topics | No |
| [`describe`](#describe) | Configuration, partitions, segments, group lag | No |
| [`produce`](#produce) | Sends messages | Yes |
| [`tail`](#tail) | Shows messages | No |
| [`dump`](#dump) | Shows the raw content of a segment | No |

`mojay --help` and `mojay <command> --help` list every option.

### `create-topic`

```bash
mojay create-topic <name> [options]
```

| Option | Default | Effect |
| --- | --- | --- |
| `-p`, `--partitions <n>` | `1` | Number of partitions. Fixed: adding some would change the partition of every key. |
| `--segment-size <size>` | `64MiB` | Size of a segment before moving to the next one. |
| `--fsync <policy>` | `always` | `always` (each message is on disk before it becomes readable), `never` (left to the OS), or an interval such as `100ms`. |
| `--retention-size <size>` | none | Maximum size of **one partition**; beyond it, the oldest segments are deleted. |
| `--retention-time <duration>` | none | Maximum age of a segment, since its last write. |

Names accept letters, digits, `.`, `_` and `-`. The configuration is fixed at
creation.

```console
$ mojay create-topic events -p 4 --fsync 100ms --retention-size 10GiB --retention-time 7d
topic "events" created (4 partitions)
```

### `topics`

Lists the topics, one per line, sorted by name.

### `describe`

Shows a topic's configuration, the state of each partition, and the lag of each
consumer group. It is the command to keep at hand while an application runs.

```console
$ mojay describe orders
topic       orders
partitions  3
segments    64 MiB max
fsync       always
retention   7d per segment

partition         start   read limit     messages  segments         size
0                     0            2            2         1        127 B
1                     0            2            2         1         98 B
2                     0            1            1         1         56 B

group                partition     committed          lag
billing              0                     2            0
billing              1                     1            1
billing              2                     1            0
```

Here, the `billing` group read everything, then a new message arrived on
partition 1: the group is one message behind.

- **start**: the first available offset; it moves forward when retention deletes segments.
- **read limit**: the first offset not yet readable; you can read `start..read limit`.
- **lag**: the messages the group has not committed yet on that partition.

### `produce`

Sends one message with the given value or, without a value, **one message per
line of standard input**. Prints `partition:offset` for each message.

```bash
mojay produce <topic> [value] [options]
```

| Option | Effect |
| --- | --- |
| `-k`, `--key <key>` | Message key. It picks the partition (murmur2, like Kafka): messages with the same key stay together, in order. |
| `-p`, `--partition <n>` | Forced partition. With neither key nor partition, messages are spread round-robin. |
| `-H`, `--header <key=value>` | A header. Repeatable. |
| `-q`, `--quiet` | Prints nothing. |

```console
$ mojay produce orders '{"order": 1}' -k client-42 -H source=web
0:0
$ printf 'first\nsecond\nthird\n' | mojay produce orders
0:1
1:0
2:0
```

`produce` works even while applications are writing to the same topic: writers
take turns on each partition, one batch of messages at a time.

### `tail`

Shows a topic's messages. `tail` is a plain reader: it joins no group and
commits nothing, so it has no effect on your consumers.

```bash
mojay tail <topic> [options]
```

| Option | Effect |
| --- | --- |
| `-p`, `--partition <n>` | A single partition (default: all). |
| `-n`, `--last <n>` | Only the last n messages of each partition. |
| `--offset <n>` | Start at this offset (with `--partition`). |
| `-f`, `--follow` | Keep showing new messages, until Ctrl-C. |
| `--full` | Full values (otherwise cut at 120 characters). |

Each line gives `partition:offset`, the UTC timestamp, the key, the value and
the headers:

```console
$ mojay tail orders
0:0  2026-11-20T13:37:52.209Z  key="client-42"  value="{\"order\": 1}"  source="web"
0:1  2026-11-20T13:37:52.238Z  key=null  value="first"
1:0  2026-11-20T13:37:52.244Z  key=null  value="second"
2:0  2026-11-20T13:37:52.252Z  key=null  value="third"
```

Text is shown in quotes, with special characters escaped; a binary value is
shown in hexadecimal (`0xff0010`); a missing key or value is shown as `null`.

### `dump`

Shows the raw content of segments, record by record: their position in the
file, their offset, their size and their content. It is the tool to check by
eye what is stored, or to understand a damaged file. `dump` never repairs
anything.

```bash
mojay dump <topic> [-p <partition>] [--index] [--full]
mojay dump <file.log>
```

```console
$ mojay dump orders -p 0 --index
== data/topics/orders/p0/00000000000000000000.log (127 B, base 0)
valid header, format v1
  position     offset    size  timestamp                 contents
        16          0      71  2026-11-20T13:37:52.209Z  key="client-42" value="{\"order\": 1}" source="web"
        87          1      40  2026-11-20T13:37:52.238Z  key=null value="first"
2 readable record(s)
index: 0 entry(ies)
```

On a damaged file, `dump` shows everything before the problem, then where and
why the rest cannot be read:

```console
$ mojay dump data/topics/orders/p0/00000000000000000000.log
…
1 readable record(s)
!! unreadable tail at position 87: 40 B (invalid CRC)
```

`--index` also shows the index entries and flags the inconsistent ones (normal
reads ignore them).

## Units

| Type | Examples | Accepted units |
| --- | --- | --- |
| Size | `4096`, `512B`, `64KiB`, `16MiB`, `10GiB` | `B`, `KiB`/`K`, `MiB`/`M`, `GiB`/`G`, `TiB`/`T`, always base 1024 |
| Duration | `250ms`, `30s`, `15m`, `12h`, `7d` | `ms`, `s`, `m`/`min`, `h`, `d`; the unit is required |

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success. Output closed along the way (`mojay tail orders \| head`) counts as success. |
| `1` | Error, explained on standard error (`error: …`). |
| `2` | Invalid arguments: `clap` prints the command's usage. |
