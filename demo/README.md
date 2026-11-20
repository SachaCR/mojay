# Mojay demo: one producer, several readers

Two small programs to watch Mojay work locally, with no server:

- **`mojay-demo-producer`** publishes order events as JSON to the `demo-events`
  topic (3 partitions), until Ctrl-C. Each message's key is the customer, so a
  customer's events stay in the same partition, in order.
- **`mojay-demo-consumer`** joins the `demo-loggers` group and prints the
  messages it receives, along with the partitions it gets or gives up.

## Running the demo

From the repository root, in a first terminal:

```bash
cargo run -p mojay-demo-producer
```

Then, in one to three other terminals:

```bash
cargo run -p mojay-demo-consumer
```

They all share the `./demo-data` folder, which Git ignores.

## What you will see

- **One reader** gets all 3 partitions.
- **A second reader**: within a second, the first one gives up a partition
  (`- gave partition 2 to another member`), which the second one receives
  (`+ received partition 2`).
- **Three readers**: one partition each.
- **Ctrl-C on a reader**: it commits, hands its partitions back, and the others
  pick them up exactly where it stopped, with no duplicate.
- **A killed reader** (`kill -9`): the others take over its partitions from its
  last commit (at most one second of messages is read again).
- **Restarting a reader**: it resumes where the group left off.

To follow the topic's state and the group's lag meanwhile:

```bash
cargo run -p mojay-cli -- -d ./demo-data describe demo-events
```

## Useful options

| Program | Option | Effect |
| --- | --- | --- |
| producer | `--rate 1000` | 1,000 events per second (a summary every second instead of one line per event) |
| consumer | `--group other` | Another group: it reads everything again, independently of `demo-loggers` |
| consumer | `--latest` | Without a commit, read only new messages |
| both | `--data path` | Another data folder |

## Disk space

The topic is created with a retention of about **500 MiB in total** (the limit
applies per partition: 500 MiB / 3), with 16 MiB segments. The producer can
therefore run forever: the oldest segments are deleted as it goes.

To start over: stop the programs, then delete `./demo-data`.
