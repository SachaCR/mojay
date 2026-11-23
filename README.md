# Mojay

[![CI](https://github.com/SachaCR/mojay/actions/workflows/ci.yml/badge.svg)](https://github.com/SachaCR/mojay/actions/workflows/ci.yml)

**SQLite for Kafka.** Mojay is an embedded Rust library that gives you topics,
partitions, offsets and consumer groups in a local folder, with no server.
Any number of processes on the machine can produce and consume at the same
time; they coordinate through file locks.

Documentation: **[mojay.dev](https://mojay.dev)**

```rust
use std::time::Duration;
use mojay::{ConsumerConfig, Db, Message, TopicConfig};

fn main() -> mojay::Result<()> {
    let db = Db::open("data")?;
    db.create_topic("orders", TopicConfig { partitions: 3, ..Default::default() })?;

    let mut producer = db.producer("orders")?;
    producer.send(&Message::new("customer-42", r#"{"total": 99}"#))?;

    let mut consumer = db.consumer("billing", &["orders"], ConsumerConfig::default())?;
    for record in consumer.poll(Duration::from_millis(500))? {
        println!("partition {} offset {}", record.partition, record.offset);
    }
    consumer.commit()
}
```

The same from Node.js:

```js
import { Db } from "@mojay-dev/node";

const db = Db.open("data");
db.createTopic("orders", { partitions: 3 });
await db.producer("orders").send({ key: "customer-42", value: '{"total": 99}' });
```

## What's in this repository

| Folder | Content |
| --- | --- |
| `src/` | The `mojay` library. |
| [`cli/`](https://github.com/SachaCR/mojay/blob/main/cli/README.md) | The `mojay` command: create topics, produce, tail, describe, dump segments. |
| [`bindings/node/`](https://github.com/SachaCR/mojay/blob/main/bindings/node/README.md) | The Node.js package (napi-rs). |
| [`bench/`](https://github.com/SachaCR/mojay/blob/main/bench/README.md) | Throughput and latency benchmarks. |
| [`demo/`](https://github.com/SachaCR/mojay/blob/main/demo/README.md) | A producer and a consumer group, as separate programs. |
| [`website/`](https://github.com/SachaCR/mojay/blob/main/website/README.md) | The [mojay.dev](https://mojay.dev) website (Docusaurus). |
| [`docs/conception.md`](https://github.com/SachaCR/mojay/blob/main/docs/conception.md) | The design note, in French: the reference for the disk format and behavior. |

Mojay runs on macOS and Linux, on a local disk.

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/SachaCR/mojay/blob/main/LICENSE-APACHE) or
[MIT license](https://github.com/SachaCR/mojay/blob/main/LICENSE-MIT), at your option.
