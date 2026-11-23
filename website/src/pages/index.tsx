import type { CSSProperties, ReactNode } from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import Head from "@docusaurus/Head";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Layout from "@theme/Layout";
import Heading from "@theme/Heading";
import CodeBlock from "@theme/CodeBlock";

import InstallCommand from "@site/src/components/InstallCommand";
import LogAnimation from "@site/src/components/LogAnimation";

import styles from "./index.module.css";

const GITHUB_URL = "https://github.com/SachaCR/mojay";

const INSTALL_OPTIONS = [
  {
    label: "Rust",
    command: "cargo add mojay",
    registryUrl: "https://crates.io/crates/mojay",
    registryName: "crates.io",
  },
  {
    label: "Node.js",
    command: "npm install @mojay-dev/node",
    registryUrl: "https://www.npmjs.com/package/@mojay-dev/node",
    registryName: "npm",
  },
];

function Hero(): ReactNode {
  return (
    <header className={styles.hero}>
      <div className={styles.glow} aria-hidden="true" />
      <div className={clsx("container", styles.heroInner)}>
        <div className={styles.heroText}>
          <p className={styles.eyebrow}>Embedded event log · Rust · Node.js</p>
          <Heading as="h1" className={styles.title}>
            <span className={styles.gradientText}>SQLite</span> for Kafka
          </Heading>
          <p className={styles.subtitle}>
            Topics, partitions, offsets and consumer groups in a local folder.
            No server, no cluster: open a directory and start streaming, from as
            many processes as you like.
          </p>
          <div className={styles.buttons}>
            <Link className={clsx("button button--lg", styles.primaryButton)} to="/docs/intro">
              Get started
            </Link>
            <Link className={clsx("button button--lg", styles.ghostButton)} href={GITHUB_URL}>
              GitHub
            </Link>
          </div>
          <InstallCommand options={INSTALL_OPTIONS} className={styles.install} />
        </div>
        <div className={styles.heroVisual}>
          <LogAnimation />
        </div>
      </div>
    </header>
  );
}

type Feature = { title: string; description: ReactNode; accent: string };

const FEATURES: Feature[] = [
  {
    title: "No server",
    accent: "var(--mojay-orange)",
    description: (
      <>
        A Mojay database is a folder, like a SQLite file. Nothing to deploy, run
        or monitor: your application links the library and reads and writes the
        files directly.
      </>
    ),
  },
  {
    title: "Many processes",
    accent: "var(--mojay-pink)",
    description: (
      <>
        Any number of producers and consumers, in any process on the machine,
        on the same topic and even the same partition. They coordinate through
        file locks.
      </>
    ),
  },
  {
    title: "Consumer groups",
    accent: "#c64be8",
    description: (
      <>
        Members of a group share the partitions and commit offsets, like Kafka.
        When a member joins, leaves or crashes, the others rebalance, without
        any coordinator.
      </>
    ),
  },
  {
    title: "Durable by default",
    accent: "var(--mojay-violet)",
    description: (
      <>
        A message is on disk before anyone can read it, and concurrent writers
        share flushes. After a crash, recovery keeps every complete record and
        only drops a half-written tail.
      </>
    ),
  },
];

function Features(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className={styles.featureGrid}>
          {FEATURES.map((feature) => (
            <div
              key={feature.title}
              className={styles.featureCard}
              style={{ "--accent": feature.accent } as CSSProperties}
            >
              <Heading as="h3">{feature.title}</Heading>
              <p>{feature.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

const RUST_EXAMPLE = `use std::time::Duration;
use mojay::{ConsumerConfig, Db, Message, TopicConfig};

fn main() -> mojay::Result<()> {
    let db = Db::open("data")?;
    let config = TopicConfig { partitions: 3, ..Default::default() };
    db.create_topic("orders", config)?;

    // Same key, same partition: a customer's orders stay in order.
    let mut producer = db.producer("orders")?;
    producer.send(&Message::new("customer-42", r#"{"total": 99}"#))?;

    // A member of the "billing" group gets its share of the partitions.
    let config = ConsumerConfig::default();
    let mut consumer = db.consumer("billing", &["orders"], config)?;
    for record in consumer.poll(Duration::from_millis(500))? {
        println!("{}:{}", record.partition, record.offset);
    }
    consumer.commit()
}`;

const NODE_EXAMPLE = `import { Db } from "@mojay-dev/node";

const db = Db.open("data");
db.createTopic("orders", { partitions: 3 });

const producer = db.producer("orders");
const { partition, offset } = await producer.send({
  key: "customer-42",
  value: JSON.stringify({ total: 99 }),
});

// Another process can run the same code: they share the partitions.
const consumer = db.consumer("billing", ["orders"], { autoCommitMs: 1000 });
for (const record of await consumer.poll(500)) {
  console.log(record.partition, record.offset, record.value?.toString());
}
await consumer.close();`;

const CLI_EXAMPLE = `# Create a topic, send a message, follow the stream.
mojay create-topic orders --partitions 3
echo '{"total": 99}' | mojay produce orders --key customer-42
mojay tail orders --follow

# See what is really on disk.
mojay describe orders
mojay dump orders -p 0 --index`;

type Example = {
  tag: string;
  title: string;
  description: ReactNode;
  code: string;
  language: string;
  fileName: string;
  docLink: string;
  docLabel: string;
};

const EXAMPLES: Example[] = [
  {
    tag: "Rust",
    title: "A library, not a service",
    description: (
      <>
        Open a folder, create topics, produce and consume. Errors are typed,
        and damaged data on disk comes back as an error, never as a crash.
      </>
    ),
    code: RUST_EXAMPLE,
    language: "rust",
    fileName: "src/main.rs",
    docLink: "/docs/getting-started/rust",
    docLabel: "Start with Rust",
  },
  {
    tag: "Node.js",
    title: "Async, and never blocking the event loop",
    description: (
      <>
        The Node.js package wraps the same library. Each producer and consumer
        runs on its own thread, and every call returns a Promise.
      </>
    ),
    code: NODE_EXAMPLE,
    language: "ts",
    fileName: "index.ts",
    docLink: "/docs/getting-started/nodejs",
    docLabel: "Start with Node.js",
  },
  {
    tag: "CLI",
    title: "See what your app is doing",
    description: (
      <>
        The <code>mojay</code> command creates topics, sends and tails messages,
        describes partitions and consumer lag, and dumps segments byte by byte.
      </>
    ),
    code: CLI_EXAMPLE,
    language: "bash",
    fileName: "terminal",
    docLink: "/docs/getting-started/cli",
    docLabel: "Explore the CLI",
  },
];

function Examples(): ReactNode {
  return (
    <section className={styles.examples}>
      {EXAMPLES.map((example, index) => (
        <div
          key={example.tag}
          className={clsx(styles.exampleRow, index % 2 === 1 && styles.exampleRowAlt)}
        >
          <div className={clsx("container", styles.exampleInner, index % 2 === 1 && styles.reverse)}>
            <div className={styles.exampleText}>
              <span className={styles.tag}>{example.tag}</span>
              <Heading as="h2">{example.title}</Heading>
              <p>{example.description}</p>
              <Link className={clsx("button button--outline button--primary")} to={example.docLink}>
                {example.docLabel}
              </Link>
            </div>
            <div className={styles.exampleCode}>
              <CodeBlock language={example.language} title={example.fileName}>
                {example.code}
              </CodeBlock>
            </div>
          </div>
        </div>
      ))}
    </section>
  );
}

const NUMBERS = [
  { value: "5.7 M", unit: "msg/s", label: "written in batches of 100, fsync left to the OS" },
  { value: "27 k", unit: "msg/s", label: "written in batches of 100, each batch on disk first" },
  { value: "5–15 M", unit: "msg/s", label: "read from a partition" },
  { value: "< 10", unit: "MB", label: "of memory for the whole benchmark run" },
];

function Numbers(): ReactNode {
  return (
    <section className={styles.numbers}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          Fast enough to forget about it
        </Heading>
        <div className={styles.numberGrid}>
          {NUMBERS.map((number) => (
            <div key={number.label} className={styles.number}>
              <div className={styles.numberValue}>
                <span className={styles.gradientText}>{number.value}</span>
                <small>{number.unit}</small>
              </div>
              <p>{number.label}</p>
            </div>
          ))}
        </div>
        <p className={styles.numbersNote}>
          100-byte messages on an Apple M5 laptop.{" "}
          <Link to="/docs/benchmarks">Method and full results</Link>.
        </p>
      </div>
    </section>
  );
}

function Fit(): ReactNode {
  return (
    <section className={styles.fit}>
      <div className="container">
        <p className={styles.fitIntro}>
          Mojay was created to prototype Kafka-style designs locally, without
          the burden of running Kafka.
        </p>
      </div>
      <div className={clsx("container", styles.fitGrid)}>
        <div className={styles.fitCard}>
          <Heading as="h3">Made for</Heading>
          <ul>
            <li>Prototyping event-driven designs on your laptop, with no broker to install</li>
            <li>Tests: a real log in a temporary folder, created and thrown away in milliseconds</li>
            <li>CI pipelines, where starting a Kafka cluster is slow and flaky</li>
            <li>Demos and local development environments</li>
          </ul>
        </div>
        <div className={clsx(styles.fitCard, styles.fitCardMuted)}>
          <Heading as="h3">Not the right tool</Heading>
          <ul>
            <li>Several machines sharing one stream: there is no network or replication</li>
            <li>Clients that speak the Kafka protocol: Mojay is a library, not a broker</li>
            <li>Network file systems: locks and fsync need a local disk</li>
          </ul>
        </div>
      </div>
      <div className="container">
        <div className={styles.fitWarning}>
          <Heading as="h3">In production, at your own risk for now</Heading>
          <p>
            Mojay can also run in real applications on one machine: passing
            events and jobs between processes, a durable queue for a desktop,
            CLI or edge application, a local event log to replay. It is built
            with care for durability and crash recovery, but it is young and
            has not yet been proven in production. If you use it there, test
            it against your own failure scenarios and keep a way back.
          </p>
        </div>
      </div>
    </section>
  );
}

function CallToAction(): ReactNode {
  return (
    <section className={styles.cta}>
      <div className="container">
        <Heading as="h2">Stream in a folder.</Heading>
        <p>Five minutes from install to your first consumer group.</p>
        <div className={styles.buttons}>
          <Link className={clsx("button button--lg", styles.primaryButton)} to="/docs/intro">
            Read the docs
          </Link>
          <Link className={clsx("button button--lg", styles.ghostButton)} to="/docs/how-it-works/on-disk-layout">
            How it works
          </Link>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title="SQLite for Kafka"
      description="Mojay is an embedded Rust library, also for Node.js: topics, partitions, offsets and consumer groups in a local folder, with no server."
    >
      <Head>
        <meta
          name="keywords"
          content="kafka, embedded, event log, message queue, consumer groups, rust, nodejs, sqlite"
        />
        <script type="application/ld+json">
          {JSON.stringify({
            "@context": "https://schema.org",
            "@type": "SoftwareSourceCode",
            name: "Mojay",
            description:
              "An embedded event log: Kafka-style topics, partitions, offsets and consumer groups in a local folder, with no server.",
            codeRepository: GITHUB_URL,
            programmingLanguage: ["Rust", "JavaScript"],
            license: ["https://spdx.org/licenses/MIT.html", "https://spdx.org/licenses/Apache-2.0.html"],
            url: siteConfig.url,
          })}
        </script>
      </Head>
      <Hero />
      <main>
        <Features />
        <Examples />
        <Numbers />
        <Fit />
        <CallToAction />
      </main>
    </Layout>
  );
}
