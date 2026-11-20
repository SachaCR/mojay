// A producer and a consumer in the same script, on ./quickstart-data.
// Run from bindings/node, after `npm run build`: node examples/quickstart.mjs

import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { Db } = require("../index.js");

const db = Db.open("./quickstart-data");

try {
  db.createTopic("orders", {
    partitions: 3,
    retentionBytes: 100 * 1024 * 1024,
  });
} catch (error) {
  if (error.code !== "MOJAY_TOPIC_EXISTS") throw error;
}

// The topic's writer: only one at a time, across all processes.
const producer = db.producer("orders");

for (const customer of ["client-1", "client-2", "client-3", "client-1"]) {
  const order = { customer, amountCents: Math.round(Math.random() * 10_000) };

  const { partition, offset } = await producer.send({
    key: customer, // a customer's orders stay in the same partition, in order
    value: JSON.stringify(order),
    headers: { source: "quickstart" },
  });

  console.log(
    `sent     ${customer} -> partition ${partition}, offset ${offset}`,
  );
}

await producer.close();

// A member of the "billing" group: rerunning the script only reads new messages.
const consumer = db.consumer("billing", ["orders"], { autoCommitMs: 1000 });

for (const record of await consumer.poll(1000)) {
  const order = JSON.parse(record.value.toString());

  console.log(
    `received p${record.partition}:${record.offset} ${order.customer} €${(order.amountCents / 100).toFixed(2)}`,
  );
}

await consumer.close(); // commits and hands the partitions back to the group
