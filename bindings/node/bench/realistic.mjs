// Realistic benchmark of the Node.js binding: the JSON events of
// bench/src/payload.rs (ported in payload.mjs), on 8 partitions.
//
//   npm run build        # release build: debug numbers are meaningless
//   npm run bench
//   npm run bench -- --seconds 5 --only delivery
//
// Each scenario runs in its own temporary data folder, deleted at the end.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'
import { createRequire } from 'node:module'
import { setTimeout as sleep } from 'node:timers/promises'

import { messageBytes, pool } from './payload.mjs'

const require = createRequire(import.meta.url)
const { Db } = require('../index.js')

const TOPIC = 'events'
const PARTITIONS = 8
const SEED = 2
/** Carries the time (ms, from `performance.now()`) the message was due to be sent. */
const SENT_AT_HEADER = 'bench-sent-at'
/** Sends in flight at once in the pipelined scenario. */
const WINDOW = 100
/** Bounds the promises kept while sending at a fixed rate. */
const MAX_PENDING = 1000
const BACKLOG_BYTES = 128 * 1024 * 1024
const MIB = 1024 * 1024

const { values: args } = parseArgs({
  options: {
    seconds: { type: 'string', default: '2' },
    only: { type: 'string' },
  },
})
const SECONDS = Number(args.seconds)

/** Every scenario, in run order: [name, function returning a result]. */
function scenarios() {
  const list = []
  for (const fsync of ['always', 'never']) {
    list.push([`ingest mixed, ${fsync}, await each send`, () => ingestAwaited(fsync)])
    list.push([`ingest mixed, ${fsync}, ${WINDOW} sends in flight`, () => ingestPipelined(fsync)])
  }
  for (const [fsync, rate] of [['always', 100], ['never', 1_000], ['never', 10_000]]) {
    list.push([`delivery, ${fsync}, ${rate.toLocaleString('en-US')} msg/s`, () => delivery(fsync, rate)])
  }
  list.push([`catch-up: read ${BACKLOG_BYTES / MIB} MiB with a group`, () => catchUp(false)])
  list.push([`catch-up: read ${BACKLOG_BYTES / MIB} MiB with a group + JSON.parse`, () => catchUp(true)])
  return list
}

/** A fresh data folder with the `events` topic; `cleanup` deletes it. */
function newTopic(fsync) {
  const dir = mkdtempSync(join(tmpdir(), 'mojay-bench-'))
  const db = Db.open(dir)
  db.createTopic(TOPIC, { partitions: PARTITIONS, fsync })
  return { db, cleanup: () => rmSync(dir, { recursive: true, force: true }) }
}

function diskBytes(db) {
  return db.describeTopic(TOPIC).partitions.reduce((sum, p) => sum + p.sizeBytes, 0)
}

/** One `await producer.send()` at a time. */
async function ingestAwaited(fsync) {
  const { db, cleanup } = newTopic(fsync)
  const producer = db.producer(TOPIC)
  const messages = pool('mixed', SEED)
  const latencies = []
  let bytes = 0
  const started = performance.now()
  const deadline = started + SECONDS * 1000
  while (performance.now() < deadline) {
    const message = messages[latencies.length % messages.length]
    const before = performance.now()
    await producer.send(message)
    latencies.push(performance.now() - before)
    bytes += messageBytes(message)
  }
  const elapsed = performance.now() - started
  await producer.close()
  const disk = diskBytes(db)
  cleanup()
  return { messages: latencies.length, bytes, elapsed, disk, label: 'per send', latencies }
}

/** `WINDOW` sends in flight, awaited together: the producer thread queues them. */
async function ingestPipelined(fsync) {
  const { db, cleanup } = newTopic(fsync)
  const producer = db.producer(TOPIC)
  const messages = pool('mixed', SEED)
  const latencies = []
  let bytes = 0
  const started = performance.now()
  const deadline = started + SECONDS * 1000
  while (performance.now() < deadline) {
    const window = []
    for (let i = 0; i < WINDOW; i++) {
      const message = messages[(latencies.length + i) % messages.length]
      const before = performance.now()
      window.push(producer.send(message).then(() => latencies.push(performance.now() - before)))
      bytes += messageBytes(message)
    }
    await Promise.all(window)
  }
  const elapsed = performance.now() - started
  await producer.close()
  const disk = diskBytes(db)
  cleanup()
  return { messages: latencies.length, bytes, elapsed, disk, label: 'per send', latencies }
}

/**
 * A producer at `rate` messages per second and a consumer, in this process.
 * Latency runs from the time each message was due, so a producer falling
 * behind counts (no coordinated omission).
 */
async function delivery(fsync, rate) {
  const { db, cleanup } = newTopic(fsync)
  const producer = db.producer(TOPIC)
  const consumer = db.consumer('app', [TOPIC], { autoCommitMs: 1000 })
  await consumer.poll(100) // joins the group and takes the partitions
  const total = Math.floor(rate * SECONDS)
  const latencies = []
  const consuming = (async () => {
    while (latencies.length < total) {
      for (const record of await consumer.poll(100)) {
        const sentAt = Number(record.headers[SENT_AT_HEADER])
        latencies.push(performance.now() - sentAt)
      }
    }
  })()
  const started = performance.now()
  const bytes = await produceAtRate(producer, rate, total)
  await consuming
  const elapsed = performance.now() - started
  await producer.close()
  await consumer.close()
  const disk = diskBytes(db)
  cleanup()
  return { messages: total, bytes, elapsed, disk, label: 'delivery', latencies }
}

async function produceAtRate(producer, rate, total) {
  const messages = pool('mixed', SEED)
  const interval = 1000 / rate
  const origin = performance.now()
  let pending = []
  let bytes = 0
  for (let i = 0; i < total; i++) {
    const due = origin + i * interval
    const wait = due - performance.now()
    // Timers are no finer than a millisecond: smaller waits are caught up in bursts.
    if (wait >= 1) await sleep(wait)
    const message = messages[i % messages.length]
    const headers = { ...message.headers, [SENT_AT_HEADER]: due.toFixed(3) }
    pending.push(producer.send({ ...message, headers }))
    bytes += messageBytes(message)
    if (pending.length >= MAX_PENDING) {
      await Promise.all(pending)
      pending = []
    }
  }
  await Promise.all(pending)
  return bytes
}

/** A consumer comes back to a backlog and reads it all, parsing each value or not. */
async function catchUp(parse) {
  const { db, cleanup } = newTopic('never')
  const { messages: written, bytes } = await prefill(db)
  const consumer = db.consumer('catch-up', [TOPIC], { autoCommitMs: 1000 })
  let read = 0
  let parsedFields = 0
  const started = performance.now()
  while (read < written) {
    for (const record of await consumer.poll(100)) {
      if (parse) parsedFields += Object.keys(JSON.parse(record.value.toString())).length
      read += 1
    }
  }
  const elapsed = performance.now() - started
  await consumer.close()
  const disk = diskBytes(db)
  cleanup()
  if (parse && parsedFields === 0) throw new Error('nothing parsed')
  return { messages: read, bytes, elapsed, disk }
}

async function prefill(db) {
  const producer = db.producer(TOPIC)
  const messages = pool('mixed', SEED)
  let count = 0
  let bytes = 0
  while (bytes < BACKLOG_BYTES) {
    const window = []
    for (let i = 0; i < MAX_PENDING; i++) {
      const message = messages[count % messages.length]
      window.push(producer.send(message))
      bytes += messageBytes(message)
      count += 1
    }
    await Promise.all(window)
  }
  await producer.close()
  return { messages: count, bytes }
}

// Output, in the same columns as the Rust bench.

const COLUMNS = [
  ['scenario', 56, 'left'],
  ['messages', 10],
  ['msg/s', 9],
  ['MiB/s', 7],
  ['on disk', 9],
  ['latency', 9, 'left'],
  ['p50', 9],
  ['p99', 9],
  ['p99.9', 9],
  ['max', 9],
]

function tableRow(cells) {
  const padded = cells.map((cell, i) => {
    const [, width, align] = COLUMNS[i]
    return align === 'left' ? cell.padEnd(width) : cell.padStart(width)
  })
  return `| ${padded.join(' | ')} |`
}

function separator() {
  const dashes = COLUMNS.map(([, width, align]) => '-'.repeat(width - 1) + (align === 'left' ? '-' : ':'))
  return `| ${dashes.join(' | ')} |`
}

function percentile(sorted, fraction) {
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * fraction) - 1))]
}

function formatMs(ms) {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`
  if (ms >= 1) return `${ms.toFixed(1)} ms`
  return `${Math.round(ms * 1000)} µs`
}

function formatRate(rate) {
  if (rate >= 1e6) return `${(rate / 1e6).toFixed(2)} M`
  if (rate >= 1e3) return `${(rate / 1e3).toFixed(1)} k`
  return rate.toFixed(0)
}

function formatBytes(bytes) {
  if (bytes >= 1024 * MIB) return `${(bytes / 1024 / MIB).toFixed(1)} GiB`
  if (bytes >= MIB) return `${Math.round(bytes / MIB)} MiB`
  return `${Math.round(bytes / 1024)} KiB`
}

function cells(name, result) {
  const seconds = result.elapsed / 1000
  let latency = ['', '', '', '', '']
  if (result.latencies?.length) {
    const sorted = Float64Array.from(result.latencies).sort()
    latency = [
      result.label,
      ...[0.5, 0.99, 0.999].map((f) => formatMs(percentile(sorted, f))),
      formatMs(sorted[sorted.length - 1]),
    ]
  }
  return [
    name,
    result.messages.toLocaleString('en-US'),
    formatRate(result.messages / seconds),
    (result.bytes / seconds / MIB).toFixed(1),
    formatBytes(result.disk),
    ...latency,
  ]
}

console.log(tableRow(COLUMNS.map(([title]) => title)))
console.log(separator())
for (const [name, run] of scenarios()) {
  if (args.only && !name.includes(args.only)) continue
  console.log(tableRow(cells(name, await run())))
}
