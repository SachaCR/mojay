// Node.js binding tests: `npm test` (after `npm run build:debug`).

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const { Db } = require('../index.js')

/** A temporary data directory, deleted at the end of the test. */
function newDb(t) {
  const dir = mkdtempSync(join(tmpdir(), 'mojay-node-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  return { dir, db: Db.open(dir) }
}

/** Calls `poll` until it has `count` messages, or fails after 10 s. */
async function pollExactly(consumer, count) {
  const records = []
  const deadline = Date.now() + 10_000
  while (records.length < count) {
    assert.ok(Date.now() < deadline, `only ${records.length} messages out of ${count}`)
    records.push(...(await consumer.poll(100)))
  }
  assert.equal(records.length, count, 'too many messages')
  return records
}

async function rejectsWithCode(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof Error)
    assert.equal(error.code, code)
    return true
  })
}

function throwsWithCode(fn, code) {
  assert.throws(fn, (error) => {
    assert.ok(error instanceof Error)
    assert.equal(error.code, code)
    return true
  })
}

test('a sent message is read back unchanged', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders')
  const producer = db.producer('orders')
  const sent = await producer.send({
    key: 'client-42',
    value: Buffer.from([0, 1, 2, 255]),
    headers: { source: 'web', raw: Buffer.from('ok') },
    timestamp: 1_700_000_000_000,
  })
  assert.deepEqual(sent, { partition: 0, offset: 0 })
  await producer.send({ key: 'client-42' }) // no value: tombstone

  const consumer = db.consumer('billing', ['orders'])
  const [first, tombstone] = await pollExactly(consumer, 2)
  assert.equal(first.topic, 'orders')
  assert.equal(first.key.toString(), 'client-42')
  assert.deepEqual([...first.value], [0, 1, 2, 255])
  assert.equal(first.headers.source.toString(), 'web')
  assert.equal(first.headers.raw.toString(), 'ok')
  assert.equal(first.timestamp, 1_700_000_000_000)
  assert.equal(tombstone.offset, 1)
  assert.equal(tombstone.value, undefined)
  await Promise.all([producer.close(), consumer.close()])
})

test('sends keep call order, even without await', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders', { fsync: 'never' })
  const producer = db.producer('orders')
  const sends = Array.from({ length: 200 }, (_, i) => producer.send({ value: String(i) }))
  const offsets = (await Promise.all(sends)).map((r) => r.offset)
  assert.deepEqual(offsets, [...Array(200).keys()])

  const consumer = db.consumer('billing', ['orders'])
  const values = (await pollExactly(consumer, 200)).map((r) => Number(r.value.toString()))
  assert.deepEqual(values, [...Array(200).keys()])
  await Promise.all([producer.close(), consumer.close()])
})

test('a waiting poll does not block the event loop', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders')
  const consumer = db.consumer('billing', ['orders'])

  let ticks = 0
  const timer = setInterval(() => ticks++, 10)
  const started = Date.now()
  const empty = await consumer.poll(300)
  clearInterval(timer)
  assert.deepEqual(empty, [])
  assert.ok(Date.now() - started >= 290, 'poll must wait for its timeout')
  assert.ok(ticks >= 10, `the event loop ran during the wait (${ticks} ticks)`)

  // A message arriving during the wait ends it immediately.
  const producer = db.producer('orders')
  const waiting = consumer.poll(10_000)
  setTimeout(() => producer.send({ value: 'at last' }), 100)
  const started2 = Date.now()
  const records = await waiting
  assert.equal(records[0].value.toString(), 'at last')
  assert.ok(Date.now() - started2 < 3000)
  await Promise.all([producer.close(), consumer.close()])
})

test('a consumer resumes from the last commit', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders')
  const producer = db.producer('orders')
  for (let i = 0; i < 10; i++) await producer.send({ value: String(i) })

  const first = db.consumer('billing', ['orders'])
  await pollExactly(first, 10)
  await first.commit()
  await first.close()

  await producer.send({ value: '10' })
  const second = db.consumer('billing', ['orders'])
  const [next] = await pollExactly(second, 1)
  assert.equal(next.offset, 10)
  await Promise.all([producer.close(), second.close()])
})

test('two members of a group share the partitions, with no duplicates', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders', { partitions: 4, fsync: 'never' })
  const producer = db.producer('orders')
  for (let i = 0; i < 400; i++) await producer.send({ value: String(i), partition: i % 4 })

  const options = { autoCommitMs: 0, maxPollBytes: 2000 }
  const a = db.consumer('g', ['orders'], options)
  const b = db.consumer('g', ['orders'], options)
  const seen = new Set()
  const pollBoth = async () => {
    for (const consumer of [a, b]) {
      for (const r of await consumer.poll(20)) {
        const id = `${r.partition}:${r.offset}`
        assert.ok(!seen.has(id), `duplicate ${id}`)
        seen.add(id)
      }
    }
  }
  const deadline = Date.now() + 10_000
  while (seen.size < 400) {
    assert.ok(Date.now() < deadline, `${seen.size} messages read`)
    await pollBoth()
  }
  // Rebalancing runs inside `poll`, at most every 200 ms: on a fast machine,
  // `a` can read everything before `b` gets its share. Keep polling until the
  // shares converge, as the Rust tests do.
  let owned = await Promise.all([a.assignment(), b.assignment()])
  while (owned[0].length !== 2 || owned[1].length !== 2) {
    assert.ok(Date.now() < deadline, `assignment ${owned[0].length}/${owned[1].length}, expected 2/2`)
    await pollBoth()
    owned = await Promise.all([a.assignment(), b.assignment()])
  }
  const [ownedA, ownedB] = owned
  const all = [...ownedA, ...ownedB].map((p) => p.partition).sort()
  assert.deepEqual(all, [0, 1, 2, 3], 'each partition owned by exactly one member')
  assert.equal(seen.size, 400, 'nothing read twice after the rebalance')
  const events = await b.rebalanceEvents()
  assert.ok(events.every((e) => e.kind === 'assigned'))
  await Promise.all([producer.close(), a.close(), b.close()])
})

test('close() is final, and closing twice does nothing', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders')
  const producer = db.producer('orders')
  await producer.send({ value: 'before' })
  await producer.close()
  await producer.close()
  await rejectsWithCode(producer.send({ value: 'after' }), 'MOJAY_CLOSED')
})

test('two producers write to the same partition, each in its own order', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders', { fsync: 'never', segmentBytes: 600 })
  const a = db.producer('orders')
  const b = db.producer('orders')
  const sends = []
  for (let i = 0; i < 100; i++) {
    sends.push(a.send({ value: `a:${i}` }), b.send({ value: `b:${i}` }))
  }
  const offsets = (await Promise.all(sends)).map((r) => r.offset).sort((x, y) => x - y)
  assert.deepEqual(offsets, [...Array(200).keys()], 'every offset given once')

  const consumer = db.consumer('check', ['orders'])
  const values = (await pollExactly(consumer, 200)).map((r) => r.value.toString())
  for (const writer of ['a', 'b']) {
    const seqs = values.filter((v) => v.startsWith(`${writer}:`)).map((v) => Number(v.slice(2)))
    assert.deepEqual(seqs, [...Array(100).keys()], `messages of ${writer} in order`)
  }
  await Promise.all([a.close(), b.close(), consumer.close()])
})

test('producers in two Node processes write to the same topic', async (t) => {
  const { dir, db } = newDb(t)
  db.createTopic('orders', { fsync: 'never', segmentBytes: 4096 })

  const child = (writer) =>
    new Promise((resolve, reject) => {
      const p = spawn(process.execPath, [join(import.meta.dirname, 'fixtures', 'writer.mjs'), dir, writer])
      p.stderr.on('data', (chunk) => process.stderr.write(chunk))
      p.on('exit', (code) => (code === 0 ? resolve() : reject(new Error(`exit code ${code}`))))
    })
  await Promise.all([child('p1'), child('p2')])

  const consumer = db.consumer('check', ['orders'])
  const values = (await pollExactly(consumer, 1000)).map((r) => r.value.toString())
  for (const writer of ['p1', 'p2']) {
    const seqs = values.filter((v) => v.startsWith(`${writer}:`)).map((v) => Number(v.split(':')[1]))
    assert.deepEqual(seqs, [...Array(500).keys()], `messages of ${writer} in order`)
  }
  await consumer.close()
})

test('errors carry a stable code', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders', { partitions: 2 })
  throwsWithCode(() => db.createTopic('orders'), 'MOJAY_TOPIC_EXISTS')
  throwsWithCode(() => db.createTopic('../x'), 'MOJAY_INVALID_NAME')
  throwsWithCode(() => db.createTopic('x', { fsync: 'sometimes' }), 'MOJAY_INVALID_ARGUMENT')
  throwsWithCode(() => db.createTopic('x', { retentionBytes: -1 }), 'MOJAY_INVALID_ARGUMENT')
  throwsWithCode(() => db.createTopic('x', { partitions: 0 }), 'MOJAY_INVALID_CONFIG')
  throwsWithCode(() => db.producer('missing'), 'MOJAY_UNKNOWN_TOPIC')
  throwsWithCode(() => db.consumer('g', ['orders'], { startFrom: 'middle' }), 'MOJAY_INVALID_ARGUMENT')
  const producer = db.producer('orders')
  await rejectsWithCode(producer.send({ value: 'x', partition: 9 }), 'MOJAY_UNKNOWN_PARTITION')
  await producer.close()
})

test('describeTopic gives the offsets and size of each partition', async (t) => {
  const { db } = newDb(t)
  db.createTopic('orders', { partitions: 2, retentionBytes: 1 << 20, fsync: 100 })
  const producer = db.producer('orders')
  for (let i = 0; i < 3; i++) await producer.send({ value: 'x', partition: 1 })
  await producer.close()
  assert.deepEqual(db.listTopics(), ['orders'])
  const [p0, p1] = db.describeTopic('orders').partitions
  assert.equal(p0.readLimit, 0)
  assert.equal(p1.readLimit, 3)
  assert.equal(p1.startOffset, 0)
  assert.ok(p1.sizeBytes > p0.sizeBytes)
})

test('two Node processes share a group', async (t) => {
  const { dir, db } = newDb(t)
  db.createTopic('orders', { partitions: 4, fsync: 'never' })

  // Each member says "ready" once the group is balanced; the messages are
  // written only then, so that each one gets its share whatever its speed.
  const child = () => {
    let markReady
    const ready = new Promise((resolve) => (markReady = resolve))
    const done = new Promise((resolve, reject) => {
      const p = spawn(process.execPath, [join(import.meta.dirname, 'fixtures', 'member.mjs'), dir])
      let out = ''
      p.stdout.on('data', (chunk) => {
        out += chunk
        if (out.startsWith('ready\n')) markReady()
      })
      p.stderr.on('data', (chunk) => process.stderr.write(chunk))
      p.on('exit', (code) => (code === 0 ? resolve(out) : reject(new Error(`code ${code}`))))
    })
    return { ready, done }
  }
  const members = [child(), child()]
  await Promise.all(members.map((m) => m.ready))

  const producer = db.producer('orders')
  for (let i = 0; i < 2000; i++) await producer.send({ value: String(i), partition: i % 4 })
  await producer.close()
  const outputs = await Promise.all(members.map((m) => m.done))

  const seen = new Set()
  for (const output of outputs) {
    const ids = output.split('\n').filter((line) => line && line !== 'ready')
    assert.ok(ids.length > 0, 'each process received a share of the work')
    for (const id of ids) {
      assert.ok(!seen.has(id), `duplicate across processes: ${id}`)
      seen.add(id)
    }
  }
  assert.equal(seen.size, 2000)
})
