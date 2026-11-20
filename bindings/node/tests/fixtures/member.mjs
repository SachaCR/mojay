// A member of group "g", started in its own process by the tests.
// It waits for a second member, reads while printing "partition:offset" per
// message, and stops after one second without receiving anything.

import { readdirSync } from 'node:fs'
import { join } from 'node:path'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const { Db } = require('../../index.js')

const dir = process.argv[2]
const db = Db.open(dir)
const consumer = db.consumer('g', ['orders'], { autoCommitMs: 0, maxPollBytes: 2000 })

const members = () => readdirSync(join(dir, 'groups', 'g', 'members')).length
const deadline = Date.now() + 30_000
while (members() < 2) {
  if (Date.now() > deadline) throw new Error('the second member never arrived')
  await new Promise((resolve) => setTimeout(resolve, 10))
}

let idleSince = Date.now()
let receivedAny = false
while (!receivedAny || Date.now() - idleSince < 1000) {
  const records = await consumer.poll(100)
  if (records.length > 0) {
    receivedAny = true
    idleSince = Date.now()
  }
  for (const r of records) process.stdout.write(`${r.partition}:${r.offset}\n`)
}
await consumer.close()
