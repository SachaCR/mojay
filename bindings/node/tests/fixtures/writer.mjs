// A producer started in its own process by the tests: sends 500 messages
// "<writer>:<seq>" to the `orders` topic, which another process writes to as well.

import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const { Db } = require('../../index.js')

const [dir, writer] = process.argv.slice(2)
const producer = Db.open(dir).producer('orders')
for (let seq = 0; seq < 500; seq++) {
  await producer.send({ value: `${writer}:${seq}` })
}
await producer.close()
