// Realistic messages: a port of bench/src/payload.rs, which is the reference.
// Same seed, same random draws in the same order, same bytes: Rust and Node.js
// numbers measure the same messages. Keep the two files in step.

const MASK = (1n << 64n) - 1n

/** xorshift64*, on BigInt to get the same 64-bit arithmetic as Rust. */
class Rng {
  constructor(seed) {
    const state = (BigInt(seed) ^ 0x9e3779b97f4a7c15n) & MASK
    this.state = state === 0n ? 1n : state
  }

  nextU64() {
    let x = this.state
    x ^= x >> 12n
    x ^= (x << 25n) & MASK
    x ^= x >> 27n
    this.state = x
    return (x * 0x2545f4914f6cdd1dn) & MASK
  }

  /** A number in `0..n`. */
  below(n) {
    return Number(this.nextU64() % BigInt(n))
  }

  /** A number in `low..=high`. */
  between(low, high) {
    return low + this.below(high - low + 1)
  }

  pick(items) {
    return items[this.below(items.length)]
  }

  hex(digits) {
    let text = ''
    while (text.length < digits) {
      text += this.nextU64().toString(16).padStart(16, '0')
    }
    return text.slice(0, digits)
  }

  words(count) {
    const words = []
    for (let i = 0; i < count; i++) words.push(this.pick(WORDS))
    return words.join(' ')
  }

  wordsBetween(low, high) {
    return this.words(this.between(low, high))
  }
}

const USERS = 50_000
const BASE_TIMESTAMP_MS = 1_764_000_000_000
const pad = (number, width) => String(number).padStart(width, '0')

export class EventGenerator {
  constructor(seed) {
    this.rng = new Rng(seed)
    this.sequence = 0
  }

  /** `workload`: 'click', 'order', 'document' or 'mixed'. */
  next(workload) {
    let shape = workload
    if (workload === 'mixed') {
      const draw = this.rng.below(100)
      shape = draw < 80 ? 'click' : draw < 98 ? 'order' : 'document'
    }
    this.sequence += 1
    const [eventType, key, value] = this[shape]()
    const traceId = this.rng.hex(32)
    return { key, value, headers: { 'event-type': eventType, 'trace-id': traceId } }
  }

  timestamp() {
    return BASE_TIMESTAMP_MS + this.sequence * 37
  }

  click() {
    const r = this.rng
    const user = `user-${pad(r.below(USERS), 5)}`
    const session = r.hex(16)
    const category = r.pick(CATEGORIES)
    const product = pad(r.below(1_000_000), 6)
    const query = r.wordsBetween(1, 4).replaceAll(' ', '+')
    const userAgent = r.pick(USER_AGENTS)
    const width = r.pick(['390', '768', '1280', '1440', '1920'])
    const height = r.pick(['844', '1024', '800', '900', '1080'])
    const locale = r.pick(['en-US', 'fr-FR', 'de-DE', 'es-ES'])
    const value =
      `{"type":"page_view","event_id":"evt-${pad(this.sequence, 12)}","user_id":"${user}",` +
      `"session_id":"${session}","url":"https://shop.example.com/${category}/${product}",` +
      `"referrer":"https://www.example.org/search?q=${query}","user_agent":"${userAgent}",` +
      `"viewport":{"width":${width},"height":${height}},"locale":"${locale}","ts":${this.timestamp()}}`
    return ['page_view', user, value]
  }

  order() {
    const r = this.rng
    const customer = `customer-${pad(r.below(USERS), 5)}`
    const items = []
    let totalCents = 0
    const count = r.between(3, 10)
    for (let i = 0; i < count; i++) {
      const quantity = r.between(1, 5)
      const unitPriceCents = r.between(199, 19_999)
      totalCents += quantity * unitPriceCents
      const sku = pad(r.below(100_000), 5)
      const name = r.words(3)
      items.push(
        `{"sku":"SKU-${sku}","name":"${name}","quantity":${quantity},"unit_price_cents":${unitPriceCents}}`,
      )
    }
    const first = r.pick(FIRST_NAMES)
    const last = r.pick(LAST_NAMES)
    const number = r.between(1, 300)
    const street = r.pick(WORDS)
    const city = r.pick(CITIES)
    const postcode = pad(r.below(100_000), 5)
    const country = r.pick(['FR', 'DE', 'ES', 'IT', 'GB'])
    const address =
      `{"name":"${first} ${last}","street":"${number} ${street} street","city":"${city}",` +
      `"postcode":"${postcode}","country":"${country}"}`
    const method = r.pick(['card', 'paypal', 'transfer'])
    const transaction = r.hex(24)
    const notes = r.wordsBetween(5, 30)
    const value =
      `{"type":"order_created","order_id":"ord-${pad(this.sequence, 12)}","customer_id":"${customer}",` +
      `"status":"pending","currency":"EUR","items":[${items.join(',')}],"total_cents":${totalCents},` +
      `"shipping_address":${address},"billing_address":${address},"payment":{"method":"${method}",` +
      `"transaction_id":"${transaction}"},"notes":"${notes}","created_at":${this.timestamp()}}`
    return ['order_created', customer, value]
  }

  document() {
    const r = this.rng
    const document = `doc-${pad(r.below(USERS * 10), 8)}`
    const tags = []
    const tagCount = r.between(2, 8)
    for (let i = 0; i < tagCount; i++) tags.push(`"${r.pick(WORDS)}"`)
    const sections = []
    const sectionCount = r.between(24, 40)
    for (let index = 0; index < sectionCount; index++) {
      const heading = r.wordsBetween(3, 6)
      const body = r.wordsBetween(120, 180)
      sections.push(`{"index":${index},"heading":"${heading}","body":"${body}"}`)
    }
    const title = r.wordsBetween(4, 10)
    const first = r.pick(FIRST_NAMES)
    const last = r.pick(LAST_NAMES)
    const value =
      `{"type":"document_indexed","document_id":"${document}","title":"${title}",` +
      `"author":"${first} ${last}","tags":[${tags.join(',')}],"sections":[${sections.join(',')}],` +
      `"indexed_at":${this.timestamp()}}`
    return ['document_indexed', document, value]
  }
}

const POOL_MAX_MESSAGES = 2048
const POOL_MAX_BYTES = 16 * 1024 * 1024

/** Messages to cycle through, so that generating them is not measured. */
export function pool(workload, seed) {
  const generator = new EventGenerator(seed)
  const messages = []
  let bytes = 0
  while (messages.length < POOL_MAX_MESSAGES && bytes < POOL_MAX_BYTES) {
    const message = generator.next(workload)
    bytes += messageBytes(message)
    messages.push(message)
  }
  return messages
}

/** Bytes handed to Mojay: key, value and headers (all ASCII here). */
export function messageBytes(message) {
  let bytes = message.key.length + message.value.length
  for (const [key, value] of Object.entries(message.headers)) bytes += key.length + value.length
  return bytes
}

const CATEGORIES = ['shoes', 'books', 'kitchen', 'garden', 'toys', 'laptops', 'phones', 'music']

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
  'Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0',
]

const FIRST_NAMES = ['Alice', 'Bruno', 'Chloe', 'David', 'Emma', 'Farid', 'Giulia', 'Hugo', 'Ines', 'Jonas']

const LAST_NAMES = ['Martin', 'Bernard', 'Dubois', 'Garcia', 'Muller', 'Rossi', 'Smith', 'Nguyen', 'Kowalski']

const CITIES = ['Paris', 'Lyon', 'Berlin', 'Madrid', 'Milan', 'London', 'Lisbon', 'Brussels']

const WORDS = [
  'amber', 'bridge', 'canvas', 'delta', 'ember', 'falcon', 'garnet', 'harbor', 'island',
  'jasmine', 'kernel', 'lantern', 'meadow', 'nectar', 'orbit', 'pepper', 'quartz', 'river',
  'saffron', 'timber', 'umbra', 'velvet', 'willow', 'xenon', 'yonder', 'zephyr', 'anchor',
  'beacon', 'cobalt', 'dune', 'echo', 'fjord', 'glacier', 'horizon', 'indigo', 'juniper', 'kelp',
  'lagoon', 'marble', 'nimbus', 'onyx', 'prairie', 'quill', 'ripple', 'summit', 'tundra',
  'upland', 'vortex', 'walnut', 'yarrow',
]
