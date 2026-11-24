//! Realistic messages: JSON events of three shapes, generated from a seed so
//! that every run writes the same bytes. Each event has a key (the entity it
//! is about, so the partitioner spreads them) and two headers, like most
//! Kafka traffic.
//!
//! The JSON is built by hand: the generated strings never contain a quote or
//! a backslash, so no escaping is needed, and the bench needs no JSON crate.
//! `bindings/node/bench/realistic.mjs` ports this file: keep them in step.

use std::fmt::Write;

use mojay::Message;

/// A message shape, from the smallest to the largest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// A page view from a clickstream, about 500 B.
    Click,
    /// An order with its line items and address, about 1.3 KiB.
    Order,
    /// A document with nested sections of text, about 35 KiB.
    Document,
}

/// What a scenario sends: one shape only, or a mix that looks like real traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    Only(Shape),
    /// 80 % clicks, 18 % orders, 2 % documents.
    Mixed,
}

impl Workload {
    pub fn name(self) -> &'static str {
        match self {
            Workload::Only(Shape::Click) => "click",
            Workload::Only(Shape::Order) => "order",
            Workload::Only(Shape::Document) => "document",
            Workload::Mixed => "mixed",
        }
    }
}

/// Generated messages are reused in a loop, so that generating JSON is not
/// part of what is measured; the pool stays below these bounds.
const POOL_MAX_MESSAGES: usize = 2048;
const POOL_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// A pool of messages to cycle through, generated from `seed`.
pub fn pool(workload: Workload, seed: u64) -> Vec<Message> {
    let mut generator = EventGenerator::new(seed);
    let mut messages = Vec::new();
    let mut bytes = 0;
    while messages.len() < POOL_MAX_MESSAGES && bytes < POOL_MAX_BYTES {
        let message = generator.next(workload);
        bytes += message_bytes(&message);
        messages.push(message);
    }
    messages
}

/// Bytes a user hands to Mojay for this message: key, value and headers.
pub fn message_bytes(message: &Message) -> u64 {
    let len = |bytes: &Option<Vec<u8>>| bytes.as_ref().map_or(0, Vec::len);
    let headers: usize = message
        .headers
        .iter()
        .map(|h| h.key.len() + h.value.len())
        .sum();
    (len(&message.key) + len(&message.value) + headers) as u64
}

/// xorshift64*: tiny, fast, and easy to port to JavaScript with BigInt.
/// Not for anything but test data.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // A zero state would stay zero forever.
        Self((seed ^ 0x9E37_79B9_7F4A_7C15).max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A number in `0..n` (`n` > 0); the modulo bias is irrelevant here.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// A number in `low..=high`.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        low + self.below(high - low + 1)
    }

    pub fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len() as u64) as usize]
    }

    fn hex(&mut self, digits: usize) -> String {
        let mut text = String::with_capacity(digits);
        while text.len() < digits {
            let _ = write!(text, "{:016x}", self.next_u64());
        }
        text.truncate(digits);
        text
    }

    /// Between `low` and `high` words, separated by spaces.
    fn words_between(&mut self, low: u64, high: u64) -> String {
        let count = self.between(low, high);
        self.words(count)
    }

    fn words(&mut self, count: u64) -> String {
        let words: Vec<&str> = (0..count).map(|_| self.pick(WORDS)).collect();
        words.join(" ")
    }
}

/// Distinct users and customers: keys repeat, as they do in real traffic.
const USERS: u64 = 50_000;
/// 2025-11-24, in milliseconds: event times start here and move forward.
const BASE_TIMESTAMP_MS: u64 = 1_764_000_000_000;

pub struct EventGenerator {
    rng: Rng,
    sequence: u64,
}

impl EventGenerator {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Rng::new(seed),
            sequence: 0,
        }
    }

    pub fn next(&mut self, workload: Workload) -> Message {
        let shape = match workload {
            Workload::Only(shape) => shape,
            Workload::Mixed => match self.rng.below(100) {
                0..80 => Shape::Click,
                80..98 => Shape::Order,
                _ => Shape::Document,
            },
        };
        self.sequence += 1;
        let (event_type, key, value) = match shape {
            Shape::Click => self.click(),
            Shape::Order => self.order(),
            Shape::Document => self.document(),
        };
        let trace_id = self.rng.hex(32);
        Message::new(key, value)
            .header("event-type", event_type)
            .header("trace-id", trace_id)
    }

    fn timestamp(&self) -> u64 {
        BASE_TIMESTAMP_MS + self.sequence * 37
    }

    fn click(&mut self) -> (&'static str, String, String) {
        let user = format!("user-{:05}", self.rng.below(USERS));
        let (sequence, timestamp) = (self.sequence, self.timestamp());
        let r = &mut self.rng;
        let value = format!(
            concat!(
                r#"{{"type":"page_view","event_id":"evt-{:012}","user_id":"{}","#,
                r#""session_id":"{}","url":"https://shop.example.com/{}/{:06}","#,
                r#""referrer":"https://www.example.org/search?q={}","user_agent":"{}","#,
                r#""viewport":{{"width":{},"height":{}}},"locale":"{}","ts":{}}}"#
            ),
            sequence,
            user,
            r.hex(16),
            r.pick(CATEGORIES),
            r.below(1_000_000),
            r.words_between(1, 4).replace(' ', "+"),
            r.pick(USER_AGENTS),
            r.pick(&["390", "768", "1280", "1440", "1920"]),
            r.pick(&["844", "1024", "800", "900", "1080"]),
            r.pick(&["en-US", "fr-FR", "de-DE", "es-ES"]),
            timestamp,
        );
        ("page_view", user, value)
    }

    fn order(&mut self) -> (&'static str, String, String) {
        let customer = format!("customer-{:05}", self.rng.below(USERS));
        let (sequence, timestamp) = (self.sequence, self.timestamp());
        let r = &mut self.rng;
        let mut items = Vec::new();
        let mut total_cents = 0;
        for _ in 0..r.between(3, 10) {
            let quantity = r.between(1, 5);
            let unit_price_cents = r.between(199, 19_999);
            total_cents += quantity * unit_price_cents;
            items.push(format!(
                r#"{{"sku":"SKU-{:05}","name":"{}","quantity":{},"unit_price_cents":{}}}"#,
                r.below(100_000),
                r.words(3),
                quantity,
                unit_price_cents
            ));
        }
        let address = format!(
            r#"{{"name":"{} {}","street":"{} {} street","city":"{}","postcode":"{:05}","country":"{}"}}"#,
            r.pick(FIRST_NAMES),
            r.pick(LAST_NAMES),
            r.between(1, 300),
            r.pick(WORDS),
            r.pick(CITIES),
            r.below(100_000),
            r.pick(&["FR", "DE", "ES", "IT", "GB"]),
        );
        let value = format!(
            concat!(
                r#"{{"type":"order_created","order_id":"ord-{:012}","customer_id":"{}","#,
                r#""status":"pending","currency":"EUR","items":[{}],"total_cents":{},"#,
                r#""shipping_address":{},"billing_address":{},"payment":{{"method":"{}","#,
                r#""transaction_id":"{}"}},"notes":"{}","created_at":{}}}"#
            ),
            sequence,
            customer,
            items.join(","),
            total_cents,
            address,
            address,
            r.pick(&["card", "paypal", "transfer"]),
            r.hex(24),
            r.words_between(5, 30),
            timestamp,
        );
        ("order_created", customer, value)
    }

    fn document(&mut self) -> (&'static str, String, String) {
        let document = format!("doc-{:08}", self.rng.below(USERS * 10));
        let timestamp = self.timestamp();
        let r = &mut self.rng;
        let tags: Vec<String> = (0..r.between(2, 8))
            .map(|_| format!(r#""{}""#, r.pick(WORDS)))
            .collect();
        let sections: Vec<String> = (0..r.between(24, 40))
            .map(|index| {
                format!(
                    r#"{{"index":{},"heading":"{}","body":"{}"}}"#,
                    index,
                    r.words_between(3, 6),
                    r.words_between(120, 180)
                )
            })
            .collect();
        let value = format!(
            concat!(
                r#"{{"type":"document_indexed","document_id":"{}","title":"{}","#,
                r#""author":"{} {}","tags":[{}],"sections":[{}],"indexed_at":{}}}"#
            ),
            document,
            r.words_between(4, 10),
            r.pick(FIRST_NAMES),
            r.pick(LAST_NAMES),
            tags.join(","),
            sections.join(","),
            timestamp,
        );
        ("document_indexed", document, value)
    }
}

const CATEGORIES: &[&str] = &[
    "shoes", "books", "kitchen", "garden", "toys", "laptops", "phones", "music",
];

const USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
];

const FIRST_NAMES: &[&str] = &[
    "Alice", "Bruno", "Chloe", "David", "Emma", "Farid", "Giulia", "Hugo", "Ines", "Jonas",
];

const LAST_NAMES: &[&str] = &[
    "Martin", "Bernard", "Dubois", "Garcia", "Muller", "Rossi", "Smith", "Nguyen", "Kowalski",
];

const CITIES: &[&str] = &[
    "Paris", "Lyon", "Berlin", "Madrid", "Milan", "London", "Lisbon", "Brussels",
];

const WORDS: &[&str] = &[
    "amber", "bridge", "canvas", "delta", "ember", "falcon", "garnet", "harbor", "island",
    "jasmine", "kernel", "lantern", "meadow", "nectar", "orbit", "pepper", "quartz", "river",
    "saffron", "timber", "umbra", "velvet", "willow", "xenon", "yonder", "zephyr", "anchor",
    "beacon", "cobalt", "dune", "echo", "fjord", "glacier", "horizon", "indigo", "juniper", "kelp",
    "lagoon", "marble", "nimbus", "onyx", "prairie", "quill", "ripple", "summit", "tundra",
    "upland", "vortex", "walnut", "yarrow",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn average_value_size(workload: Workload) -> usize {
        let mut generator = EventGenerator::new(7);
        let total: usize = (0..2000)
            .map(|_| generator.next(workload).value.expect("value").len())
            .sum();
        total / 2000
    }

    #[test]
    fn same_seed_same_messages() {
        let mut a = EventGenerator::new(42);
        let mut b = EventGenerator::new(42);
        for _ in 0..100 {
            assert_eq!(a.next(Workload::Mixed), b.next(Workload::Mixed));
        }
        let mut c = EventGenerator::new(43);
        assert_ne!(a.next(Workload::Mixed), c.next(Workload::Mixed));
    }

    #[test]
    fn values_look_like_json_objects() {
        let mut generator = EventGenerator::new(1);
        for _ in 0..500 {
            let message = generator.next(Workload::Mixed);
            let value = String::from_utf8(message.value.expect("value")).expect("UTF-8");
            assert!(value.starts_with('{') && value.ends_with('}'), "{value}");
            assert!(!value.contains('\\'));
            assert_eq!(value.matches('{').count(), value.matches('}').count());
            assert_eq!(value.matches('[').count(), value.matches(']').count());
            assert_eq!(value.matches('"').count() % 2, 0);
            assert!(message.key.is_some());
            assert_eq!(message.headers.len(), 2);
        }
    }

    #[test]
    fn shapes_have_the_documented_sizes() {
        let click = average_value_size(Workload::Only(Shape::Click));
        let order = average_value_size(Workload::Only(Shape::Order));
        let document = average_value_size(Workload::Only(Shape::Document));
        assert!((350..700).contains(&click), "click: {click} B");
        assert!((1_000..2_200).contains(&order), "order: {order} B");
        assert!(
            (25_000..45_000).contains(&document),
            "document: {document} B"
        );
    }

    #[test]
    fn the_pool_is_bounded() {
        let pool = pool(Workload::Only(Shape::Document), 3);
        let bytes: u64 = pool.iter().map(message_bytes).sum();
        assert!(bytes < POOL_MAX_BYTES + 64 * 1024);
        assert!(pool.len() < POOL_MAX_MESSAGES);
    }
}
