//! Les événements de la démo : la vie de commandes d'une petite boutique.
//! Logique pure : un générateur pseudo-aléatoire déterministe, sans dépendance.

use serde::Serialize;

const KINDS: [&str; 4] = [
    "order_created",
    "order_paid",
    "order_shipped",
    "order_cancelled",
];
const CUSTOMERS: u64 = 50;

/// Un événement, tel qu'il est publié en JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Event {
    pub id: u64,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub customer: String,
    pub amount_cents: u64,
    /// Millisecondes depuis l'epoch.
    pub emitted_at: u64,
}

impl Event {
    pub fn to_json(&self) -> Vec<u8> {
        // Une structure de chaînes et d'entiers se sérialise toujours.
        serde_json::to_vec(self).expect("un Event est toujours sérialisable")
    }
}

/// Fabrique des événements variés mais reproductibles à graine égale.
pub struct EventGenerator {
    state: u64,
    next_id: u64,
}

impl EventGenerator {
    pub fn new(seed: u64) -> Self {
        Self {
            // xorshift ne doit jamais partir de zéro.
            state: seed | 1,
            next_id: 0,
        }
    }

    pub fn next_event(&mut self, emitted_at: u64) -> Event {
        let id = self.next_id;
        self.next_id += 1;
        Event {
            id,
            kind: KINDS[(self.random() % KINDS.len() as u64) as usize],
            customer: format!("client-{}", self.random() % CUSTOMERS + 1),
            amount_cents: 500 + self.random() % 50_000,
            emitted_at,
        }
    }

    /// xorshift64* : largement assez pour varier des données de démo.
    fn random(&mut self) -> u64 {
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        self.state.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_numbered_and_reproducible() {
        let mut a = EventGenerator::new(42);
        let mut b = EventGenerator::new(42);
        for id in 0..100 {
            let event = a.next_event(1_000);
            assert_eq!(event.id, id);
            assert_eq!(event, b.next_event(1_000));
            assert!(KINDS.contains(&event.kind));
            assert!((500..50_500).contains(&event.amount_cents));
        }
    }

    #[test]
    fn json_has_the_expected_fields() {
        let event = EventGenerator::new(7).next_event(1_700_000_000_000);
        let json: serde_json::Value = serde_json::from_slice(&event.to_json()).unwrap();
        assert_eq!(json["id"], 0);
        assert_eq!(json["type"], event.kind);
        assert_eq!(json["customer"], event.customer);
        assert_eq!(json["emitted_at"], 1_700_000_000_000u64);
    }
}
