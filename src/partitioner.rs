//! Choix de la partition d'un message : par sa clé (murmur2, comme Kafka),
//! sinon en round-robin. Logique pure, sans I/O.

/// Choisit la partition des messages d'un producteur.
#[derive(Debug)]
pub(crate) struct Partitioner {
    partitions: u32,
    next_round_robin: u32,
}

impl Partitioner {
    pub(crate) fn new(partitions: u32) -> Self {
        Self {
            partitions,
            next_round_robin: 0,
        }
    }

    /// Partition d'un message : déterminée par sa clé s'il en a une, ce qui
    /// garde l'ordre des messages de même clé ; sinon la suivante en round-robin.
    pub(crate) fn partition_for(&mut self, key: Option<&[u8]>) -> u32 {
        match key {
            Some(key) => partition_for_key(key, self.partitions),
            None => {
                let partition = self.next_round_robin;
                self.next_round_robin = (self.next_round_robin + 1) % self.partitions;
                partition
            }
        }
    }
}

/// La partition qu'un producteur Kafka choisirait pour `key` : une clé tombe
/// dans la même partition avec Mojay et avec Kafka.
pub(crate) fn partition_for_key(key: &[u8], partitions: u32) -> u32 {
    // `toPositive` de Kafka : on retire le bit de signe.
    (murmur2(key) as u32 & 0x7fff_ffff) % partitions
}

/// Murmur2 32 bits, à l'identique de `org.apache.kafka.common.utils.Utils.murmur2`.
pub(crate) fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let mut h = SEED ^ data.len() as u32;
    let (chunks, tail) = data.as_chunks::<4>();
    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    if !tail.is_empty() {
        for (i, &byte) in tail.iter().enumerate().rev() {
            h ^= u32::from(byte) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Les valeurs de référence du test `UtilsTest.testMurmur2` de Kafka.
    #[test]
    fn murmur2_matches_kafka() {
        let cases: [(&[u8], i32); 6] = [
            (b"21", -973932308),
            (b"foobar", -790332482),
            (b"a-little-bit-long-string", -985981536),
            (b"a-little-bit-longer-string", -1486304829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58897971,
            ),
            (b"abc", 479470107),
        ];
        for (key, expected) in cases {
            assert_eq!(
                murmur2(key),
                expected,
                "clé {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn same_key_always_goes_to_the_same_partition() {
        let mut partitioner = Partitioner::new(8);
        let first = partitioner.partition_for(Some(b"client-42"));
        for _ in 0..10 {
            assert_eq!(partitioner.partition_for(Some(b"client-42")), first);
        }
    }

    #[test]
    fn messages_without_key_are_spread_in_round_robin() {
        let mut partitioner = Partitioner::new(3);
        let partitions: Vec<_> = (0..7).map(|_| partitioner.partition_for(None)).collect();
        assert_eq!(partitions, vec![0, 1, 2, 0, 1, 2, 0]);
    }
}
