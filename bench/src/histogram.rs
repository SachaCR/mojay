//! A fixed-size latency histogram: constant memory whatever the number of
//! samples (512 counters), with percentiles accurate to about 12%.
//!
//! Each power of two (in nanoseconds) is split into 8 linear sub-buckets.

use std::time::Duration;

const SUB_BUCKETS: usize = 8;
const POWERS: usize = 64;

#[derive(Debug, Clone)]
pub struct Histogram {
    counts: Box<[u64; POWERS * SUB_BUCKETS]>,
    total: u64,
    max: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            counts: Box::new([0; POWERS * SUB_BUCKETS]),
            total: 0,
            max: 0,
        }
    }
}

impl Histogram {
    pub fn record(&mut self, value: Duration) {
        let nanos = u64::try_from(value.as_nanos()).unwrap_or(u64::MAX).max(1);
        self.counts[bucket(nanos)] += 1;
        self.total += 1;
        self.max = self.max.max(nanos);
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (mine, theirs) in self.counts.iter_mut().zip(other.counts.iter()) {
            *mine += theirs;
        }
        self.total += other.total;
        self.max = self.max.max(other.max);
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// The value below which `fraction` of the samples fall (upper bound of its bucket).
    pub fn percentile(&self, fraction: f64) -> Duration {
        let wanted = ((self.total as f64) * fraction).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (index, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= wanted {
                return Duration::from_nanos(upper_bound(index).min(self.max));
            }
        }
        Duration::from_nanos(self.max)
    }

    /// The largest sample, exactly.
    pub fn max(&self) -> Duration {
        Duration::from_nanos(self.max)
    }

    /// One line of text, so that a child process can send its histogram to
    /// the parent: `max` then the non-empty buckets as `index:count`.
    pub fn encode(&self) -> String {
        let mut line = self.max.to_string();
        for (index, count) in self.counts.iter().enumerate() {
            if *count > 0 {
                line.push_str(&format!(" {index}:{count}"));
            }
        }
        line
    }

    /// The reverse of [`encode`](Self::encode); `None` if the line is malformed.
    pub fn decode(line: &str) -> Option<Self> {
        let mut words = line.split_whitespace();
        let mut histogram = Self {
            max: words.next()?.parse().ok()?,
            ..Self::default()
        };
        for word in words {
            let (index, count) = word.split_once(':')?;
            let count: u64 = count.parse().ok()?;
            *histogram.counts.get_mut(index.parse::<usize>().ok()?)? += count;
            histogram.total += count;
        }
        Some(histogram)
    }
}

fn bucket(nanos: u64) -> usize {
    let power = 63 - nanos.leading_zeros() as usize;
    // Position inside [2^power, 2^(power+1)), in eighths.
    let sub = if power >= 3 {
        ((nanos >> (power - 3)) & 0b111) as usize
    } else {
        0
    };
    power * SUB_BUCKETS + sub
}

fn upper_bound(index: usize) -> u64 {
    let power = index / SUB_BUCKETS;
    let sub = (index % SUB_BUCKETS) as u64;
    let base = 1u64 << power;
    if power >= 3 {
        base + (sub + 1) * (base / 8)
    } else {
        base * 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_within_an_eighth() {
        let mut histogram = Histogram::default();
        for micros in 1..=1000 {
            histogram.record(Duration::from_micros(micros));
        }
        for (fraction, expected) in [(0.5, 500.0), (0.99, 990.0)] {
            let got = histogram.percentile(fraction).as_secs_f64() * 1e6;
            assert!(
                (got - expected).abs() / expected < 0.13,
                "p{fraction}: {got} µs"
            );
        }
        assert_eq!(histogram.percentile(1.0), Duration::from_micros(1000));
    }

    #[test]
    fn merge_adds_counts() {
        let mut a = Histogram::default();
        let mut b = Histogram::default();
        a.record(Duration::from_millis(1));
        b.record(Duration::from_millis(3));
        a.merge(&b);
        assert_eq!(a.percentile(1.0), Duration::from_millis(3));
    }

    #[test]
    fn encode_then_decode_gives_the_same_percentiles() {
        let mut histogram = Histogram::default();
        for micros in [3, 50, 50, 700, 12_000] {
            histogram.record(Duration::from_micros(micros));
        }
        let decoded = Histogram::decode(&histogram.encode()).expect("valid line");
        for fraction in [0.2, 0.5, 0.99, 1.0] {
            assert_eq!(decoded.percentile(fraction), histogram.percentile(fraction));
        }
        assert_eq!(decoded.max(), Duration::from_micros(12_000));
        assert!(Histogram::decode("12 9999:1").is_none());
        assert!(Histogram::decode("").is_none());
    }
}
