//! Retention: which segments to delete to respect a partition's size
//! and age limits. Pure logic, with no I/O.
//!
//! Only whole sealed segments are deleted, from oldest to most
//! recent, never touching the active segment: the partition stays a continuous
//! sequence of offsets, which simply starts further along.

use std::time::{Duration, SystemTime};

/// Retention limits of a partition. None by default: everything is kept.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Retention {
    /// Maximum size of the partition, active segment included, checked on
    /// each append. Beyond it, the oldest sealed segments are
    /// deleted. Since the active segment is never deleted, the partition only
    /// exceeds this size if the active segment alone exceeds it.
    pub max_bytes: Option<u64>,
    /// Maximum age of a sealed segment, measured from its last write.
    pub max_age: Option<Duration>,
}

impl Retention {
    pub(crate) fn is_unlimited(&self) -> bool {
        self.max_bytes.is_none() && self.max_age.is_none()
    }
}

/// What needs to be known about a sealed segment to decide whether to delete it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SealedSegment {
    pub(crate) size: u64,
    /// Time of the last write to the segment. `None` when it is not
    /// needed (no age limit): this avoids reading it from disk.
    pub(crate) last_write: Option<SystemTime>,
}

/// Number of sealed segments to delete, starting from the oldest.
///
/// `sealed` lists the sealed segments from oldest to most recent;
/// `active_size` is the size of the active segment.
pub(crate) fn segments_to_delete(
    sealed: &[SealedSegment],
    active_size: u64,
    retention: &Retention,
    now: SystemTime,
) -> usize {
    let too_old = |s: &SealedSegment| {
        retention.max_age.is_some_and(|max| {
            // A date in the future (clock moved backwards) is never too old.
            s.last_write
                .is_some_and(|written| now.duration_since(written).is_ok_and(|age| age > max))
        })
    };
    let mut total: u64 = sealed.iter().map(|s| s.size).sum::<u64>() + active_size;
    let mut count = 0;
    for segment in sealed {
        let too_big = retention.max_bytes.is_some_and(|max| total > max);
        if !too_big && !too_old(segment) {
            break;
        }
        total -= segment.size;
        count += 1;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: Duration = Duration::from_secs(60);

    fn now() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000)
    }

    /// 100-byte segments, written `ages` minutes ago (oldest first).
    fn sealed(ages: &[u64]) -> Vec<SealedSegment> {
        ages.iter()
            .map(|&minutes| SealedSegment {
                size: 100,
                last_write: Some(now() - MINUTE * minutes as u32),
            })
            .collect()
    }

    #[test]
    fn nothing_is_deleted_without_limits() {
        let segments = sealed(&[999, 500, 1]);
        assert_eq!(
            segments_to_delete(&segments, 50, &Retention::default(), now()),
            0
        );
    }

    #[test]
    fn oldest_segments_go_until_the_size_fits() {
        let limit = |max_bytes| Retention {
            max_bytes: Some(max_bytes),
            max_age: None,
        };
        // 3 × 100 + 50 = 350 bytes.
        let segments = sealed(&[3, 2, 1]);
        assert_eq!(segments_to_delete(&segments, 50, &limit(350), now()), 0);
        assert_eq!(segments_to_delete(&segments, 50, &limit(250), now()), 1);
        assert_eq!(segments_to_delete(&segments, 50, &limit(249), now()), 2);
    }

    #[test]
    fn active_segment_is_never_counted_as_deletable() {
        let retention = Retention {
            max_bytes: Some(10),
            max_age: None,
        };
        assert_eq!(
            segments_to_delete(&sealed(&[3, 2]), 500, &retention, now()),
            2
        );
        assert_eq!(segments_to_delete(&[], 500, &retention, now()), 0);
    }

    #[test]
    fn segments_older_than_max_age_go_but_not_beyond_the_first_young_one() {
        let retention = Retention {
            max_bytes: None,
            max_age: Some(MINUTE * 10),
        };
        assert_eq!(
            segments_to_delete(&sealed(&[30, 20, 5, 15]), 0, &retention, now()),
            2
        );
    }

    #[test]
    fn clock_going_backwards_deletes_nothing() {
        let retention = Retention {
            max_bytes: None,
            max_age: Some(MINUTE),
        };
        let future = vec![SealedSegment {
            size: 100,
            last_write: Some(now() + MINUTE * 60),
        }];
        assert_eq!(segments_to_delete(&future, 0, &retention, now()), 0);
    }
}
