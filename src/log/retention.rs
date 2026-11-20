//! Rétention : quels segments supprimer pour respecter les limites de taille
//! et d'âge d'une partition. Logique pure, sans I/O.
//!
//! On ne supprime que des segments scellés entiers, du plus ancien au plus
//! récent, sans jamais toucher au segment actif : la partition reste une suite
//! continue d'offsets, qui commence simplement plus loin.

use std::time::{Duration, SystemTime};

/// Limites de rétention d'une partition. Par défaut, aucune : on garde tout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Retention {
    /// Taille maximale de la partition, segment actif compris, vérifiée à
    /// chaque ajout. Au-delà, les segments scellés les plus anciens sont
    /// supprimés. Le segment actif n'étant jamais supprimé, la partition ne
    /// dépasse cette taille que si le segment actif seul la dépasse.
    pub max_bytes: Option<u64>,
    /// Âge maximal d'un segment scellé, mesuré depuis sa dernière écriture.
    pub max_age: Option<Duration>,
}

impl Retention {
    pub(crate) fn is_unlimited(&self) -> bool {
        self.max_bytes.is_none() && self.max_age.is_none()
    }
}

/// Ce qu'il faut savoir d'un segment scellé pour décider de le supprimer.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SealedSegment {
    pub(crate) size: u64,
    /// Date de la dernière écriture dans le segment. `None` quand elle n'est
    /// pas utile (pas de limite d'âge) : on évite alors de la lire sur le disque.
    pub(crate) last_write: Option<SystemTime>,
}

/// Nombre de segments scellés à supprimer, en partant du plus ancien.
///
/// `sealed` liste les segments scellés du plus ancien au plus récent ;
/// `active_size` est la taille du segment actif.
pub(crate) fn segments_to_delete(
    sealed: &[SealedSegment],
    active_size: u64,
    retention: &Retention,
    now: SystemTime,
) -> usize {
    let too_old = |s: &SealedSegment| {
        retention.max_age.is_some_and(|max| {
            // Une date dans le futur (horloge reculée) n'est jamais trop vieille.
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

    /// Des segments de 100 octets, écrits il y a `ages` minutes (le plus ancien d'abord).
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
        // 3 × 100 + 50 = 350 octets.
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
