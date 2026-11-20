//! Répartition des partitions entre les membres vivants d'un groupe.
//! Logique pure, sans I/O.
//!
//! Avec P partitions et N membres, le membre de rang r (0 = le plus ancien)
//! doit en posséder ⌊P / N⌋, plus une si r < P mod N. La somme des quotas vaut
//! exactement P, et deux quotas diffèrent d'au plus un.

/// Nombre de partitions que doit posséder le membre de rang `rank`.
pub(super) fn quota(partitions: usize, members: usize, rank: usize) -> usize {
    if members == 0 {
        return 0;
    }
    partitions / members + usize::from(rank < partitions % members)
}

/// Indice de la première partition que le membre de rang `rank` tente de
/// prendre : chaque membre part d'un endroit différent, pour limiter les
/// collisions quand plusieurs cherchent des partitions libres en même temps.
pub(super) fn claim_start(partitions: usize, members: usize, rank: usize) -> usize {
    if members == 0 || partitions == 0 {
        return 0;
    }
    rank * partitions / members % partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotas_add_up_to_the_partition_count_and_stay_balanced() {
        for partitions in 0..40 {
            for members in 1..12 {
                let quotas: Vec<_> = (0..members)
                    .map(|rank| quota(partitions, members, rank))
                    .collect();
                assert_eq!(quotas.iter().sum::<usize>(), partitions);
                let (min, max) = (quotas.iter().min(), quotas.iter().max());
                assert!(max.unwrap() - min.unwrap() <= 1, "{quotas:?}");
            }
        }
    }

    #[test]
    fn older_members_take_the_extra_partitions() {
        assert_eq!(
            (0..3).map(|r| quota(4, 3, r)).collect::<Vec<_>>(),
            vec![2, 1, 1]
        );
        assert_eq!(quota(2, 5, 4), 0, "plus de membres que de partitions");
    }

    #[test]
    fn members_start_their_search_at_different_places() {
        let starts: Vec<_> = (0..4).map(|r| claim_start(8, 4, r)).collect();
        assert_eq!(starts, vec![0, 2, 4, 6]);
        assert_eq!(claim_start(0, 3, 1), 0);
    }
}
