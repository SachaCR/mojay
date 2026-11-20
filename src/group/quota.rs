//! Distribution of partitions among the live members of a group.
//! Pure logic, with no I/O.
//!
//! With P partitions and N members, the member of rank r (0 = the oldest)
//! must own ⌊P / N⌋ of them, plus one if r < P mod N. The quotas add up to
//! exactly P, and two quotas differ by at most one.

/// Number of partitions the member of rank `rank` must own.
pub(super) fn quota(partitions: usize, members: usize, rank: usize) -> usize {
    if members == 0 {
        return 0;
    }
    partitions / members + usize::from(rank < partitions % members)
}

/// Index of the first partition the member of rank `rank` tries to
/// take: each member starts from a different place, to limit
/// collisions when several are looking for free partitions at the same time.
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
        assert_eq!(quota(2, 5, 4), 0, "more members than partitions");
    }

    #[test]
    fn members_start_their_search_at_different_places() {
        let starts: Vec<_> = (0..4).map(|r| claim_start(8, 4, r)).collect();
        assert_eq!(starts, vec![0, 2, 4, 6]);
        assert_eq!(claim_start(0, 3, 1), 0);
    }
}
