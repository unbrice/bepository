// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::device_id::DeviceId;
use crate::error::{BepError, Result};
use crate::proto::bep::{Counter, FileInfo, Vector};

/// Result of conflict resolution — who wins, who loses, and optionally where the loser is backed up.
pub struct ConflictResolution<'a> {
    pub winner: &'a FileInfo,
    pub loser: &'a FileInfo,
    /// Path at which to store the loser, or `None` to discard it without a backup.
    pub loser_path: Option<String>,
}

/// Policy for resolving concurrent edits (neither version vector dominates).
///
/// Injected into [`BepEngine`](crate::BepEngine) at construction time.
/// The engine calls this when [`StorageFolder::apply_update`](crate::StorageFolder::apply_update)
/// returns [`UpdateResult::Concurrent`](crate::UpdateResult::Concurrent).
pub trait ConflictResolver: Send + Sync + 'static {
    fn resolve<'a>(
        &self,
        local: &'a FileInfo,
        local_device: &DeviceId,
        remote: &'a FileInfo,
        remote_device: &DeviceId,
    ) -> Result<ConflictResolution<'a>>;
}

/// Sample conflict resolver for tests.
///
/// Strategy: (1) larger total version wins, (2) not-deleted wins, (3) larger device ID.
/// The loser is backed up at `<name>.sync-conflict`.
#[cfg(any(test, feature = "test-utils"))]
pub struct BackupResolver;

#[cfg(any(test, feature = "test-utils"))]
impl ConflictResolver for BackupResolver {
    fn resolve<'a>(
        &self,
        local: &'a FileInfo,
        local_device: &DeviceId,
        remote: &'a FileInfo,
        remote_device: &DeviceId,
    ) -> Result<ConflictResolution<'a>> {
        let (winner, loser) = resolve_conflict(local, local_device, remote, remote_device)?;
        Ok(ConflictResolution {
            winner,
            loser,
            loser_path: Some(format!("{}.sync-conflict", loser.name)),
        })
    }
}

/// Resolve a conflict between two concurrent FileInfo versions.
/// Returns (winner, loser) based on a sample tie-breaking strategy.
pub fn resolve_conflict<'a>(
    a: &'a FileInfo,
    a_device: &DeviceId,
    b: &'a FileInfo,
    b_device: &DeviceId,
) -> Result<(&'a FileInfo, &'a FileInfo)> {
    let a_total = a.version.as_ref().map(total).transpose()?.unwrap_or(0);
    let b_total = b.version.as_ref().map(total).transpose()?.unwrap_or(0);

    // Rule 1: larger total version wins
    if a_total != b_total {
        return Ok(if a_total > b_total { (a, b) } else { (b, a) });
    }

    // Rule 2: not-deleted wins
    if a.deleted != b.deleted {
        return Ok(if !a.deleted { (a, b) } else { (b, a) });
    }

    // Rule 3: lexicographic device ID comparison
    Ok(if a_device > b_device { (a, b) } else { (b, a) })
}

/// Result of comparing two version vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    /// A dominates B (A is strictly newer).
    Greater,
    /// B dominates A (B is strictly newer).
    Less,
    /// A and B are equal.
    Equal,
    /// Neither dominates — concurrent modifications (conflict).
    Concurrent,
}

/// Compare two version vectors using Syncthing semantics.
///
/// A dominates B if for every counter in B, A has >= that value,
/// and at least one counter in A is strictly greater.
///
/// Returns `Err` if either vector contains duplicate counter IDs, which is a
/// peer protocol violation.
pub fn compare(a: &Vector, b: &Vector) -> Result<Ordering> {
    let a_sorted = sorted_unique(a)?;
    let b_sorted = sorted_unique(b)?;

    let mut a_dominated = false; // a has some counter > b
    let mut b_dominated = false; // b has some counter > a

    let mut ai = 0;
    let mut bi = 0;
    while ai < a_sorted.len() && bi < b_sorted.len() {
        let ac = a_sorted[ai];
        let bc = b_sorted[bi];
        match ac.id.cmp(&bc.id) {
            std::cmp::Ordering::Equal => {
                if ac.value > bc.value {
                    a_dominated = true;
                } else if ac.value < bc.value {
                    b_dominated = true;
                }
                ai += 1;
                bi += 1;
            }
            std::cmp::Ordering::Less => {
                if ac.value > 0 {
                    a_dominated = true;
                }
                ai += 1;
            }
            std::cmp::Ordering::Greater => {
                if bc.value > 0 {
                    b_dominated = true;
                }
                bi += 1;
            }
        }
    }
    for ac in &a_sorted[ai..] {
        if ac.value > 0 {
            a_dominated = true;
        }
    }
    for bc in &b_sorted[bi..] {
        if bc.value > 0 {
            b_dominated = true;
        }
    }

    Ok(match (a_dominated, b_dominated) {
        (false, false) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (true, true) => Ordering::Concurrent,
    })
}

/// Sum all counters in a version vector (used for conflict tie-breaking).
///
/// Returns `Err` if the vector contains duplicate counter IDs.
pub fn total(v: &Vector) -> Result<u64> {
    sorted_unique(v)?;
    Ok(v.counters.iter().map(|c| c.value).sum())
}

/// Sort counters by ID and verify there are no duplicates.
///
/// Returns `Err(BepError::PeerBadMessage)` on the first duplicate found.
fn sorted_unique(v: &Vector) -> Result<Vec<&Counter>> {
    let mut sorted: Vec<&Counter> = v.counters.iter().collect();
    sorted.sort_by_key(|c| c.id);
    for w in sorted.windows(2) {
        if w[0].id == w[1].id {
            return Err(BepError::PeerBadMessage(format!(
                "duplicate counter id {} in version vector",
                w[0].id
            )));
        }
    }
    Ok(sorted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::bep::Counter;

    fn vec_from(pairs: &[(u64, u64)]) -> Vector {
        Vector {
            counters: pairs
                .iter()
                .map(|&(id, value)| Counter { id, value })
                .collect(),
        }
    }

    #[test]
    fn equal_versions() {
        let a = vec_from(&[(1, 5), (2, 3)]);
        let b = vec_from(&[(1, 5), (2, 3)]);
        assert_eq!(compare(&a, &b).unwrap(), Ordering::Equal);
    }

    #[test]
    fn both_empty() {
        let a = vec_from(&[]);
        let b = vec_from(&[]);
        assert_eq!(compare(&a, &b).unwrap(), Ordering::Equal);
    }

    #[test]
    fn a_dominates() {
        let a = vec_from(&[(1, 6), (2, 3)]);
        let b = vec_from(&[(1, 5), (2, 3)]);
        assert_eq!(compare(&a, &b).unwrap(), Ordering::Greater);
    }

    #[test]
    fn b_dominates() {
        let a = vec_from(&[(1, 5), (2, 3)]);
        let b = vec_from(&[(1, 5), (2, 4)]);
        assert_eq!(compare(&a, &b).unwrap(), Ordering::Less);
    }

    #[test]
    fn concurrent() {
        let a = vec_from(&[(1, 6), (2, 3)]);
        let b = vec_from(&[(1, 5), (2, 4)]);
        assert_eq!(compare(&a, &b).unwrap(), Ordering::Concurrent);
    }

    #[test]
    fn a_has_extra_counter() {
        let a = vec_from(&[(1, 5), (3, 1)]);
        let b = vec_from(&[(1, 5)]);
        assert_eq!(compare(&a, &b).unwrap(), Ordering::Greater);
    }

    #[test]
    fn b_has_extra_counter() {
        let a = vec_from(&[(1, 5)]);
        let b = vec_from(&[(1, 5), (3, 1)]);
        assert_eq!(compare(&a, &b).unwrap(), Ordering::Less);
    }

    #[test]
    fn concurrent_with_disjoint_counters() {
        let a = vec_from(&[(1, 5)]);
        let b = vec_from(&[(2, 3)]);
        assert_eq!(compare(&a, &b).unwrap(), Ordering::Concurrent);
    }

    #[test]
    fn total_sum() {
        let v = vec_from(&[(1, 5), (2, 3), (3, 7)]);
        assert_eq!(total(&v).unwrap(), 15);
    }

    #[test]
    fn duplicate_ids_in_a_rejected() {
        let a = vec_from(&[(1, 5), (1, 10)]);
        let b = vec_from(&[(1, 7)]);
        assert!(matches!(compare(&a, &b), Err(BepError::PeerBadMessage(_))));
    }

    #[test]
    fn duplicate_ids_in_b_rejected() {
        let a = vec_from(&[(1, 7)]);
        let b = vec_from(&[(1, 5), (1, 10)]);
        assert!(matches!(compare(&a, &b), Err(BepError::PeerBadMessage(_))));
    }

    #[test]
    fn total_rejects_duplicate_ids() {
        let v = vec_from(&[(1, 5), (1, 10)]);
        assert!(matches!(total(&v), Err(BepError::PeerBadMessage(_))));
    }
}
