// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Compaction GC statistics: per-row counters, per-2-byte-prefix value-size
//! histograms, and the tracing emission at end-of-compaction.
//!
//! Used offline to decide whether a prefix should be split into its own
//! segment (churn ratio) or get out-of-band storage for its large values
//! (size distribution).

use std::collections::HashMap;

use slatedb::{RowEntry, ValueDeletable};

#[derive(Default)]
pub(crate) struct FilterStats {
    pub(crate) blocks_dropped: u64,
    pub(crate) blocks_kept: u64,
    pub(crate) refs_dropped: u64,
    pub(crate) refs_kept: u64,
    pub(crate) reverse_refs_tombstoned: u64,
    pub(crate) reverse_refs_kept: u64,
    pub(crate) inbox_tombstoned: u64,
    pub(crate) inbox_kept: u64,
    pub(crate) seqs_pruned: u64,
    pub(crate) seqs_kept: u64,
    pub(crate) tombstones_pruned: u64,
    pub(crate) files_kept: u64,
    pub(crate) metadata_kept: u64,
    pub(crate) kept: u64,
    prefix_stats: HashMap<[u8; 2], PrefixStats>,
}

impl FilterStats {
    /// Per-row observation: bump the prefix's `seen`/`bytes_*` and bucket.
    /// Call once at the top of the filter for every row.
    pub(crate) fn record_seen(&mut self, key: &[u8], entry: &RowEntry) {
        self.prefix_stats
            .entry(prefix_for(key))
            .or_default()
            .record_seen(entry);
    }

    /// Per-row drop: bump the prefix's `dropped` counter. Call at every site
    /// where the filter returns Drop or Modify(Tombstone).
    pub(crate) fn record_dropped(&mut self, key: &[u8]) {
        self.prefix_stats
            .entry(prefix_for(key))
            .or_default()
            .dropped += 1;
    }

    /// Emit `compaction GC complete` plus one `compaction GC prefix-stats`
    /// event per non-empty prefix, sorted by `seen` desc so the dominant
    /// contributors come first when scanning the log.
    pub(crate) fn log_completion(&self, folder_sk: &str, compaction_id: u64, is_bottom: bool) {
        tracing::info!(
            folder_id = %folder_sk,
            compaction_id,
            is_bottom,
            blocks_dropped = self.blocks_dropped,
            blocks_kept = self.blocks_kept,
            refs_dropped = self.refs_dropped,
            refs_kept = self.refs_kept,
            reverse_refs_tombstoned = self.reverse_refs_tombstoned,
            reverse_refs_kept = self.reverse_refs_kept,
            inbox_tombstoned = self.inbox_tombstoned,
            inbox_kept = self.inbox_kept,
            seqs_pruned = self.seqs_pruned,
            seqs_kept = self.seqs_kept,
            tombstones_pruned = self.tombstones_pruned,
            files_kept = self.files_kept,
            metadata_kept = self.metadata_kept,
            kept = self.kept,
            "compaction GC complete"
        );

        let mut entries: Vec<(&[u8; 2], &PrefixStats)> = self.prefix_stats.iter().collect();
        entries.sort_by_key(|(_, s)| std::cmp::Reverse(s.seen));
        for (prefix, s) in entries {
            let h = &s.bytes_hist;
            tracing::info!(
                folder_id = %folder_sk,
                compaction_id,
                prefix = %String::from_utf8_lossy(prefix),
                seen = s.seen,
                tombstones_seen = s.tombstones_seen,
                dropped = s.dropped,
                bytes_total = s.bytes_total,
                bytes_max = s.bytes_max,
                bucket_0 = h[0],
                bucket_16 = h[1],
                bucket_64 = h[2],
                bucket_256 = h[3],
                bucket_1k = h[4],
                bucket_4k = h[5],
                bucket_16k = h[6],
                bucket_64k = h[7],
                bucket_256k = h[8],
                bucket_1M = h[9],
                bucket_4M = h[10],
                bucket_16M = h[11],
                bucket_64M = h[12],
                bucket_128M = h[13],
                "compaction GC prefix-stats"
            );
        }
    }
}

fn prefix_for(key: &[u8]) -> [u8; 2] {
    match key.len() {
        0 => [0, 0],
        1 => [key[0], 0],
        _ => [key[0], key[1]],
    }
}

/// 14 ×4 size buckets indexed by the histogram array; field names in
/// `log_completion` are `bucket_<min>` where `<min>` is the bucket's
/// inclusive lower bound (0, 16, 64, …, 64M, 128M). The step from 64M to
/// 128M is ×2 rather than ×4 because 128 MiB is the chosen ceiling and
/// doesn't sit on the ×4 ladder; the `bucket_64M` bucket therefore covers half the
/// dynamic range of its neighbours — fine for the "is this prefix large
/// enough to deserve OOB?" question but worth knowing when reading the
/// histogram.
fn bytes_bucket_index(len: u64) -> usize {
    if len < 16 {
        return 0;
    }
    if len >= 1 << 27 {
        return 13;
    }
    let bit = 63 - len.leading_zeros() as usize;
    ((bit - 4) >> 1) + 1
}

#[derive(Default)]
struct PrefixStats {
    seen: u64,
    tombstones_seen: u64,
    dropped: u64,
    bytes_total: u64,
    bytes_max: u64,
    bytes_hist: [u64; 14],
}

impl PrefixStats {
    fn record_seen(&mut self, entry: &RowEntry) {
        self.seen += 1;
        let len = match &entry.value {
            ValueDeletable::Value(b) | ValueDeletable::Merge(b) => b.len() as u64,
            ValueDeletable::Tombstone => {
                self.tombstones_seen += 1;
                0
            }
        };
        self.bytes_total = self.bytes_total.saturating_add(len);
        if len > self.bytes_max {
            self.bytes_max = len;
        }
        self.bytes_hist[bytes_bucket_index(len)] += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_index_boundaries() {
        assert_eq!(bytes_bucket_index(0), 0);
        assert_eq!(bytes_bucket_index(15), 0);
        assert_eq!(bytes_bucket_index(16), 1);
        assert_eq!(bytes_bucket_index(63), 1);
        assert_eq!(bytes_bucket_index(64), 2);
        assert_eq!(bytes_bucket_index(255), 2);
        assert_eq!(bytes_bucket_index(256), 3);
        assert_eq!(bytes_bucket_index(1024), 4);
        assert_eq!(bytes_bucket_index(4096), 5);
        assert_eq!(bytes_bucket_index(65_536), 7);
        assert_eq!(bytes_bucket_index(1 << 26), 12);
        assert_eq!(bytes_bucket_index((1 << 27) - 1), 12);
        assert_eq!(bytes_bucket_index(1 << 27), 13);
        assert_eq!(bytes_bucket_index(u64::MAX), 13);
    }
}
