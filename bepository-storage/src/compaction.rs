// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Compaction GC filter for removing orphaned blocks and stale metadata during
//! SlateDB compaction.
//!
//! Before each compaction job a bloom filter is built from the block hashes
//! referenced by committed files (`n/`) and in-progress transfers (`in/`),
//! and a `peer_floor` is computed as `min(dx/<devid>.max_sequence)`.
//!
//! During compaction the filter:
//! - Drops/tombstones block data/ref/reverse-ref keys whose hash is NOT in the
//!   bloom filter.
//! - Prunes `s/<seq>` entries below `peer_floor` (no peer needs them for delta
//!   indexing).
//! - Prunes deleted `n/<name>` entries whose sequence is below `peer_floor`
//!   (all peers have already seen the deletion).

use std::num::NonZeroI64;
use std::sync::Arc;

use async_trait::async_trait;
use bepository_lock::Epoch;
use fastbloom::AtomicBloomFilter;
use prost::Message;
use slatedb::compactor::{
    CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId,
};
use slatedb::config::CompactorOptions;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, RowEntry, ValueDeletable,
};

use crate::store::{CompactionState, FolderStore};
use crate::store_keys;

/// Default expected number of unique block hashes per folder.
const BLOOM_CAPACITY: usize = 5_000_000;

/// False-positive rate for the compaction bloom filter.
const BLOOM_FP_RATE: f64 = 0.001;

/// Factory that creates a [`GcFilter`] for each compaction job.
///
/// Holds a late-binding reference to the [`FolderStore`] (via `OnceLock`)
/// because the supplier must be registered with `Db::builder` before the
/// `FolderStore` exists.
pub(crate) struct GcFilterSupplier {
    store_slot: Arc<std::sync::OnceLock<Arc<FolderStore>>>,
    gc: Arc<CompactionState>,
    epoch: Epoch,
}

impl GcFilterSupplier {
    pub fn new(
        store_slot: Arc<std::sync::OnceLock<Arc<FolderStore>>>,
        gc: Arc<CompactionState>,
        epoch: Epoch,
    ) -> Self {
        Self {
            store_slot,
            gc,
            epoch,
        }
    }
}

#[async_trait]
impl CompactionFilterSupplier for GcFilterSupplier {
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        let store = self
            .store_slot
            .get()
            .ok_or_else(|| {
                CompactionFilterError::CreationError("FolderStore not yet initialised".into())
            })?
            .clone();

        // Build atomic bloom filter to safely capture concurrent writes during scan.
        let bloom = Arc::new(
            AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(BLOOM_CAPACITY),
        );

        // Register with shared compaction state before scanning so writes go into our bloom filter.
        let job = self.gc.register(bloom.clone());

        // Committed files (n/ entries).
        let files = store
            .all_files()
            .await
            .map_err(|e| CompactionFilterError::CreationError(e.into()))?;
        for file in &files {
            for block in &file.blocks {
                if block.hash.len() == store_keys::HASH_LEN {
                    bloom.insert(&block.hash[..]);
                }
            }
        }

        // In-progress transfers (in/<epoch>/ entries).
        let inbox_files = store
            .inbox_files(self.epoch)
            .await
            .map_err(|e| CompactionFilterError::CreationError(e.into()))?;
        for file in &inbox_files {
            for block in &file.blocks {
                if block.hash.len() == store_keys::HASH_LEN {
                    bloom.insert(&block.hash[..]);
                }
            }
        }

        // Compute peer floor for sequence/tombstone pruning.
        let peer_floor = store
            .compute_peer_floor()
            .await
            .map_err(|e| CompactionFilterError::CreationError(e.into()))?;

        Ok(Box::new(GcFilter {
            job,
            epoch: self.epoch,
            peer_floor,
            is_bottom: context.is_dest_last_run,
            stats: FilterStats::default(),
        }))
    }
}

#[derive(Default)]
struct FilterStats {
    blocks_dropped: u64,
    refs_dropped: u64,
    reverse_refs_tombstoned: u64,
    inbox_tombstoned: u64,
    seqs_pruned: u64,
    tombstones_pruned: u64,
    kept: u64,
}

struct GcFilter {
    job: crate::store::CompactionJob,
    epoch: Epoch,
    peer_floor: Option<NonZeroI64>,
    is_bottom: bool,
    stats: FilterStats,
}

impl GcFilter {
    /// Decision for pruning stale metadata: Drop at the bottom sorted run
    /// (no older value to resurrect), Tombstone otherwise.
    fn prune_decision(&self) -> CompactionFilterDecision {
        if self.is_bottom {
            CompactionFilterDecision::Drop
        } else {
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        }
    }
}

#[async_trait]
impl CompactionFilter for GcFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        let key = &entry.key;

        // b/<dir>/<hash> — block data
        if let Some(hash) = store_keys::parse_block_data_key(key) {
            if !self.job.gc.known_live_contains(self.job.job_id, &hash) {
                self.stats.blocks_dropped += 1;
                return Ok(CompactionFilterDecision::Drop);
            }
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // b/<dir>/<hash>/ref — block reference
        if let Some(hash) = store_keys::parse_block_ref_key(key) {
            if !self.job.gc.known_live_contains(self.job.job_id, &hash) {
                self.stats.refs_dropped += 1;
                return Ok(CompactionFilterDecision::Drop);
            }
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // br/<hash>/<name> — reverse reference
        if let Some((hash, _name)) = store_keys::parse_block_reverse_key(key) {
            if !self.job.gc.known_live_contains(self.job.job_id, &hash) {
                self.stats.reverse_refs_tombstoned += 1;
                return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
            }
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // in/<epoch>/<name> — stale inbox entries
        if let Some((epoch, _name)) = store_keys::parse_inbox_key(key) {
            if epoch < self.epoch {
                self.stats.inbox_tombstoned += 1;
                return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
            }
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // s/<seq> — prune sequence mappings below peer_floor
        if let Some(seq) = store_keys::parse_seq_key(key) {
            if let Some(floor) = self.peer_floor
                && u64::try_from(floor.get()).is_ok_and(|f| seq < f)
            {
                self.stats.seqs_pruned += 1;
                return Ok(self.prune_decision());
            }
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // n/<name> — prune deleted-file tombstones below peer_floor
        if let Some(name) = store_keys::parse_file_key(key) {
            if let Some(floor) = self.peer_floor
                && let ValueDeletable::Value(val) = &entry.value
                && let Ok(file_wrapper) = crate::proto::storage::File::decode(val.as_ref())
            {
                if let Some(fi) = file_wrapper.file_info {
                    if fi.deleted && fi.sequence < floor.get() {
                        self.stats.tombstones_pruned += 1;
                        return Ok(self.prune_decision());
                    }
                } else {
                    tracing::warn!(
                        name = name,
                        "missing file_info in File entry during compaction"
                    );
                }
            }
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // Everything else (ix, dx/) — keep
        self.stats.kept += 1;
        Ok(CompactionFilterDecision::Keep)
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        tracing::info!(
            compaction_id = self.job.job_id,
            blocks_dropped = self.stats.blocks_dropped,
            refs_dropped = self.stats.refs_dropped,
            reverse_refs_tombstoned = self.stats.reverse_refs_tombstoned,
            inbox_tombstoned = self.stats.inbox_tombstoned,
            seqs_pruned = self.stats.seqs_pruned,
            tombstones_pruned = self.stats.tombstones_pruned,
            kept = self.stats.kept,
            "compaction GC complete"
        );
        Ok(())
    }
}

/// Scheduler supplier that forces a single full compaction.
///
/// When plugged into a [`CompactorBuilder`](slatedb::CompactorBuilder), the
/// compactor will merge *all* L0 SSTs and sorted runs into one sorted run on
/// the first poll, causing every key to pass through the registered
/// [`CompactionFilterSupplier`] (i.e. the GC filter). Subsequent polls
/// return no proposals because there is nothing left to compact.
pub(crate) struct FullCompactionSchedulerSupplier;

impl CompactionSchedulerSupplier for FullCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        _options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(FullCompactionScheduler)
    }
}

struct FullCompactionScheduler;

impl CompactionScheduler for FullCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let manifest = state.manifest();

        let mut sources: Vec<SourceId> = manifest
            .l0
            .iter()
            .map(|sst| SourceId::SstView(sst.id))
            .collect();

        for sr in &manifest.compacted {
            sources.push(SourceId::SortedRun(sr.id));
        }

        if sources.is_empty() {
            return vec![];
        }

        // If there are no compacted runs yet, we create the first one at slot 0
        // otherwise we re-use the ID of the first compacted run to match the behaviour of the default compactor
        let destination = manifest.compacted.iter().map(|sr| sr.id).min().unwrap_or(0);
        vec![CompactionSpec::new(sources, destination)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use slatedb::RowEntry;

    fn epoch(n: u64) -> Epoch {
        Epoch::new(n).unwrap()
    }

    fn make_row(key: &[u8]) -> RowEntry {
        RowEntry {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Value(Bytes::new()),
            seq: 0,
            create_ts: None,
            expire_ts: None,
        }
    }

    fn make_gc_filter(job: crate::store::CompactionJob, epoch: Epoch) -> GcFilter {
        make_gc_filter_with_floor(job, epoch, None, false)
    }

    fn make_gc_filter_with_floor(
        job: crate::store::CompactionJob,
        epoch: Epoch,
        peer_floor: Option<NonZeroI64>,
        is_bottom: bool,
    ) -> GcFilter {
        GcFilter {
            job,
            epoch,
            peer_floor,
            is_bottom,
            stats: FilterStats::default(),
        }
    }

    // --- CompactionState unit tests ---

    #[tokio::test]
    async fn compaction_state_no_active_compactions_is_safe() {
        let state = Arc::new(CompactionState::new());
        assert!(state.is_block_safe(&[0xAA; 32]));
    }

    #[tokio::test]
    async fn compaction_state_bloom_hit_is_safe() {
        let state = Arc::new(CompactionState::new());
        let hash = [0xBB; 32];

        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        bloom.insert(&hash);

        let job = state.register(bloom.clone());
        let id = job.job_id;
        assert!(state.is_block_safe(&hash));
        assert!(state.known_live_contains(id, &hash));

        // Unknown hash should NOT be safe.
        assert!(!state.is_block_safe(&[0xCC; 32]));

        drop(job);
        // Wait for the async unregister task spawned by Drop to complete.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        // After unregister, all hashes are safe again.
        assert!(state.is_block_safe(&[0xCC; 32]));
    }

    #[tokio::test]
    async fn compaction_state_written_since_is_safe() {
        let state = Arc::new(CompactionState::new());
        let hash = [0xDD; 32];

        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        // Empty bloom — hash is NOT in it.

        let _job = state.register(bloom.clone());
        assert!(!state.is_block_safe(&hash));

        // After recording a write, hash becomes safe.
        state.record_block_write(&hash);
        assert!(state.is_block_safe(&hash));
    }

    // --- GcFilter unit tests ---

    #[tokio::test]
    async fn gc_filter_drops_orphan_block_data() {
        let gc = Arc::new(CompactionState::new());
        let live_hash = [0xAA; 32];

        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        bloom.insert(&live_hash);

        let job = gc.register(bloom.clone());
        let mut filter = make_gc_filter(job, epoch(1));

        // Live block data — keep.
        let live_key = store_keys::block_data_key("docs", &live_hash);
        let decision = filter.filter(&make_row(&live_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        // Orphan block data — drop.
        let orphan_hash = [0xBB; 32];
        let orphan_key = store_keys::block_data_key("docs", &orphan_hash);
        let decision = filter.filter(&make_row(&orphan_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Drop));

        assert_eq!(filter.stats.kept, 1);
        assert_eq!(filter.stats.blocks_dropped, 1);
    }

    #[tokio::test]
    async fn gc_filter_drops_orphan_block_ref() {
        let gc = Arc::new(CompactionState::new());
        let live_hash = [0xAA; 32];

        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        bloom.insert(&live_hash);

        let job = gc.register(bloom.clone());
        let mut filter = make_gc_filter(job, epoch(1));

        // Live block ref — keep.
        let live_key = store_keys::block_ref_key("backup", &live_hash);
        let decision = filter.filter(&make_row(&live_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        // Orphan block ref — drop.
        let orphan_hash = [0xCC; 32];
        let orphan_key = store_keys::block_ref_key("backup", &orphan_hash);
        let decision = filter.filter(&make_row(&orphan_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Drop));

        assert_eq!(filter.stats.refs_dropped, 1);
    }

    #[tokio::test]
    async fn gc_filter_tombstones_orphan_reverse_ref() {
        let gc = Arc::new(CompactionState::new());
        let orphan_hash = [0xDD; 32];

        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        // Empty bloom.

        let job = gc.register(bloom.clone());
        let mut filter = make_gc_filter(job, epoch(1));

        let rev_key = store_keys::block_reverse_key(&orphan_hash, "old.txt");
        let decision = filter.filter(&make_row(&rev_key)).await.unwrap();
        assert!(matches!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        ));

        assert_eq!(filter.stats.reverse_refs_tombstoned, 1);
    }

    #[tokio::test]
    async fn gc_filter_tombstones_stale_inbox() {
        let gc = Arc::new(CompactionState::new());
        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));

        let job = gc.register(bloom.clone());
        let current_epoch = epoch(5);
        let mut filter = make_gc_filter(job, current_epoch);

        // Old epoch inbox entry — tombstone.
        let old_key = store_keys::inbox_key(epoch(3), "stale.txt");
        let decision = filter.filter(&make_row(&old_key)).await.unwrap();
        assert!(matches!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        ));

        // Current epoch inbox entry — keep.
        let current_key = store_keys::inbox_key(epoch(5), "active.txt");
        let decision = filter.filter(&make_row(&current_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        assert_eq!(filter.stats.inbox_tombstoned, 1);
        assert_eq!(filter.stats.kept, 1);
    }

    #[tokio::test]
    async fn gc_filter_keeps_non_block_keys() {
        let gc = Arc::new(CompactionState::new());
        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));

        let job = gc.register(bloom.clone());
        let mut filter = make_gc_filter(job, epoch(1));

        // File index key.
        let file_key = store_keys::file_key("hello.txt");
        let decision = filter.filter(&make_row(&file_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        // Sequence key.
        let seq_key = store_keys::seq_key(42).unwrap();
        let decision = filter.filter(&make_row(&seq_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        // Index metadata.
        let decision = filter.filter(&make_row(store_keys::IX_KEY)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        assert_eq!(filter.stats.kept, 3);
    }

    // --- Sequence and tombstone pruning tests ---

    fn make_row_with_value(key: &[u8], value: &[u8]) -> RowEntry {
        RowEntry {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Value(Bytes::copy_from_slice(value)),
            seq: 0,
            create_ts: None,
            expire_ts: None,
        }
    }

    fn make_deleted_file(name: &str, sequence: i64) -> Vec<u8> {
        let fi = crate::proto::storage::FileInfo {
            name: name.to_string(),
            sequence,
            deleted: true,
            ..Default::default()
        };
        let file = crate::proto::storage::File {
            file_info: Some(fi),
        };
        file.encode_to_vec()
    }

    fn make_live_file(name: &str, sequence: i64) -> Vec<u8> {
        let fi = crate::proto::storage::FileInfo {
            name: name.to_string(),
            sequence,
            deleted: false,
            ..Default::default()
        };
        let file = crate::proto::storage::File {
            file_info: Some(fi),
        };
        file.encode_to_vec()
    }

    #[tokio::test]
    async fn gc_filter_prunes_old_sequence_entry() {
        let gc = Arc::new(CompactionState::new());
        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom.clone());

        // peer_floor=10, not bottom level → Tombstone
        let mut filter = make_gc_filter_with_floor(job, epoch(1), NonZeroI64::new(10), false);

        let key = store_keys::seq_key(5).unwrap(); // seq 5 < peer_floor 10
        let decision = filter.filter(&make_row(&key)).await.unwrap();
        assert!(matches!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        ));
        assert_eq!(filter.stats.seqs_pruned, 1);
    }

    #[tokio::test]
    async fn gc_filter_keeps_recent_sequence_entry() {
        let gc = Arc::new(CompactionState::new());
        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom.clone());

        let mut filter = make_gc_filter_with_floor(job, epoch(1), NonZeroI64::new(10), false);

        let key = store_keys::seq_key(15).unwrap(); // seq 15 >= peer_floor 10
        let decision = filter.filter(&make_row(&key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));
        assert_eq!(filter.stats.kept, 1);
    }

    #[tokio::test]
    async fn gc_filter_prunes_deleted_file_tombstone() {
        let gc = Arc::new(CompactionState::new());
        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom.clone());

        let mut filter = make_gc_filter_with_floor(job, epoch(1), NonZeroI64::new(10), false);

        let key = store_keys::file_key("old.txt");
        let value = make_deleted_file("old.txt", 5); // deleted, seq 5 < peer_floor 10
        let decision = filter
            .filter(&make_row_with_value(&key, &value))
            .await
            .unwrap();
        assert!(matches!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        ));
        assert_eq!(filter.stats.tombstones_pruned, 1);
    }

    #[tokio::test]
    async fn gc_filter_keeps_live_file() {
        let gc = Arc::new(CompactionState::new());
        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom.clone());

        let mut filter = make_gc_filter_with_floor(job, epoch(1), NonZeroI64::new(10), false);

        let key = store_keys::file_key("live.txt");
        let value = make_live_file("live.txt", 5); // not deleted, seq 5 < peer_floor
        let decision = filter
            .filter(&make_row_with_value(&key, &value))
            .await
            .unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));
        assert_eq!(filter.stats.kept, 1);
    }

    #[tokio::test]
    async fn gc_filter_keeps_recent_deleted_file() {
        let gc = Arc::new(CompactionState::new());
        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom.clone());

        let mut filter = make_gc_filter_with_floor(job, epoch(1), NonZeroI64::new(10), false);

        let key = store_keys::file_key("recent.txt");
        let value = make_deleted_file("recent.txt", 15); // deleted, seq 15 >= peer_floor
        let decision = filter
            .filter(&make_row_with_value(&key, &value))
            .await
            .unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));
        assert_eq!(filter.stats.kept, 1);
    }

    #[tokio::test]
    async fn gc_filter_no_pruning_without_peers() {
        let gc = Arc::new(CompactionState::new());
        let bloom = Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom.clone());

        // No peers (peer_floor=None) — nothing should be pruned
        let mut filter = make_gc_filter_with_floor(job, epoch(1), None, false);

        // Sequence entry — keep
        let seq_key = store_keys::seq_key(1).unwrap();
        let decision = filter.filter(&make_row(&seq_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        // Deleted file — keep
        let file_key = store_keys::file_key("old.txt");
        let value = make_deleted_file("old.txt", 1);
        let decision = filter
            .filter(&make_row_with_value(&file_key, &value))
            .await
            .unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        assert_eq!(filter.stats.kept, 2);
        assert_eq!(filter.stats.seqs_pruned, 0);
        assert_eq!(filter.stats.tombstones_pruned, 0);
    }
}
