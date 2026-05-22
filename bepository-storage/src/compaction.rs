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
use slatedb::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
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

/// Scan `prefix` against `snapshot`, decode each entry, and insert every block
/// hash and blockseq from the contained `FileInfo` into `known_live_hashes` and `known_live_seqs`.
/// A wrong-length hash or invalid blockseq is treated as corruption: the compaction aborts.
async fn index_blocks_under_prefix<F>(
    snapshot: &slatedb::DbSnapshot,
    prefix: &[u8],
    known_live_hashes: &AtomicBloomFilter,
    known_live_seqs: &AtomicBloomFilter,
    extract: F,
) -> Result<(), CompactionFilterError>
where
    F: Fn(bytes::Bytes) -> Result<Option<crate::proto::storage::FileInfo>, prost::DecodeError>,
{
    let mut iter = snapshot
        .scan_prefix(prefix)
        .await
        .map_err(|e| CompactionFilterError::CreationError(crate::store::slate_err(e).into()))?;
    while let Some(kv) = iter
        .next()
        .await
        .map_err(|e| CompactionFilterError::CreationError(crate::store::slate_err(e).into()))?
    {
        let fi = match extract(kv.value)
            .map_err(|e| CompactionFilterError::CreationError(format!("decode: {e}").into()))?
        {
            Some(fi) => fi,
            None => continue,
        };
        for block in &fi.blocks {
            if let Some(seq) = block.blockseq {
                let hash: &[u8; store_keys::HASH_LEN] =
                    block.hash.as_slice().try_into().map_err(|_| {
                        CompactionFilterError::CreationError(
                            format!(
                                "invalid hash length {} in file {}",
                                block.hash.len(),
                                fi.name
                            )
                            .into(),
                        )
                    })?;
                known_live_hashes.insert(hash);
                store_keys::validate_block_seq(seq).map_err(|e| {
                    CompactionFilterError::CreationError(
                        format!("invalid blockseq in file {}: {e}", fi.name).into(),
                    )
                })?;
                known_live_seqs.insert(&seq);
            }
        }
    }
    Ok(())
}

/// Factory that creates a [`GcFilter`] for each compaction job.
///
/// Holds a late-binding reference to the [`FolderStore`] (via `OnceLock`)
/// because the supplier must be registered with `Db::builder` before the
/// `FolderStore` exists.
pub(crate) struct GcFilterSupplier {
    folder_sk: String,
    store_slot: Arc<std::sync::OnceLock<Arc<FolderStore>>>,
    gc: Arc<CompactionState>,
    epoch: Epoch,
}

impl GcFilterSupplier {
    pub fn new(
        folder_sk: String,
        store_slot: Arc<std::sync::OnceLock<Arc<FolderStore>>>,
        gc: Arc<CompactionState>,
        epoch: Epoch,
    ) -> Self {
        Self {
            folder_sk,
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

        // Build atomic bloom filters to safely capture concurrent writes during scan.
        let known_live_hashes = Arc::new(
            AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(BLOOM_CAPACITY),
        );
        let known_live_seqs = Arc::new(
            AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(BLOOM_CAPACITY),
        );

        // Register with shared compaction state before scanning so writes go into our bloom filters.
        let job = self
            .gc
            .register(known_live_hashes.clone(), known_live_seqs.clone());

        // Single snapshot covers both scans: `complete_file` atomically moves
        // an entry from `in/<epoch>/<name>` to `n/<name>`, so scanning the two
        // prefixes against the same point-in-time view guarantees every live
        // file appears in exactly one of them. Independent scans would each
        // take their own snapshot and could miss a file mid-transition.
        let snapshot =
            store.db.snapshot().await.map_err(|e| {
                CompactionFilterError::CreationError(crate::store::slate_err(e).into())
            })?;

        let inbox_prefix = store_keys::inbox_key(self.epoch, "");
        index_blocks_under_prefix(
            &snapshot,
            &inbox_prefix,
            &known_live_hashes,
            &known_live_seqs,
            |bytes| crate::proto::storage::Inbox::decode(bytes).map(|i| i.file_info),
        )
        .await?;
        index_blocks_under_prefix(
            &snapshot,
            store_keys::FILE_PREFIX,
            &known_live_hashes,
            &known_live_seqs,
            |bytes| crate::proto::storage::File::decode(bytes).map(|f| f.file_info),
        )
        .await?;

        // Compute peer floor for sequence/tombstone pruning.
        let peer_floor = store
            .compute_peer_floor()
            .await
            .map_err(|e| CompactionFilterError::CreationError(e.into()))?;

        Ok(Box::new(GcFilter {
            folder_sk: self.folder_sk.clone(),
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
    blocks_kept: u64,
    refs_dropped: u64,
    refs_kept: u64,
    reverse_refs_tombstoned: u64,
    reverse_refs_kept: u64,
    inbox_tombstoned: u64,
    inbox_kept: u64,
    seqs_pruned: u64,
    seqs_kept: u64,
    tombstones_pruned: u64,
    files_kept: u64,
    metadata_kept: u64,
    kept: u64,
}

struct GcFilter {
    folder_sk: String,
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

        // bd/<seq> — block data
        if key.starts_with(store_keys::BLOCK_DATA_PREFIX) {
            let seq = store_keys::parse_block_data_seq_key(key).ok_or_else(|| {
                CompactionFilterError::FilterError(
                    format!("corrupted bd/ key width/value: {}", hex::encode(key)).into(),
                )
            })?;
            if !self.job.gc.known_live_seq_contains(self.job.job_id, seq) {
                self.stats.blocks_dropped += 1;
                return Ok(self.prune_decision());
            }
            self.stats.blocks_kept += 1;
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // b/<dir>/<hash> — block pointer
        if let Some(hash) = store_keys::parse_block_pointer_key(key) {
            if !self.job.gc.known_live_hash_contains(self.job.job_id, &hash) {
                self.stats.refs_dropped += 1;
                return Ok(self.prune_decision());
            }
            self.stats.refs_kept += 1;
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // br/<hash>/<name> — reverse reference
        if let Some((hash, _name)) = store_keys::parse_block_reverse_key(key) {
            if !self.job.gc.known_live_hash_contains(self.job.job_id, &hash) {
                self.stats.reverse_refs_tombstoned += 1;
                return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
            }
            self.stats.reverse_refs_kept += 1;
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // in/<epoch>/<name> — stale inbox entries
        if let Some((epoch, _name)) = store_keys::parse_inbox_key(key) {
            if epoch < self.epoch {
                self.stats.inbox_tombstoned += 1;
                return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
            }
            self.stats.inbox_kept += 1;
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
            self.stats.seqs_kept += 1;
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
            self.stats.files_kept += 1;
            self.stats.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        // Everything else (ix, dx/) — keep
        self.stats.metadata_kept += 1;
        self.stats.kept += 1;
        Ok(CompactionFilterDecision::Keep)
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        tracing::info!(
            folder_sk = %self.folder_sk,
            compaction_id = self.job.job_id,
            is_bottom = self.is_bottom,
            blocks_dropped = self.stats.blocks_dropped,
            blocks_kept = self.stats.blocks_kept,
            refs_dropped = self.stats.refs_dropped,
            refs_kept = self.stats.refs_kept,
            reverse_refs_tombstoned = self.stats.reverse_refs_tombstoned,
            reverse_refs_kept = self.stats.reverse_refs_kept,
            inbox_tombstoned = self.stats.inbox_tombstoned,
            inbox_kept = self.stats.inbox_kept,
            seqs_pruned = self.stats.seqs_pruned,
            seqs_kept = self.stats.seqs_kept,
            tombstones_pruned = self.stats.tombstones_pruned,
            files_kept = self.stats.files_kept,
            metadata_kept = self.stats.metadata_kept,
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
        let mut specs = Vec::new();

        // Default (unsegmented) tree. With a segment extractor configured this
        // is empty by construction, but we still propose for it so the same
        // scheduler works without segmentation.
        //
        // Only propose when there is L0 to merge in or more than one SR to
        // combine; otherwise the spec is a no-op (compact SR n → SR n) and the
        // compactor would loop on it.
        let default_l0: Vec<SourceId> = manifest
            .l0()
            .iter()
            .map(|sst| SourceId::SstView(sst.id))
            .collect();
        let default_srs: Vec<SourceId> = manifest
            .compacted()
            .iter()
            .map(|sr| SourceId::SortedRun(sr.id))
            .collect();
        let default_has_work = !default_l0.is_empty() || default_srs.len() > 1;
        if default_has_work {
            let mut default_sources = default_l0;
            default_sources.extend(default_srs);
            let destination = manifest
                .compacted()
                .iter()
                .map(|sr| sr.id)
                .min()
                .unwrap_or(0);
            specs.push(CompactionSpec::new(default_sources, destination));
        }

        // One tiered spec per named segment whose tree has any L0 or
        // compacted runs. Without these, segmented data never drains under
        // `FullCompactionSchedulerSupplier`.
        //
        // Destination ids must be unique across all in-flight specs; we pick
        // ids by taking max-known SR id + 1, then incrementing per segment.
        let mut next_dest = manifest
            .compacted()
            .iter()
            .map(|sr| sr.id)
            .chain(
                manifest
                    .segments()
                    .iter()
                    .flat_map(|s| s.compacted().iter().map(|sr| sr.id)),
            )
            .max()
            .map(|id| id.saturating_add(1))
            .unwrap_or(0);

        for segment in manifest.segments() {
            // Skip segments that have nothing to merge — single SR with no
            // L0 means the segment is already fully compacted, and
            // proposing `compact SR n into SR n` would loop indefinitely
            // because the spec passes validation but produces no progress.
            let has_l0 = !segment.l0().is_empty();
            let multi_sr = segment.compacted().len() > 1;
            if !has_l0 && !multi_sr {
                continue;
            }

            let mut sources: Vec<SourceId> = segment
                .l0()
                .iter()
                .map(|sst| SourceId::SstView(sst.id))
                .collect();
            for sr in segment.compacted() {
                sources.push(SourceId::SortedRun(sr.id));
            }
            let destination = segment
                .compacted()
                .iter()
                .map(|sr| sr.id)
                .min()
                .unwrap_or(next_dest);
            if segment.compacted().is_empty() {
                next_dest = next_dest.saturating_add(1);
            }
            specs.push(CompactionSpec::for_segment(
                segment.prefix().clone(),
                sources,
                destination,
            ));
        }

        specs
    }
}

pub(crate) struct BepCompactionSchedulerSupplier {
    size_tiered_supplier: SizeTieredCompactionSchedulerSupplier,
}

impl BepCompactionSchedulerSupplier {
    pub(crate) fn new() -> Self {
        Self {
            size_tiered_supplier: SizeTieredCompactionSchedulerSupplier::new(),
        }
    }
}

impl CompactionSchedulerSupplier for BepCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(BepCompactionScheduler {
            size_tiered: self.size_tiered_supplier.compaction_scheduler(options),
        })
    }
}

/// Minimum number of L0 SSTs in a metadata segment before a full-compaction
/// pass is proposed. Each pass rewrites the entire bottom SR of that segment,
/// so the cost is dominated by the SR size and is roughly constant in the
/// number of L0 SSTs being merged in. Triggering on every single new L0 SST
/// burns that fixed rewrite cost for ~no GC progress; waiting for a few L0s
/// to accumulate amortises the rewrite over a meaningful data increment.
///
/// Independent from the size-tiered `min_compaction_sources` that governs
/// `bd/` (much higher, since `bd/` reads don't suffer from many L0 SSTs).
const METADATA_MIN_L0: usize = 4;

struct BepCompactionScheduler {
    size_tiered: Box<dyn CompactionScheduler + Send + Sync>,
}

impl CompactionScheduler for BepCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let size_tiered_proposals = self.size_tiered.propose(state);
        let mut specs = retain_block_segment_specs(size_tiered_proposals);

        let manifest = state.manifest();
        // Destinations must be globally unique across every active spec; if the
        // size-tiered scheduler already picked some for the `bd/` segment, fold
        // them into the max so the metadata-segment specs below don't collide
        // and get rejected by `add_compaction`.
        let mut next_dest = next_free_sr_id(
            manifest
                .compacted()
                .iter()
                .map(|sr| sr.id)
                .chain(
                    manifest
                        .segments()
                        .iter()
                        .flat_map(|s| s.compacted().iter().map(|sr| sr.id)),
                )
                .chain(specs.iter().filter_map(|s| s.destination())),
        );

        for segment in manifest.segments() {
            if segment.prefix().as_ref() == crate::store_keys::BLOCK_SEGMENT_PREFIX {
                continue;
            }

            let enough_l0 = segment.l0().len() >= METADATA_MIN_L0;
            let multi_sr = segment.compacted().len() > 1;
            if !enough_l0 && !multi_sr {
                continue;
            }

            let mut sources: Vec<SourceId> = segment
                .l0()
                .iter()
                .map(|sst| SourceId::SstView(sst.id))
                .collect();
            for sr in segment.compacted() {
                sources.push(SourceId::SortedRun(sr.id));
            }
            let destination = segment
                .compacted()
                .iter()
                .map(|sr| sr.id)
                .min()
                .unwrap_or(next_dest);
            if segment.compacted().is_empty() {
                next_dest = next_dest.saturating_add(1);
            }
            specs.push(CompactionSpec::for_segment(
                segment.prefix().clone(),
                sources,
                destination,
            ));
        }

        specs
    }
}

/// Keep only the specs that target the `bd/` (block-data) segment. Used to
/// forward the size-tiered scheduler's proposals while suppressing everything
/// it might have proposed for other segments.
fn retain_block_segment_specs(specs: Vec<CompactionSpec>) -> Vec<CompactionSpec> {
    specs
        .into_iter()
        .filter(|spec| spec.segment().as_ref() == crate::store_keys::BLOCK_SEGMENT_PREFIX)
        .collect()
}

/// Compute the next sorted-run id to assign to a freshly created SR. Inputs
/// must cover both already-committed SRs in the manifest and destinations
/// already reserved by in-flight specs in the same proposal batch — SR ids
/// are globally unique across segments (RFC-0024), and collisions are
/// rejected by `add_compaction`.
fn next_free_sr_id(taken: impl Iterator<Item = u32>) -> u32 {
    taken.max().map(|id| id.saturating_add(1)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::all)]
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
            folder_sk: "test".to_string(),
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

        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        bloom_hashes.insert(&hash);

        let job = state.register(bloom_hashes.clone(), bloom_seqs.clone());
        let id = job.job_id;
        assert!(state.is_block_safe(&hash));
        assert!(state.known_live_hash_contains(id, &hash));

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

        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        // Empty bloom — hash is NOT in it.

        let _job = state.register(bloom_hashes.clone(), bloom_seqs.clone());
        assert!(!state.is_block_safe(&hash));

        // After recording a write, hash becomes safe.
        state.record_block_write(&hash, None);
        assert!(state.is_block_safe(&hash));
    }

    // --- GcFilter unit tests ---

    #[tokio::test]
    async fn gc_filter_drops_orphan_block_pointer() {
        let gc = Arc::new(CompactionState::new());
        let live_hash = [0xAA; 32];

        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        bloom_hashes.insert(&live_hash);

        let job = gc.register(bloom_hashes.clone(), bloom_seqs.clone());
        let mut filter = make_gc_filter_with_floor(job, epoch(1), None, true); // bottom run

        // Live block pointer — keep.
        let live_key = store_keys::block_pointer_key("docs", &live_hash);
        let decision = filter.filter(&make_row(&live_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        // Orphan block pointer — drop (since is_bottom is true).
        let orphan_hash = [0xBB; 32];
        let orphan_key = store_keys::block_pointer_key("docs", &orphan_hash);
        let decision = filter.filter(&make_row(&orphan_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Drop));

        assert_eq!(filter.stats.kept, 1);
        assert_eq!(filter.stats.refs_dropped, 1);
    }

    #[tokio::test]
    async fn gc_filter_tombstones_orphan_block_pointer_non_bottom() {
        let gc = Arc::new(CompactionState::new());
        let orphan_hash = [0xBB; 32];

        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));

        let job = gc.register(bloom_hashes.clone(), bloom_seqs.clone());
        let mut filter = make_gc_filter_with_floor(job, epoch(1), None, false); // non-bottom run

        let orphan_key = store_keys::block_pointer_key("docs", &orphan_hash);
        let decision = filter.filter(&make_row(&orphan_key)).await.unwrap();
        assert!(matches!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        ));
        assert_eq!(filter.stats.refs_dropped, 1);
    }

    #[tokio::test]
    async fn gc_filter_handles_block_data_keys() {
        let gc = Arc::new(CompactionState::new());
        let live_seq = 1050u64;
        let orphan_seq = 1060u64;

        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        bloom_seqs.insert(&live_seq);

        let job_bottom = gc.register(bloom_hashes.clone(), bloom_seqs.clone());

        // 1. Live bd/ key — keep
        let mut filter = make_gc_filter_with_floor(job_bottom, epoch(1), None, true);
        let live_key = store_keys::block_data_seq_key(live_seq);
        let decision = filter.filter(&make_row(&live_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));

        // 2. Orphan bd/ key (bottom run) — drop
        let orphan_key = store_keys::block_data_seq_key(orphan_seq);
        let decision = filter.filter(&make_row(&orphan_key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Drop));
        assert_eq!(filter.stats.blocks_dropped, 1);

        // 3. Orphan bd/ key (non-bottom run) — tombstone
        let job_non_bottom = gc.register(bloom_hashes.clone(), bloom_seqs.clone());
        let mut filter_non_bottom =
            make_gc_filter_with_floor(job_non_bottom, epoch(1), None, false);
        let decision = filter_non_bottom
            .filter(&make_row(&orphan_key))
            .await
            .unwrap();
        assert!(matches!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        ));
        assert_eq!(filter_non_bottom.stats.blocks_dropped, 1);

        // 4. Corrupted bd/ key — returns error
        let mut corrupted_key = store_keys::BLOCK_DATA_PREFIX.to_vec();
        corrupted_key.extend_from_slice(&[0, 0, 0]); // wrong size
        let res = filter.filter(&make_row(&corrupted_key)).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn gc_filter_tombstones_orphan_reverse_ref() {
        let gc = Arc::new(CompactionState::new());
        let orphan_hash = [0xDD; 32];

        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        // Empty bloom.

        let job = gc.register(bloom_hashes.clone(), bloom_seqs.clone());
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
        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));

        let job = gc.register(bloom_hashes.clone(), bloom_seqs.clone());
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
        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));

        let job = gc.register(bloom_hashes, bloom_seqs);
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
        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom_hashes, bloom_seqs);

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
        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom_hashes, bloom_seqs);

        let mut filter = make_gc_filter_with_floor(job, epoch(1), NonZeroI64::new(10), false);

        let key = store_keys::seq_key(15).unwrap(); // seq 15 >= peer_floor 10
        let decision = filter.filter(&make_row(&key)).await.unwrap();
        assert!(matches!(decision, CompactionFilterDecision::Keep));
        assert_eq!(filter.stats.kept, 1);
    }

    #[tokio::test]
    async fn gc_filter_prunes_deleted_file_tombstone() {
        let gc = Arc::new(CompactionState::new());
        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom_hashes, bloom_seqs);

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
        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom_hashes, bloom_seqs);

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
        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom_hashes, bloom_seqs);

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
        let bloom_hashes =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let bloom_seqs =
            Arc::new(AtomicBloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(100));
        let job = gc.register(bloom_hashes, bloom_seqs);

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

    // --- BepCompactionScheduler helpers ---

    #[test]
    fn next_free_sr_id_returns_zero_when_empty() {
        let taken: Vec<u32> = Vec::new();
        assert_eq!(next_free_sr_id(taken.into_iter()), 0);
    }

    #[test]
    fn next_free_sr_id_returns_max_plus_one() {
        let taken = vec![3u32, 1, 7, 4];
        assert_eq!(next_free_sr_id(taken.into_iter()), 8);
    }

    #[test]
    fn next_free_sr_id_avoids_in_flight_destinations() {
        // Without folding in-flight destinations, both schedulers would pick
        // the same next id (= committed_max + 1) and collide in add_compaction.
        let committed = [4u32, 2];
        let in_flight_destinations = [5u32]; // size-tiered already reserved 5
        let next = next_free_sr_id(committed.iter().copied().chain(in_flight_destinations));
        assert_eq!(next, 6, "must skip past in-flight destination 5");
    }

    #[test]
    fn retain_block_segment_specs_forwards_bd_and_drops_others() {
        // Build one spec per segment prefix; only the block (`b`) segment
        // should survive. There are only two valid segments at runtime —
        // the metadata spec uses the `m` prefix.
        let bd_spec = CompactionSpec::for_segment(
            Bytes::copy_from_slice(crate::store_keys::BLOCK_SEGMENT_PREFIX),
            vec![SourceId::SortedRun(0)],
            1,
        );
        let m_spec = CompactionSpec::for_segment(
            Bytes::copy_from_slice(crate::store_keys::METADATA_SEGMENT_PREFIX),
            vec![SourceId::SortedRun(2)],
            3,
        );

        let kept = retain_block_segment_specs(vec![bd_spec.clone(), m_spec]);
        assert_eq!(kept.len(), 1);
        assert_eq!(
            kept[0].segment().as_ref(),
            crate::store_keys::BLOCK_SEGMENT_PREFIX
        );
        assert_eq!(kept[0].destination(), Some(1));
    }

    #[test]
    fn retain_block_segment_specs_empty_passthrough() {
        // No size-tiered proposals → empty result, not a panic.
        assert!(retain_block_segment_specs(Vec::new()).is_empty());
    }

    #[tokio::test]
    async fn measure_compaction_overhead_and_bloom_cost() {
        use crate::SlateStorage;
        use bepository_bep::storage::StorageInspectorForTests;
        use bepository_bep::{FolderId, FolderLabel, Storage, StorageFolder};
        use futures::StreamExt;
        use futures::stream::BoxStream;
        use object_store::{
            GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
            PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
        };
        use std::sync::atomic::{AtomicU64, Ordering};

        #[derive(Debug)]
        struct TrackingObjectStore {
            inner: Arc<dyn ObjectStore>,
            bytes_written: Arc<AtomicU64>,
            compacted_bytes_written: Arc<AtomicU64>,
        }

        impl std::fmt::Display for TrackingObjectStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "TrackingObjectStore")
            }
        }

        #[async_trait]
        impl ObjectStore for TrackingObjectStore {
            async fn put_opts(
                &self,
                location: &Path,
                payload: PutPayload,
                opts: PutOptions,
            ) -> object_store::Result<PutResult> {
                let len = payload.content_length() as u64;
                self.bytes_written.fetch_add(len, Ordering::Relaxed);
                let loc_str = location.as_ref();
                if loc_str.contains("compacted") && loc_str.ends_with(".sst") {
                    self.compacted_bytes_written
                        .fetch_add(len, Ordering::Relaxed);
                }
                self.inner.put_opts(location, payload, opts).await
            }

            async fn put_multipart_opts(
                &self,
                location: &Path,
                opts: PutMultipartOptions,
            ) -> object_store::Result<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(location, opts).await
            }

            async fn get_opts(
                &self,
                location: &Path,
                options: GetOptions,
            ) -> object_store::Result<GetResult> {
                self.inner.get_opts(location, options).await
            }

            async fn delete(&self, location: &Path) -> object_store::Result<()> {
                self.inner.delete(location).await
            }

            fn list(
                &self,
                prefix: Option<&Path>,
            ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
                self.inner.list(prefix)
            }

            async fn list_with_delimiter(
                &self,
                prefix: Option<&Path>,
            ) -> object_store::Result<ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }

            async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
                self.inner.copy(from, to).await
            }

            async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
                self.inner.copy_if_not_exists(from, to).await
            }

            async fn put(
                &self,
                location: &Path,
                payload: PutPayload,
            ) -> object_store::Result<PutResult> {
                let len = payload.content_length() as u64;
                self.bytes_written.fetch_add(len, Ordering::Relaxed);
                let loc_str = location.as_ref();
                if loc_str.contains("compacted") && loc_str.ends_with(".sst") {
                    self.compacted_bytes_written
                        .fetch_add(len, Ordering::Relaxed);
                }
                self.inner.put(location, payload).await
            }

            async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
                self.inner.get(location).await
            }

            async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
                self.inner.head(location).await
            }
        }

        // Setup storage
        let mem_store = Arc::new(object_store::memory::InMemory::new());
        let bytes_written = Arc::new(AtomicU64::new(0));
        let compacted_bytes_written = Arc::new(AtomicU64::new(0));
        let tracking_store = Arc::new(TrackingObjectStore {
            inner: mem_store,
            bytes_written: bytes_written.clone(),
            compacted_bytes_written: compacted_bytes_written.clone(),
        });

        let storage = SlateStorage::new(
            tracking_store.clone(),
            None,
            tokio::runtime::Handle::current(),
        );
        storage
            .activate(bepository_lock::Epoch::new(1).unwrap())
            .await
            .unwrap();

        let folder_id = FolderId::from("measure");
        let folder_label = FolderLabel::from("Measure");
        storage
            .register_folder(folder_id, &folder_label)
            .await
            .unwrap();
        let folder = storage.folder(folder_id).await.unwrap();

        // We will write 20 files, each having 32 blocks of size 8192 (total 256 KiB per file)
        let num_files = 20usize;
        let blocks_per_file = 32usize;
        let block_size = 8192usize;

        let mut file_hashes = Vec::new();
        for f_idx in 0..num_files {
            let mut hashes = Vec::new();
            for b_idx in 0..blocks_per_file {
                let mut hash = [0u8; 32];
                let val = u32::try_from(f_idx * 1000 + b_idx).unwrap();
                let bytes = val.to_be_bytes();
                hash[0..4].copy_from_slice(&bytes);
                hashes.push(hash);
            }
            file_hashes.push(hashes);
        }

        use bepository_bep::proto::bep::{Counter, FileInfo, Vector};
        fn make_file_info(
            name: &str,
            version: u64,
            hashes: &[[u8; 32]],
            block_size: usize,
        ) -> FileInfo {
            FileInfo {
                name: name.to_string(),
                version: Some(Vector {
                    counters: vec![Counter {
                        id: 1,
                        value: version,
                    }],
                }),
                blocks: hashes
                    .iter()
                    .enumerate()
                    .map(|(i, h)| bepository_bep::proto::bep::BlockInfo {
                        offset: i64::try_from(i * block_size).unwrap(),
                        size: i32::try_from(block_size).unwrap(),
                        hash: h.to_vec(),
                    })
                    .collect(),
                size: i64::try_from(hashes.len() * block_size).unwrap(),
                ..Default::default()
            }
        }

        // Write the files initially
        for f_idx in 0..num_files {
            let name = format!("file_{f_idx}.bin");
            let fi = make_file_info(&name, 1, &file_hashes[f_idx], block_size);
            folder.insert_file(fi.clone()).await;
            for (b_idx, _hash) in file_hashes[f_idx].iter().enumerate() {
                let data = Bytes::from(vec![u8::try_from(f_idx).unwrap(); block_size]);
                folder
                    .insert_block(&name, i64::try_from(b_idx * block_size).unwrap(), data)
                    .await;
            }
            folder
                .complete_file(&name, fi.version.as_ref())
                .await
                .unwrap();
        }

        // Measure initial compaction
        let initial_write_bytes_before = compacted_bytes_written.load(Ordering::Relaxed);
        storage.compact(folder.id()).await.unwrap();
        let folder = storage.folder(folder_id).await.unwrap();
        let initial_compaction_written =
            compacted_bytes_written.load(Ordering::Relaxed) - initial_write_bytes_before;

        // Get initial SST list
        let get_sst_list = |store: Arc<dyn ObjectStore>| async move {
            let mut list = store.list(None);
            let mut ssts = Vec::new();
            while let Some(meta) = list.next().await {
                if let Ok(meta) = meta {
                    let path = meta.location.to_string();
                    if path.contains("compacted") && path.ends_with(".sst") {
                        ssts.push(path);
                    }
                }
            }
            ssts.sort();
            ssts
        };

        let tracking_store_dyn = tracking_store.clone() as Arc<dyn ObjectStore>;
        let ssts_initial = get_sst_list(tracking_store_dyn.clone()).await;

        // Cycle 2: Overwrite 5 files, Delete 3 files, Leave 12 files completely untouched
        let mut new_file_hashes = Vec::new();
        for f_idx in 0..5 {
            let mut hashes = Vec::new();
            for b_idx in 0..blocks_per_file {
                let mut hash = [0u8; 32];
                let val = u32::try_from(f_idx * 1000 + b_idx + 100000).unwrap();
                let bytes = val.to_be_bytes();
                hash[0..4].copy_from_slice(&bytes);
                hashes.push(hash);
            }
            new_file_hashes.push(hashes);
        }

        for f_idx in 0..5 {
            let name = format!("file_{f_idx}.bin");
            let fi = make_file_info(&name, 2, &new_file_hashes[f_idx], block_size);
            folder.insert_file(fi.clone()).await;
            for (b_idx, _hash) in new_file_hashes[f_idx].iter().enumerate() {
                let val = u8::try_from(f_idx).unwrap().checked_add(100).unwrap();
                let data = Bytes::from(vec![val; block_size]);
                folder
                    .insert_block(&name, i64::try_from(b_idx * block_size).unwrap(), data)
                    .await;
            }
            folder
                .complete_file(&name, fi.version.as_ref())
                .await
                .unwrap();
        }

        for f_idx in 5..8 {
            let name = format!("file_{f_idx}.bin");
            let mut deleted_file = make_file_info(&name, 2, &[], block_size);
            deleted_file.deleted = true;
            deleted_file.blocks.clear();
            deleted_file.size = 0;
            folder.insert_file(deleted_file.clone()).await;
            folder
                .complete_file(&name, deleted_file.version.as_ref())
                .await
                .unwrap();
        }

        // Measure bloom build cost
        let start_bloom = std::time::Instant::now();
        let store_slot = Arc::new(std::sync::OnceLock::new());
        store_slot
            .set(folder.store().clone())
            .ok()
            .expect("store_slot set should succeed");
        let gc = Arc::new(CompactionState::new());
        let supplier = GcFilterSupplier::new(
            "measure".to_string(),
            store_slot,
            gc,
            bepository_lock::Epoch::new(1).unwrap(),
        );
        let context = slatedb::CompactionJobContext {
            destination: 1,
            is_dest_last_run: true,
            compaction_clock_tick: 0,
            retention_min_seq: None,
        };
        let _filter = supplier.create_compaction_filter(&context).await.unwrap();
        let bloom_build_elapsed = start_bloom.elapsed();

        // Run compaction cycle 2
        let cycle2_write_bytes_before = compacted_bytes_written.load(Ordering::Relaxed);
        storage.compact(folder.id()).await.unwrap();
        let _folder = storage.folder(folder_id).await.unwrap();
        let cycle2_compaction_written =
            compacted_bytes_written.load(Ordering::Relaxed) - cycle2_write_bytes_before;

        let ssts_after_cycle2 = get_sst_list(tracking_store_dyn.clone()).await;

        let mut preserved_ssts = 0;
        for sst in &ssts_initial {
            if ssts_after_cycle2.contains(sst) {
                preserved_ssts += 1;
            }
        }

        let report = format!(
            "Phase 10 - Pre-lock-in Measurement Report\n\
             ==========================================\n\
             Initial files written: {num_files} files ({} KiB each)\n\
             Total initial block data: {} bytes\n\
             Initial compaction compacted-bytes written: {initial_compaction_written} bytes\n\
             Initial SSTs: {} files\n\n\
             Workload Cycle 2:\n\
             - Overwritten: 5 files\n\
             - Deleted: 3 files\n\
             - Untouched: 12 files\n\n\
             Settle-and-stay property:\n\
             - SSTs after Cycle 2: {} files\n\
             - Preserved initial SSTs: {preserved_ssts} of {}\n\
             - Preserved percentage: {:.2}%\n\n\
             Compaction Overhead / Write Amplification:\n\
             - Compacted bytes written in Cycle 2: {cycle2_compaction_written} bytes\n\
             - Bloom build elapsed time: {bloom_build_elapsed:?}\n",
            (blocks_per_file * block_size) / 1024,
            num_files * blocks_per_file * block_size,
            ssts_initial.len(),
            ssts_after_cycle2.len(),
            ssts_initial.len(),
            (preserved_ssts as f64 / ssts_initial.len() as f64) * 100.0,
        );

        println!("{report}");
    }
}
