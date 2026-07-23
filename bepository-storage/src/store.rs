// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lockable::{LockPool, Lockable};
use parking_lot::RwLock;
use prost::Message;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{CloseReason, Db, ErrorKind, WriteBatch};
use tokio::sync::Mutex;

use bepository_bep::device_id::DeviceId;
use bepository_bep::error::StorageError;
use bepository_bep::proto::bep::{FileInfo, Vector};

use bepository_lock::Epoch;

use crate::proto::storage::{BlockRef, File, FolderIndexMeta, Inbox, RemoteIndexState};
use crate::store_keys;

use fastbloom::AtomicBloomFilter;

/// Tracks which block hashes are known-live for a single active compaction job.
struct CompactionJobState {
    known_live_hashes: Arc<AtomicBloomFilter>,
    known_live_seqs: Arc<AtomicBloomFilter>,
}

/// Handle to an active compaction job.
/// The job remains active as long as this handle is held.
pub(crate) struct CompactionJob {
    pub(crate) gc: Arc<CompactionState>,
    pub(crate) job_id: u64,
}

impl Drop for CompactionJob {
    fn drop(&mut self) {
        self.gc.unregister(self.job_id);
    }
}

/// Shared state between [`FolderStore`] and [`GcFilterSupplier`]/[`GcFilter`].
///
/// Tracks active compaction jobs so that concurrent writes can be accounted for
/// (blocks written after the known_live snapshot are always considered live).
pub(crate) struct CompactionState {
    jobs: RwLock<HashMap<u64, CompactionJobState>>,
    next_job: AtomicU64,
}

impl CompactionState {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            next_job: AtomicU64::new(1),
        }
    }

    /// Register a new compaction job.
    /// Returns a handle used to keep the job active.
    pub fn register(
        self: &Arc<Self>,
        known_live_hashes: Arc<AtomicBloomFilter>,
        known_live_seqs: Arc<AtomicBloomFilter>,
    ) -> CompactionJob {
        let job_id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = CompactionJobState {
            known_live_hashes,
            known_live_seqs,
        };
        self.jobs.write().insert(job_id, job);
        CompactionJob {
            gc: self.clone(),
            job_id,
        }
    }

    /// Unregister a completed compaction job.
    pub(crate) fn unregister(&self, job_id: u64) {
        self.jobs.write().remove(&job_id);
    }

    /// Record that a block was written, so all active compactions know it's live.
    pub fn record_block_write(&self, hash: &[u8; 32], seq: Option<u64>) {
        let jobs = self.jobs.read();
        for job in jobs.values() {
            job.known_live_hashes.insert(hash);
            if let Some(s) = seq {
                job.known_live_seqs.insert(&s);
            }
        }
    }

    /// Check whether a block hash is considered live by all active compactions.
    ///
    /// Returns `true` if there are no active compactions, or if every active
    /// compaction recognises the hash.
    pub fn is_block_safe(&self, hash: &[u8; 32]) -> bool {
        let jobs = self.jobs.read();
        if jobs.is_empty() {
            return true;
        }
        jobs.values()
            .all(|job| job.known_live_hashes.contains(hash))
    }

    /// Check whether a specific compaction's known_live set contains a hash,
    /// also considering blocks written since that compaction started.
    pub fn known_live_hash_contains(&self, job_id: u64, hash: &[u8; 32]) -> bool {
        let jobs = self.jobs.read();
        jobs.get(&job_id)
            .is_none_or(|job| job.known_live_hashes.contains(hash))
    }

    /// Check whether a specific compaction's known_live set contains a seq,
    /// also considering blocks written since that compaction started.
    pub fn known_live_seq_contains(&self, job_id: u64, seq: u64) -> bool {
        let jobs = self.jobs.read();
        jobs.get(&job_id)
            .is_none_or(|job| job.known_live_seqs.contains(&seq))
    }
}

/// Witness that the caller holds [`FolderStore::name_locks`] for `name`.
///
/// Constructed only by [`FolderStore::lock_filename`]. Acts as a compile-time
/// proof for functions that mutate per-name state and must run under
/// the lock.
struct LockedFileName<'a> {
    name: String,
    _guard: <LockPool<String> as Lockable<String, ()>>::Guard<'a>,
}

impl LockedFileName<'_> {
    fn name(&self) -> &str {
        &self.name
    }
}

/// Per-folder SlateDB wrapper.
///
/// Each shared folder gets its own SlateDB instance with the key layout
/// defined in [`crate::store_keys`].
pub(crate) struct FolderStore {
    pub(crate) db: Db,
    pub(crate) gc: Arc<CompactionState>,
    /// Per-name async lock pool. Witnessed by `LockedFileName`.
    /// Serializes all `mn`/`ms`/`mi` mutations per name:
    /// `stage_file`/`complete_file`/`put_file`/`put_file_with_carry` and
    /// `store_block`/`reuse_block` (witness-gated via `get_file_to_update`).
    /// Compaction may drop dead entries outside this lock; see
    /// `compaction.rs`.
    name_locks: LockPool<String>,
    /// Guards the `IX_KEY` RMW and its batch write. Held briefly,
    /// in-memory only. Compaction preserves `ix`.
    seq_lock: Arc<Mutex<()>>,
    blockseq: SeqAllocator,
}

impl FolderStore {
    pub async fn new(db: Db, gc: Arc<CompactionState>) -> Result<Self, StorageError> {
        let seq_lock = Arc::new(Mutex::new(()));
        let blockseq = SeqAllocator::load(Arc::new(db.clone()), seq_lock.clone()).await?;
        Ok(Self {
            db,
            gc,
            name_locks: LockPool::new(),
            seq_lock,
            blockseq,
        })
    }

    /// Acquire the per-name lock.
    async fn lock_filename(&self, name: impl Into<String>) -> LockedFileName<'_> {
        let name = name.into();
        let guard = self.name_locks.async_lock(name.clone()).await;
        LockedFileName {
            name,
            _guard: guard,
        }
    }

    async fn put_non_durable<K, V>(&self, key: K, value: V) -> Result<(), StorageError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let options = WriteOptions {
            await_durable: false,
            ..Default::default()
        };
        self.db
            .put_with_options(key, value, &PutOptions::default(), &options)
            .await
            .map_err(slate_err)?;
        Ok(())
    }

    async fn write_non_durable(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let options = WriteOptions {
            await_durable: false,
            ..Default::default()
        };
        self.db
            .write_with_options(batch, &options)
            .await
            .map_err(slate_err)?;
        Ok(())
    }

    // --- Index metadata (ix key) ---

    pub async fn get_index_meta(&self) -> Result<FolderIndexMeta, StorageError> {
        match self.db.get(store_keys::IX_KEY).await.map_err(slate_err)? {
            Some(bytes) => FolderIndexMeta::decode(bytes)
                .map_err(|e| StorageError::Corruption(format!("decode FolderIndexMeta: {e}"))),
            None => Ok(FolderIndexMeta::default()),
        }
    }

    // --- Remote device state (dx/ keys) ---

    pub async fn get_remote_state(
        &self,
        device: &DeviceId,
    ) -> Result<RemoteIndexState, StorageError> {
        let key = store_keys::device_key(device.as_bytes());
        match self.db.get(key).await.map_err(slate_err)? {
            Some(bytes) => RemoteIndexState::decode(bytes)
                .map_err(|e| StorageError::Corruption(format!("decode RemoteIndexState: {e}"))),
            None => Ok(RemoteIndexState::default()),
        }
    }

    pub async fn put_remote_state(
        &self,
        device: &DeviceId,
        state: &RemoteIndexState,
    ) -> Result<(), StorageError> {
        let key = store_keys::device_key(device.as_bytes());
        self.put_non_durable(key, state.encode_to_vec()).await?;
        Ok(())
    }

    // --- File index (n/ and s/ keys) ---

    pub async fn get_file(&self, name: &str) -> Result<Option<FileInfo>, StorageError> {
        match self.get_file_proto(name).await? {
            Some(fi) => Ok(Some(fi.try_into()?)),
            None => Ok(None),
        }
    }

    /// Read the raw stored `FileInfo` (block pointers included) for `name`.
    async fn get_file_proto(
        &self,
        name: &str,
    ) -> Result<Option<crate::proto::storage::FileInfo>, StorageError> {
        let key = store_keys::file_key(name);
        match self.db.get(key).await.map_err(slate_err)? {
            Some(bytes) => {
                let file = File::decode(bytes)
                    .map_err(|e| StorageError::Corruption(format!("decode File: {e}")))?;
                let fi = file.file_info.ok_or_else(|| {
                    StorageError::Corruption(format!("missing file_info in File for {name}"))
                })?;
                Ok(Some(fi))
            }
            None => Ok(None),
        }
    }

    /// Allocate a sequence number and commit a file entry.
    ///
    /// The prior-entry read runs outside `seq_lock`: the per-name lock
    /// excludes other `commit_with_new_seq` calls for this name, and this
    /// fn is the sole writer of `file_key`/`seq_key`. Compaction may drop
    /// dead entries concurrently; memtable writes win on reads.
    ///
    /// Block pointers (`blockseq`/`inline_data`) are carried forward from the
    /// prior committed entry for blocks with matching hashes: wire
    /// `FileInfo`s have no pointer fields, so without this every
    /// metadata-only commit or conflict resolution would orphan the block
    /// data, leaving it unreadable and GC-eligible.
    ///
    /// INVARIANT: must remain the sole writer of `file_key`/`seq_key` in
    /// this module.
    async fn commit_with_new_seq(
        &self,
        locked_filename: &LockedFileName<'_>,
        mut stored: crate::proto::storage::FileInfo,
        mut batch: WriteBatch,
    ) -> Result<(i64, FileInfo), StorageError> {
        let name = locked_filename.name();

        // Carry block pointers from, and remove the old sequence entry of,
        // the prior committed version (if any).
        // Safe outside seq_lock — see function doc.
        if let Some(old) = self.get_file_proto(name).await? {
            carry_block_pointers(&mut stored, &old);
            if old.sequence > 0 {
                batch.delete(store_keys::seq_key(old.sequence)?);
            }
        }

        let _guard = self.seq_lock.lock().await;

        // Allocate next sequence.
        let mut meta = self.get_index_meta().await?;
        meta.max_sequence += 1;
        let seq = meta.max_sequence;
        batch.put(store_keys::IX_KEY, meta.encode_to_vec());

        stored.sequence = seq;

        let file_wrapper = File {
            file_info: Some(stored.clone()),
        };
        batch.put(store_keys::file_key(name), file_wrapper.encode_to_vec());
        batch.put(store_keys::seq_key(seq)?, name.as_bytes());

        self.write_non_durable(batch).await?;
        let committed_bep = stored.try_into()?;
        Ok((seq, committed_bep))
    }

    /// Insert or update a file in the index. Handles sequence bookkeeping.
    ///
    /// All mutations (old sequence cleanup, new sequence allocation, metadata
    /// update, file entry, sequence entry) are written in a single `WriteBatch`
    /// to ensure atomicity.
    pub async fn put_file(&self, file: &FileInfo) -> Result<i64, StorageError> {
        let locked_filename = self.lock_filename(&file.name).await;
        let (seq, _) = self
            .commit_with_new_seq(&locked_filename, file.clone().into(), WriteBatch::new())
            .await?;
        Ok(seq)
    }

    /// Commit `file` at its name, additionally carrying block pointers from
    /// the committed entry at `carry_from` (a *different* name, e.g. the
    /// original path of a conflict-copy loser).
    ///
    /// Writes `mr` reverse refs for the carried separated blocks so the copy
    /// doesn't introduce fsck "missing reverse reference" findings. With no
    /// committed entry at `carry_from`, behaves like [`put_file`](Self::put_file).
    ///
    /// Carried blocks are recorded with the compaction GC (same as
    /// `store_block`/`reuse_block`): the `carry_from` read runs before the
    /// commit, and this keeps a carried seq live even if the source entry is
    /// overwritten and a GC snapshot lands in that window.
    pub async fn put_file_with_carry(
        &self,
        file: &FileInfo,
        carry_from: &str,
    ) -> Result<i64, StorageError> {
        let mut stored: crate::proto::storage::FileInfo = file.clone().into();
        let mut batch = WriteBatch::new();
        let mut carried: Vec<([u8; store_keys::HASH_LEN], u64)> = Vec::new();
        if let Some(src) = self.get_file_proto(carry_from).await? {
            carry_block_pointers(&mut stored, &src);
            for block in &stored.blocks {
                if let Some(seq) = block.blockseq
                    && let Ok(hash) = block.hash.as_slice().try_into()
                {
                    batch.put(store_keys::block_reverse_key(hash, &file.name), []);
                    carried.push((*hash, seq));
                }
            }
        }
        let locked_filename = self.lock_filename(&file.name).await;
        let (seq, _) = self
            .commit_with_new_seq(&locked_filename, stored, batch)
            .await?;
        for (hash, block_seq) in &carried {
            self.gc.record_block_write(hash, Some(*block_seq));
        }
        Ok(seq)
    }

    // --- Full index scan ---

    /// Return all files (for full index, since == 0).
    pub async fn all_files(&self) -> Result<Vec<FileInfo>, StorageError> {
        let mut iter = self
            .db
            .scan_prefix(store_keys::FILE_PREFIX)
            .await
            .map_err(slate_err)?;

        let mut files = Vec::new();
        while let Some(kv) = iter.next().await.map_err(slate_err)? {
            let file = File::decode(kv.value)
                .map_err(|e| StorageError::Corruption(format!("decode File: {e}")))?;
            let fi = file.file_info.ok_or_else(|| {
                StorageError::Corruption("missing file_info in File index entry".into())
            })?;
            files.push(fi.try_into()?);
        }
        Ok(files)
    }

    /// Return files with sequence > since (for delta index).
    pub async fn files_since(&self, since: i64) -> Result<Vec<FileInfo>, StorageError> {
        let start = store_keys::seq_scan_start(since)?;
        let end = store_keys::SEQ_SCAN_END.to_vec();

        let mut iter = self.db.scan(start.to_vec()..end).await.map_err(slate_err)?;

        let mut files = Vec::new();
        while let Some(kv) = iter.next().await.map_err(slate_err)? {
            // Value is the file name; look up the actual FileInfo.
            let name = std::str::from_utf8(&kv.value)
                .map_err(|e| StorageError::Corruption(format!("invalid name in seq entry: {e}")))?;
            if let Some(fi) = self.get_file(name).await? {
                files.push(fi);
            }
        }
        Ok(files)
    }

    // --- Inbox (two-phase file intake) ---

    /// Stage a file in the inbox for block transfer.
    ///
    /// The inbox key is unique per `(epoch, name)`; a later `stage_file` for
    /// the same name simply overwrites the entry (last-write-wins).
    ///
    /// Holds the per-name lock to serialize against `complete_file`'s
    /// read-check-commit sequence; this ensures we don't overwrite the inbox
    /// just as a concurrent completion is trying to promote it.
    #[tracing::instrument(level = "debug", skip_all, fields(file = %file.name, epoch = %epoch.as_base32()))]
    pub async fn stage_file(&self, epoch: Epoch, file: &FileInfo) -> Result<(), StorageError> {
        let locked_filename = self.lock_filename(&file.name).await;
        let key = store_keys::inbox_key(epoch, locked_filename.name());
        let inbox_wrapper = Inbox {
            file_info: Some(file.clone().into()),
        };
        self.put_non_durable(key, inbox_wrapper.encode_to_vec())
            .await?;
        Ok(())
    }

    /// Promote a staged file from inbox to committed index.
    ///
    /// Holds the per-name lock across the entire read-check-commit so a
    /// concurrent `stage_file` (e.g. a newer `IndexUpdate` arriving
    /// mid-download) cannot race with the version check: either we observe
    /// and commit the staged version, or we observe the newer one and return
    /// `Ok(None)`.
    ///
    /// Returns `Ok(None)` if the inbox is empty (idempotent re-call) or if
    /// the staged entry's version differs from `expected_version`.
    ///
    /// Errors if, after carrying block pointers from the prior committed
    /// entry, any staged block still has neither inline data nor a blockseq —
    /// promoting it would commit silent corruption.
    #[tracing::instrument(level = "debug", skip_all, fields(file = %name, epoch = %epoch.as_base32()))]
    pub async fn complete_file(
        &self,
        epoch: Epoch,
        name: &str,
        expected_version: Option<&Vector>,
    ) -> Result<Option<FileInfo>, StorageError> {
        let locked_filename = self.lock_filename(name).await;

        let inbox_key = store_keys::inbox_key(epoch, locked_filename.name());
        let staged: crate::proto::storage::FileInfo =
            match self.db.get(&inbox_key).await.map_err(slate_err)? {
                Some(bytes) => {
                    let inbox = Inbox::decode(bytes).map_err(|e| {
                        StorageError::Corruption(format!("decode staged Inbox: {e}"))
                    })?;
                    inbox.file_info.ok_or_else(|| {
                        StorageError::Corruption(format!(
                            "missing file_info in staged Inbox for {name}"
                        ))
                    })?
                }
                None => return Ok(None), // Idempotent
            };

        // If the inbox entry has been overwritten by a newer version (e.g. from an
        // incoming IndexUpdate while older blocks were still downloading), do not commit.
        let staged_bep_version: Option<Vector> = staged.version.clone().map(Into::into);
        if staged_bep_version.as_ref() != expected_version {
            return Ok(None);
        }

        // Refuse to promote an incompletely staged file: after carrying
        // pointers from the prior committed entry (as the commit will), every
        // block must carry inline data or point at a committed block-data key.
        let mut completed = staged.clone();
        if let Some(old) = self.get_file_proto(locked_filename.name()).await? {
            carry_block_pointers(&mut completed, &old);
        }
        if completed
            .blocks
            .iter()
            .any(|b| b.inline_data.is_none() && b.blockseq.is_none())
        {
            return Err(StorageError::Internal(format!(
                "staged file {name} has blocks with neither inline data nor blockseq"
            )));
        }

        let mut batch = WriteBatch::new();
        batch.delete(inbox_key);

        let (_seq, committed) = self
            .commit_with_new_seq(&locked_filename, staged, batch)
            .await?;

        tracing::debug!("file complete");

        Ok(Some(committed))
    }

    /// Delete all inbox entries with epoch < current_epoch.
    pub async fn gc_inbox(&self, current_epoch: Epoch) -> Result<usize, StorageError> {
        let mut iter = self
            .db
            .scan_prefix(store_keys::INBOX_PREFIX)
            .await
            .map_err(slate_err)?;

        let mut batch = WriteBatch::new();
        let mut count = 0;

        while let Some(kv) = iter.next().await.map_err(slate_err)? {
            if let Some((epoch, _name)) = store_keys::parse_inbox_key(&kv.key)
                && epoch < current_epoch
            {
                batch.delete(&kv.key);
                count += 1;
            }
        }

        if count > 0 {
            self.write_non_durable(batch).await?;
        }

        tracing::debug!(removed = count, "inbox gc done");

        Ok(count)
    }

    /// Return a specific inbox entry.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn get_inbox_file(
        &self,
        epoch: Epoch,
        name: &str,
    ) -> Result<Option<FileInfo>, StorageError> {
        let key = store_keys::inbox_key(epoch, name);
        match self.db.get(key).await.map_err(slate_err)? {
            Some(bytes) => {
                let inbox = Inbox::decode(bytes)
                    .map_err(|e| StorageError::Corruption(format!("decode Inbox: {e}")))?;
                let fi = inbox
                    .file_info
                    .ok_or_else(|| {
                        StorageError::Corruption(format!("missing file_info in Inbox for {name}"))
                    })?
                    .try_into()?;
                Ok(Some(fi))
            }
            None => Ok(None),
        }
    }

    // --- Peer floor computation ---

    /// Compute `min(max_sequence)` across all known remote peers (`dx/` entries).
    ///
    /// Returns `None` if no peers exist or all peers are at sequence 0
    /// (i.e. haven't completed their first index exchange yet).
    pub(crate) async fn compute_peer_floor(&self) -> Result<Option<NonZeroI64>, StorageError> {
        let mut iter = self
            .db
            .scan_prefix(store_keys::DEVICE_PREFIX)
            .await
            .map_err(slate_err)?;

        let mut floor: Option<i64> = None;
        while let Some(kv) = iter.next().await.map_err(slate_err)? {
            let state = RemoteIndexState::decode(kv.value)
                .map_err(|e| StorageError::Corruption(format!("decode RemoteIndexState: {e}")))?;
            floor = Some(match floor {
                Some(f) => f.min(state.max_sequence),
                None => state.max_sequence,
            });
        }
        Ok(floor.and_then(NonZeroI64::new))
    }

    // --- Block storage ---

    /// Read-only lookup of a file entry, inbox first, then main index.
    /// Returns the decoded `FileInfo`, the key it was found at, and whether
    /// that key is an inbox entry.
    pub(crate) async fn find_file_entry(
        &self,
        epoch: Option<Epoch>,
        name: &str,
    ) -> Result<(crate::proto::storage::FileInfo, Vec<u8>, bool), StorageError> {
        if let Some(ep) = epoch {
            let key = store_keys::inbox_key(ep, name);
            if let Some(bytes) = self.db.get(&key).await.map_err(slate_err)? {
                let inbox = Inbox::decode(bytes)
                    .map_err(|e| StorageError::Corruption(format!("decode staged Inbox: {e}")))?;
                if let Some(fi) = inbox.file_info {
                    return Ok((fi, key, true));
                }
            }
        }

        // Fall back to main index
        let key = store_keys::file_key(name);
        if let Some(bytes) = self.db.get(&key).await.map_err(slate_err)? {
            let file_wrapper = File::decode(bytes)
                .map_err(|e| StorageError::Corruption(format!("decode File: {e}")))?;
            if let Some(fi) = file_wrapper.file_info {
                return Ok((fi, key, false));
            }
        }

        Err(StorageError::NotFound(format!(
            "file not found in inbox or main index: {name}"
        )))
    }

    /// Find the file info to update, either in the inbox or the main index.
    ///
    /// Entry point for per-name read-modify-write: the `LockedFileName`
    /// witness guarantees the caller holds the per-name lock across the
    /// read-modify-write window. Read-only callers use [`find_file_entry`].
    async fn get_file_to_update(
        &self,
        locked: &LockedFileName<'_>,
        epoch: Option<Epoch>,
    ) -> Result<(crate::proto::storage::FileInfo, Vec<u8>, bool), StorageError> {
        self.find_file_entry(epoch, locked.name()).await
    }

    /// Store block data with cross-directory dedup.
    ///
    /// Holds the per-name lock across the whole read-modify-write, so
    /// concurrent `store_block` calls on the same file serialize and cannot
    /// lose each other's pointer updates. Mutations land in a single
    /// `WriteBatch` for per-call atomicity.
    pub async fn store_block(
        &self,
        epoch: Option<Epoch>,
        name: &str,
        hash: &[u8],
        data: &[u8],
    ) -> Result<(), StorageError> {
        let hash_arr: &[u8; store_keys::HASH_LEN] = hash
            .try_into()
            .map_err(|_| StorageError::InvalidInput("block hash must be 32 bytes".into()))?;

        let locked_filename = self.lock_filename(name).await;

        let dir = store_keys::dirname(name);
        let mut batch = WriteBatch::new();

        let is_inline = data.len() < store_keys::INLINE_BLOCK_THRESHOLD as usize;
        if is_inline {
            // Find and update the FileInfo in inbox or main index.
            let (mut file_info, key_to_update, is_inbox) =
                self.get_file_to_update(&locked_filename, epoch).await?;
            let mut updated_any = false;
            for block in &mut file_info.blocks {
                if block.hash == hash {
                    block.inline_data = Some(data.to_vec());
                    block.blockseq = None;
                    updated_any = true;
                }
            }
            if !updated_any {
                return Err(StorageError::NotFound(format!(
                    "block with hash {} not found in file {}",
                    hex::encode(hash),
                    name
                )));
            }

            if is_inbox {
                let inbox_wrapper = Inbox {
                    file_info: Some(file_info),
                };
                batch.put(key_to_update, inbox_wrapper.encode_to_vec());
            } else {
                let file_wrapper = File {
                    file_info: Some(file_info),
                };
                batch.put(key_to_update, file_wrapper.encode_to_vec());
            }
            self.write_non_durable(batch).await?;
            return Ok(());
        }

        // Check if block already exists somewhere (via reverse ref scan).
        let existing = self.find_block_dir(hash_arr).await?;

        let seq = match existing {
            Some((canonical_dir, block_ref)) => {
                if canonical_dir != dir {
                    // Block exists in another directory — write a reference pointer pointing to same seqno.
                    let local_pointer_key = store_keys::block_pointer_key(dir, hash_arr);
                    batch.put(local_pointer_key, block_ref.encode_to_vec());
                }
                block_ref.seqno
            }
            None => {
                // New block — write the actual data to bd<seqno> and a pointer to mb<dir>/<hash>.
                let seq = self.blockseq.allocate().await?;
                let data_key = store_keys::block_data_seq_key(seq);
                batch.put(data_key, data);

                let pointer_key = store_keys::block_pointer_key(dir, hash_arr);
                let block_ref = BlockRef { seqno: seq };
                batch.put(pointer_key, block_ref.encode_to_vec());
                seq
            }
        };

        // Always write the reverse ref.
        let rev_key = store_keys::block_reverse_key(hash_arr, name);
        batch.put(rev_key, []);

        // Find and update the FileInfo in inbox or main index.
        let (mut file_info, key_to_update, is_inbox) =
            self.get_file_to_update(&locked_filename, epoch).await?;
        let mut updated_any = false;
        for block in &mut file_info.blocks {
            if block.hash == hash {
                block.blockseq = Some(seq);
                block.inline_data = None;
                updated_any = true;
            }
        }
        if !updated_any {
            return Err(StorageError::NotFound(format!(
                "block with hash {} not found in file {}",
                hex::encode(hash),
                name
            )));
        }

        if is_inbox {
            let inbox_wrapper = Inbox {
                file_info: Some(file_info),
            };
            batch.put(key_to_update, inbox_wrapper.encode_to_vec());
        } else {
            let file_wrapper = File {
                file_info: Some(file_info),
            };
            batch.put(key_to_update, file_wrapper.encode_to_vec());
        }

        self.write_non_durable(batch).await?;
        self.gc.record_block_write(hash_arr, Some(seq));
        Ok(())
    }

    /// Record that a block is now also used by the given file.
    ///
    /// If the block is already stored somewhere in the folder, writes a
    /// pointer (or a no-op if it's already in the same directory) and returns
    /// `true`. If the block is missing, returns `false`.
    pub async fn reuse_block(
        &self,
        epoch: Option<Epoch>,
        name: &str,
        hash: &[u8],
    ) -> Result<bool, StorageError> {
        let hash_arr: &[u8; store_keys::HASH_LEN] = hash
            .try_into()
            .map_err(|_| StorageError::InvalidInput("block hash must be 32 bytes".into()))?;

        let locked_filename = self.lock_filename(name).await;

        // Check if block already exists somewhere.
        let existing = self.find_block_dir(hash_arr).await?;

        let (canonical_dir, block_ref) = match existing {
            Some((d, r)) => (d, r),
            None => return Ok(false),
        };

        let dir = store_keys::dirname(name);
        let mut batch = WriteBatch::new();

        if canonical_dir != dir {
            // Block exists in another directory — write a reference pointer pointing to same seqno.
            let local_pointer_key = store_keys::block_pointer_key(dir, hash_arr);
            batch.put(local_pointer_key, block_ref.encode_to_vec());
        }

        // Always write/update the reverse ref.
        let rev_key = store_keys::block_reverse_key(hash_arr, name);
        batch.put(rev_key, []);

        // Find and update the FileInfo in inbox or main index.
        let (mut file_info, key_to_update, is_inbox) =
            self.get_file_to_update(&locked_filename, epoch).await?;
        let mut updated_any = false;
        for block in &mut file_info.blocks {
            if block.hash == hash {
                block.blockseq = Some(block_ref.seqno);
                block.inline_data = None;
                updated_any = true;
            }
        }
        if !updated_any {
            return Err(StorageError::NotFound(format!(
                "block with hash {} not found in file {}",
                hex::encode(hash),
                name
            )));
        }

        if is_inbox {
            let inbox_wrapper = Inbox {
                file_info: Some(file_info),
            };
            batch.put(key_to_update, inbox_wrapper.encode_to_vec());
        } else {
            let file_wrapper = File {
                file_info: Some(file_info),
            };
            batch.put(key_to_update, file_wrapper.encode_to_vec());
        }

        self.write_non_durable(batch).await?;
        self.gc.record_block_write(hash_arr, Some(block_ref.seqno));
        Ok(true)
    }

    /// Read block data, chasing references if needed.
    pub async fn read_block(&self, name: &str, hash: &[u8]) -> Result<Bytes, StorageError> {
        let _hash_arr: &[u8; store_keys::HASH_LEN] = hash
            .try_into()
            .map_err(|_| StorageError::InvalidInput("block hash must be 32 bytes".into()))?;

        // 1. Try to read from main file index
        if let Some(bytes) = self
            .db
            .get(store_keys::file_key(name))
            .await
            .map_err(slate_err)?
        {
            let file_wrapper = File::decode(bytes)
                .map_err(|e| StorageError::Corruption(format!("decode File: {e}")))?;
            if let Some(fi) = file_wrapper.file_info {
                for block in fi.blocks {
                    if block.hash == hash
                        && let Some(data) =
                            crate::block_read::resolve_block_data(&self.db, &block).await?
                    {
                        return Ok(data);
                    }
                }
            }
        }

        Err(StorageError::NotFound(format!(
            "block not found: {name} hash={}",
            hex::encode(hash)
        )))
    }

    /// Check if a block with the given hash exists anywhere in this folder.
    ///
    /// When a compaction GC is active, also verifies the hash is recognised by
    /// the known_live filter (or was written since the snapshot), so that callers
    /// don't mistakenly skip a block that compaction is about to remove.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn has_block(&self, hash: &[u8]) -> Result<bool, StorageError> {
        let hash_arr: &[u8; store_keys::HASH_LEN] = match hash.try_into() {
            Ok(h) => h,
            Err(_) => return Ok(false),
        };
        let prefix = store_keys::block_reverse_prefix(hash_arr);
        let mut iter = self.db.scan_prefix(&prefix).await.map_err(slate_err)?;
        let exists = iter.next().await.map_err(slate_err)?.is_some();
        if !exists {
            return Ok(false);
        }
        Ok(self.gc.is_block_safe(hash_arr))
    }

    /// Find which directory holds the canonical data for a block hash.
    ///
    /// Scans reverse references and verifies the canonical data actually exists
    /// in the candidate directory before returning it. When a compaction GC is
    /// active, also checks that the hash is safe (recognised by all active
    /// known_live filters or written since the snapshot).
    ///
    /// TODO: remove the `bd<seq>` existence check at the end of the loop.
    /// It is load-bearing today because the dual-bloom GC builds
    /// `known_live_hashes` and `known_live_seqs` in **separate compaction
    /// jobs with separate snapshots** — a metadata-segment job whose
    /// snapshot still sees a file F can `Keep mb<H>` while a later
    /// block-segment job whose snapshot has lost F can `Drop bd<S>`,
    /// leaving a pointer without data. The defensive get keeps that state
    /// from poisoning dedup (it falls through to the new-block branch,
    /// which self-heals by allocating a fresh seqno and re-writing the
    /// bytes). Once the two jobs share a snapshot epoch — or otherwise
    /// prove "`mb` outlives `bd`" is unreachable — this branch can return
    /// `Some((dir, block_ref))` without the second `db.get` and save one
    /// block-segment lookup per dedup hit. See the "find_block_dir
    /// defensive `bd<seq>` existence check" caveat in
    /// `bepository-storage/OVERVIEW.md`.
    async fn find_block_dir(
        &self,
        hash: &[u8; store_keys::HASH_LEN],
    ) -> Result<Option<(String, BlockRef)>, StorageError> {
        // If a compaction is about to GC this hash, pretend it doesn't exist
        // so the caller writes full data (which then gets recorded via
        // record_block_write).
        if !self.gc.is_block_safe(hash) {
            return Ok(None);
        }

        let prefix = store_keys::block_reverse_prefix(hash);
        let mut iter = self.db.scan_prefix(&prefix).await.map_err(slate_err)?;

        while let Some(kv) = iter.next().await.map_err(slate_err)? {
            if let Some((_hash, name)) = store_keys::parse_block_reverse_key(&kv.key) {
                let dir = store_keys::dirname(&name).to_string();
                let pointer_key = store_keys::block_pointer_key(&dir, hash);
                if let Some(ref_bytes) = self.db.get(&pointer_key).await.map_err(slate_err)? {
                    let block_ref = BlockRef::decode(ref_bytes)
                        .map_err(|e| StorageError::Corruption(format!("decode BlockRef: {e}")))?;
                    store_keys::validate_block_seq(block_ref.seqno)?;
                    let data_key = store_keys::block_data_seq_key(block_ref.seqno);
                    if self.db.get(&data_key).await.map_err(slate_err)?.is_some() {
                        return Ok(Some((dir, block_ref)));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Close the underlying DB, flushing the memtable and shutting down the
    /// background compactor (which runs the GC filter).
    ///
    /// `Db` has no `Drop` impl — without an explicit `close()`, background
    /// tasks are silently abandoned and unflushed writes are lost.
    pub async fn close(&self) -> Result<(), StorageError> {
        self.db.close().await.map_err(slate_err)
    }
}

pub(crate) fn slate_err(e: slatedb::Error) -> StorageError {
    match e.kind() {
        ErrorKind::Closed(CloseReason::Fenced) => {
            StorageError::Standby(format!("slatedb fenced: {e}"))
        }
        ErrorKind::Data => StorageError::Corruption(format!("slatedb data error: {e}")),
        ErrorKind::Invalid => StorageError::InvalidInput(format!("slatedb invalid: {e}")),
        _ => StorageError::TransientIo(format!("slatedb: {e}")),
    }
}

/// Copy block pointers (`blockseq`/`inline_data`) from `old` into `new` for
/// blocks that carry no pointer yet, matched by hash.
///
/// Hash match alone is sufficient: blocks are content-addressed, so equal
/// hashes imply identical content (and size).
fn carry_block_pointers(
    new: &mut crate::proto::storage::FileInfo,
    old: &crate::proto::storage::FileInfo,
) {
    let mut by_hash: HashMap<&[u8], &crate::proto::storage::BlockInfo> = HashMap::new();
    for block in &old.blocks {
        if block.blockseq.is_some() || block.inline_data.is_some() {
            by_hash.entry(&block.hash).or_insert(block);
        }
    }
    for block in &mut new.blocks {
        if block.blockseq.is_none()
            && block.inline_data.is_none()
            && let Some(src) = by_hash.get(block.hash.as_slice())
        {
            block.blockseq = src.blockseq;
            block.inline_data = src.inline_data.clone();
        }
    }
}

/// A range of sequence numbers reserved in RAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeqRange {
    pub(crate) next: u64,
    pub(crate) end: u64,
}

/// Bounded block sequence allocator. Bumps the persisted high water mark in `ix`
/// by batches of N to avoid database writes on the block hot path.
pub struct SeqAllocator {
    pub(crate) db: Arc<Db>,
    pub(crate) seq_lock: Arc<Mutex<()>>,
    pub(crate) state: parking_lot::Mutex<SeqRange>,
}

/// Reservation batch size. Amortizes the cost of durable disk writes.
/// Worst-case crash waste is bounded by N - 1.
const RESERVATION_N: u64 = 256;

impl SeqAllocator {
    pub async fn load(db: Arc<Db>, seq_lock: Arc<Mutex<()>>) -> Result<Self, StorageError> {
        let ix_bytes = db.get(store_keys::IX_KEY).await.map_err(slate_err)?;
        let persisted = match ix_bytes {
            Some(bytes) => {
                let meta = FolderIndexMeta::decode(bytes).map_err(|e| {
                    StorageError::Corruption(format!("decode FolderIndexMeta: {e}"))
                })?;
                meta.next_blockseq.unwrap_or(0)
            }
            None => 0,
        };

        Ok(Self {
            db,
            seq_lock,
            state: parking_lot::Mutex::new(SeqRange {
                next: persisted,
                end: persisted,
            }),
        })
    }

    pub async fn allocate(&self) -> Result<u64, StorageError> {
        // Fast path: check if we have remaining sequences in the current reservation.
        {
            let mut state = self.state.lock();
            if state.next < state.end {
                let seq = state.next;
                state.next += 1;
                return Ok(seq);
            }
        }

        // Slow path: acquire the async seq_lock to serialize refill operations.
        let _guard = self.seq_lock.lock().await;

        // Double check: check if someone else did the refill while we waited for seq_lock.
        {
            let mut state = self.state.lock();
            if state.next < state.end {
                let seq = state.next;
                state.next += 1;
                return Ok(seq);
            }
        }

        // Refill the reservation.
        let ix_bytes = self.db.get(store_keys::IX_KEY).await.map_err(slate_err)?;
        let mut meta = match ix_bytes {
            Some(bytes) => FolderIndexMeta::decode(bytes)
                .map_err(|e| StorageError::Corruption(format!("decode FolderIndexMeta: {e}")))?,
            None => FolderIndexMeta::default(),
        };

        let base = meta
            .next_blockseq
            .unwrap_or(0)
            .max(store_keys::MIN_BLOCK_SEQ);
        let next_limit = base + RESERVATION_N;
        meta.next_blockseq = Some(next_limit);

        // Write the new reservation without waiting on the auto-flush timer
        // (which may be tens of seconds), then trigger an immediate flush.
        let options = WriteOptions {
            await_durable: false,
            ..Default::default()
        };
        self.db
            .put_with_options(
                store_keys::IX_KEY,
                meta.encode_to_vec(),
                &PutOptions::default(),
                &options,
            )
            .await
            .map_err(slate_err)?;
        self.db.flush().await.map_err(slate_err)?;

        // Update local state and hand out the first sequence.
        {
            let mut state = self.state.lock();
            state.next = base + 1;
            state.end = next_limit;
        }

        Ok(base)
    }
}
