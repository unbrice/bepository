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
    known_live: Arc<AtomicBloomFilter>,
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
    pub fn register(self: &Arc<Self>, known_live: Arc<AtomicBloomFilter>) -> CompactionJob {
        let job_id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = CompactionJobState { known_live };
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

    /// Record that a block hash was written, so all active compactions know it's live.
    pub fn record_block_write(&self, hash: &[u8; 32]) {
        let jobs = self.jobs.read();
        for job in jobs.values() {
            job.known_live.insert(hash);
        }
    }

    /// Check whether a block hash is considered live by all active compactions.
    ///
    /// Returns `true` if there are no active compactions, or if every active
    /// compaction recognises the hash (via known_live or written_since).
    pub fn is_block_safe(&self, hash: &[u8; 32]) -> bool {
        let jobs = self.jobs.read();
        if jobs.is_empty() {
            return true;
        }
        jobs.values().all(|job| job.known_live.contains(hash))
    }

    /// Check whether a specific compaction's known_live set contains a hash,
    /// also considering blocks written since that compaction started.
    pub fn known_live_contains(&self, job_id: u64, hash: &[u8; 32]) -> bool {
        let jobs = self.jobs.read();
        jobs.get(&job_id)
            .is_none_or(|job| job.known_live.contains(hash))
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
/// defined in [`crate::keys`].
pub(crate) struct FolderStore {
    pub(crate) db: Db,
    pub(crate) gc: Arc<CompactionState>,
    /// Per-name async lock pool. Serializes `n/`, `s/`, and `in/`
    /// mutations for a given name. Witnessed by `LockedFileName`.
    /// Compaction may drop dead entries outside this lock; see
    /// `compaction.rs`.
    name_locks: LockPool<String>,
    /// Guards the `IX_KEY` RMW and its batch write. Held briefly,
    /// in-memory only. Compaction preserves `ix`.
    seq_lock: Mutex<()>,
}

impl FolderStore {
    pub fn new(db: Db, gc: Arc<CompactionState>) -> Self {
        Self {
            db,
            gc,
            name_locks: LockPool::new(),
            seq_lock: Mutex::new(()),
        }
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
        let key = store_keys::file_key(name);
        match self.db.get(key).await.map_err(slate_err)? {
            Some(bytes) => {
                let file = File::decode(bytes)
                    .map_err(|e| StorageError::Corruption(format!("decode File: {e}")))?;
                let fi = file.file_info.ok_or_else(|| {
                    StorageError::Corruption(format!("missing file_info in File for {name}"))
                })?;
                Ok(Some(fi.try_into()?))
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
    /// INVARIANT: must remain the sole writer of `file_key`/`seq_key` in
    /// this module.
    async fn commit_with_new_seq(
        &self,
        locked_filename: &LockedFileName<'_>,
        file: FileInfo,
        mut batch: WriteBatch,
    ) -> Result<(i64, FileInfo), StorageError> {
        let name = locked_filename.name();

        // Remove old sequence entry if file already exists.
        // Safe outside seq_lock — see function doc.
        if let Some(old) = self.get_file(name).await?
            && old.sequence > 0
        {
            batch.delete(store_keys::seq_key(old.sequence)?);
        }

        let _guard = self.seq_lock.lock().await;

        // Allocate next sequence.
        let mut meta = self.get_index_meta().await?;
        meta.max_sequence += 1;
        let seq = meta.max_sequence;
        batch.put(store_keys::IX_KEY, meta.encode_to_vec());

        let mut stored = file;
        stored.sequence = seq;

        let file_wrapper = File {
            file_info: Some(stored.clone().into()),
        };
        batch.put(store_keys::file_key(name), file_wrapper.encode_to_vec());
        batch.put(store_keys::seq_key(seq)?, name.as_bytes());

        self.write_non_durable(batch).await?;
        Ok((seq, stored))
    }

    /// Insert or update a file in the index. Handles sequence bookkeeping.
    ///
    /// All mutations (old sequence cleanup, new sequence allocation, metadata
    /// update, file entry, sequence entry) are written in a single `WriteBatch`
    /// to ensure atomicity.
    pub async fn put_file(&self, file: &FileInfo) -> Result<i64, StorageError> {
        let locked_filename = self.lock_filename(&file.name).await;
        let (seq, _) = self
            .commit_with_new_seq(&locked_filename, file.clone(), WriteBatch::new())
            .await?;
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
    #[tracing::instrument(level = "debug", skip_all, fields(file = %name, epoch = %epoch.as_base32()))]
    pub async fn complete_file(
        &self,
        epoch: Epoch,
        name: &str,
        expected_version: Option<&Vector>,
    ) -> Result<Option<FileInfo>, StorageError> {
        let locked_filename = self.lock_filename(name).await;

        let inbox_key = store_keys::inbox_key(epoch, locked_filename.name());
        let staged: FileInfo = match self.db.get(&inbox_key).await.map_err(slate_err)? {
            Some(bytes) => {
                let inbox = Inbox::decode(bytes)
                    .map_err(|e| StorageError::Corruption(format!("decode staged Inbox: {e}")))?;
                inbox
                    .file_info
                    .ok_or_else(|| {
                        StorageError::Corruption(format!(
                            "missing file_info in staged Inbox for {name}"
                        ))
                    })?
                    .try_into()?
            }
            None => return Ok(None), // Idempotent
        };

        // If the inbox entry has been overwritten by a newer version (e.g. from an
        // incoming IndexUpdate while older blocks were still downloading), do not commit.
        if staged.version.as_ref() != expected_version {
            return Ok(None);
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

    /// Store block data with cross-directory dedup.
    ///
    /// All writes (data or ref + reverse ref) go through a single `WriteBatch`
    /// to avoid races between concurrent `store_block` calls with the same hash.
    pub async fn store_block(
        &self,
        name: &str,
        hash: &[u8],
        data: &[u8],
    ) -> Result<(), StorageError> {
        let hash_arr: &[u8; store_keys::HASH_LEN] = hash
            .try_into()
            .map_err(|_| StorageError::InvalidInput("block hash must be 32 bytes".into()))?;

        let dir = store_keys::dirname(name);
        let mut batch = WriteBatch::new();

        // Check if block already exists somewhere (via reverse ref scan).
        let existing_dir = self.find_block_dir(hash_arr).await?;

        match existing_dir {
            Some(canonical_dir) if canonical_dir != dir => {
                // Block exists in another directory — write a reference.
                let ref_key = store_keys::block_ref_key(dir, hash_arr);
                let block_ref = BlockRef {
                    source_dir: canonical_dir,
                };
                batch.put(ref_key, block_ref.encode_to_vec());
            }
            Some(_) => {
                // Block already exists in the same directory — no-op for data.
            }
            None => {
                // New block — write the actual data.
                let data_key = store_keys::block_data_key(dir, hash_arr);
                batch.put(data_key, data);
            }
        }

        // Always write the reverse ref.
        let rev_key = store_keys::block_reverse_key(hash_arr, name);
        batch.put(rev_key, []);

        self.write_non_durable(batch).await?;
        self.gc.record_block_write(hash_arr);
        Ok(())
    }

    /// Record that a block is now also used by the given file.
    ///
    /// If the block is already stored somewhere in the folder, writes a
    /// reference (or a no-op if it's already in the same directory) and returns
    /// `true`. If the block is missing, returns `false`.
    pub async fn reuse_block(&self, name: &str, hash: &[u8]) -> Result<bool, StorageError> {
        let hash_arr: &[u8; store_keys::HASH_LEN] = hash
            .try_into()
            .map_err(|_| StorageError::InvalidInput("block hash must be 32 bytes".into()))?;

        // Check if block already exists somewhere.
        let existing_dir = self.find_block_dir(hash_arr).await?;

        let canonical_dir = match existing_dir {
            Some(d) => d,
            None => return Ok(false),
        };

        let dir = store_keys::dirname(name);
        let mut batch = WriteBatch::new();

        if canonical_dir != dir {
            // Block exists in another directory — write a reference.
            let ref_key = store_keys::block_ref_key(dir, hash_arr);
            let block_ref = BlockRef {
                source_dir: canonical_dir,
            };
            batch.put(ref_key, block_ref.encode_to_vec());
        }

        // Always write/update the reverse ref.
        let rev_key = store_keys::block_reverse_key(hash_arr, name);
        batch.put(rev_key, []);

        self.write_non_durable(batch).await?;
        self.gc.record_block_write(hash_arr);
        Ok(true)
    }

    /// Read block data, chasing references if needed.
    pub async fn read_block(&self, name: &str, hash: &[u8]) -> Result<Bytes, StorageError> {
        let hash_arr: &[u8; store_keys::HASH_LEN] = hash
            .try_into()
            .map_err(|_| StorageError::InvalidInput("block hash must be 32 bytes".into()))?;

        let dir = store_keys::dirname(name);

        // Try direct data first.
        let data_key = store_keys::block_data_key(dir, hash_arr);
        if let Some(data) = self.db.get(&data_key).await.map_err(slate_err)? {
            return Ok(data);
        }

        // Try reference.
        let ref_key = store_keys::block_ref_key(dir, hash_arr);
        if let Some(ref_bytes) = self.db.get(&ref_key).await.map_err(slate_err)? {
            let block_ref = BlockRef::decode(ref_bytes)
                .map_err(|e| StorageError::Corruption(format!("decode BlockRef: {e}")))?;
            let canonical_key = store_keys::block_data_key(&block_ref.source_dir, hash_arr);
            if let Some(data) = self.db.get(&canonical_key).await.map_err(slate_err)? {
                return Ok(data);
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
    async fn find_block_dir(
        &self,
        hash: &[u8; store_keys::HASH_LEN],
    ) -> Result<Option<String>, StorageError> {
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
                let data_key = store_keys::block_data_key(&dir, hash);
                if self.db.get(&data_key).await.map_err(slate_err)?.is_some() {
                    return Ok(Some(dir));
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
