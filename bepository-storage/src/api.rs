// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use linear_map::LinearMap;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
};
use futures::StreamExt;
use futures::stream::{self, Stream};
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use secrecy::SecretSlice;
use slatedb::admin::AdminBuilder;
use slatedb::compactor::{CompactionSpec, SourceId};
use slatedb::config::{
    CheckpointOptions, CheckpointScope, CompactorOptions, FlushOptions, FlushType, Settings,
};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::{CachedEntry, DbCache};
use slatedb::{Checkpoint, CompactorBuilder, Db, DbReader, DbReaderBuilder};
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

use bepository_bep::device_id::DeviceId;
use bepository_bep::error::StorageError;
use bepository_bep::ids::{FolderId, FolderLabel, FolderLabelRef};
use bepository_bep::proto::bep::FileInfo;
#[cfg(any(test, feature = "test-utils"))]
use bepository_bep::storage::StorageInspectorForTests;
use bepository_bep::storage::{Sequence, Storage, StorageFolder, UpdateResult, blocks_equal};
use bepository_bep::{Ordering, compare};
use bepository_lock::Epoch;

use crate::compaction::{GcFilterSupplier, StoreSlot};
use crate::meta::{self, CheckpointSchedule, FolderEntry, Meta, MetaIdentity};
use crate::store::{CompactionState, FolderStore};
use bepository_tls::Identity;

/// Storage-layer directory identifier for a folder in [`SlateStorage`].
///
/// Wraps the `folder_<BASE32>` path prefix used as the SlateDB path
/// within the object store. Opaque outside this crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FolderStorageKey(String);

impl FolderStorageKey {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FolderStorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Deref for FolderStorageKey {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<FolderStorageKey> for String {
    fn from(sk: FolderStorageKey) -> Self {
        sk.0
    }
}

/// Progress event emitted during an FSCK integrity check.
#[derive(Debug, Clone)]
pub enum FsckEvent {
    /// Integrity check started for a specific folder.
    FolderStarted { id: FolderId },
    /// An integrity error was found for the folder.
    FolderError { id: FolderId, error: String },
    /// Integrity check finished for the folder.
    FolderFinished { id: FolderId, errors_found: usize },
}

/// SlateDB-backed [`Storage`] implementation.
///
/// Persistent metadata (identity, folder registry) is stored as a TOML file
/// `bepository-{epoch}.toml` in the object store root. Each folder gets its
/// own SlateDB instance under `folder_<BASE32(id)>/` in the object store,
/// opened lazily on first access.
///
/// All folders in an activated storage session share a single Foyer block
/// cache (RAM + local disk) to optimize resource usage and improve the
/// deduplication hit rate.
///
/// `Storage` trait methods receive folder **labels** from the BEP
/// engine and resolve them to directory names via the registry.
///
/// Cloning is cheap and shares all internal state (open DB handles, caches).
#[derive(Clone)]
pub struct SlateStorage {
    inner: Arc<SlateStorageInner>,
}

struct SlateStorageInner {
    object_store: Arc<dyn ObjectStore>,
    /// Snapshot readers are pinned to immutable checkpoint UUIDs and do not
    /// depend on the epoch.
    readers: Mutex<HashMap<(FolderStorageKey, Uuid), Arc<DbReader>>>,
    /// Provider for the Foyer hybrid disk cache.
    /// When set, each folder gets a subdirectory based on the resolved device ID.
    cache_provider: Option<Arc<dyn CacheProvider + Send + Sync>>,
    runtime: tokio::runtime::Handle,
    /// Override for the SlateDB compactor `poll_interval`. `None` means use the
    /// default from `make_db_settings`. Snapshotted into [`Activated`] at
    /// `activate()` time; mutating it later has no effect.
    compactor_poll_interval: Mutex<Option<Duration>>,
    activated: std::sync::OnceLock<Arc<Activated>>,
}

/// In-memory view of the current `bepository-{epoch}.toml`. Held inside
/// `Activated::state`; rebuilt in full whenever a `modify_meta` commits.
///
/// `registry` is a projection of `meta.folders` — enforced by construction:
/// the struct's fields are private and the only constructor is `from_meta`.
struct MetaState {
    meta: Meta,
    registry: LinearMap<FolderId, (FolderStorageKey, FolderLabel)>,
}

impl MetaState {
    fn from_meta(meta: Meta) -> Self {
        let registry = meta
            .folders
            .iter()
            .map(|(key, entry)| {
                (
                    entry.id,
                    (SlateStorage::folder_storage_key(key), entry.label.clone()),
                )
            })
            .collect();
        Self { meta, registry }
    }
}

/// State that is valid only after `SlateStorage::activate()` succeeds.
/// Pinned to a single `Epoch` for its entire lifetime.
struct Activated {
    epoch: Epoch,
    stores: tokio::sync::RwLock<HashMap<String, Arc<FolderStore>>>,
    /// Single cached view of the current meta. Populated by `activate()`,
    /// replaced wholesale inside `modify_meta`. Readers always see a
    /// consistent `(meta, registry)` pair.
    state: RwLock<MetaState>,
    /// Serializes concurrent `modify_meta` callers. Kept separate from
    /// `state` so the in-memory swap is brief and readers don't stall
    /// behind the network PUT.
    meta_lock: tokio::sync::Mutex<()>,
    object_store: Arc<dyn ObjectStore>,
    cache_provider: Option<Arc<dyn CacheProvider + Send + Sync>>,
    runtime: tokio::runtime::Handle,
    db_cache: tokio::sync::OnceCell<Arc<dyn DbCache>>,
    /// Snapshot of [`SlateStorageInner::compactor_poll_interval`] taken in
    /// `activate()`. `None` keeps `make_db_settings`'s default.
    compactor_poll_interval: Option<Duration>,
}

/// Provides the base directory for the Foyer block cache, computed from the device ID.
pub trait CacheProvider: Send + Sync {
    /// Returns the cache base directory for the given device ID.
    fn get_cache_dir(&self, device_id: &DeviceId) -> Option<PathBuf>;
}

impl SlateStorage {
    /// Create a new SlateStorage backed by the given object store.
    ///
    /// `cache_provider` computes the per-device root for the Foyer hybrid disk cache.
    /// When `Some`, each folder DB gets a subdirectory. Pass `None` to use an
    /// in-memory-only block cache (e.g. in tests or the testserver).
    ///
    /// `runtime` is the dedicated Tokio handle for SlateDB background workers.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        cache_provider: Option<Arc<dyn CacheProvider + Send + Sync>>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            inner: Arc::new(SlateStorageInner {
                object_store,
                readers: Mutex::new(HashMap::new()),
                cache_provider,
                runtime,
                compactor_poll_interval: Mutex::new(None),
                activated: std::sync::OnceLock::new(),
            }),
        }
    }

    /// Override the SlateDB compactor `poll_interval` for every folder opened
    /// after this call. Must be called *before* [`activate()`] — the value is
    /// snapshotted at activation time. Use to make `compact()` start work
    /// within seconds (short-lived processes like `fsck-compact`, tests)
    /// instead of waiting up to the production default (~60 s, tuned as a
    /// battery floor).
    ///
    /// [`activate()`]: SlateStorage::activate
    pub fn set_compactor_poll_interval(&self, interval: Duration) {
        *self.inner.compactor_poll_interval.lock() = Some(interval);
    }

    fn activated(&self) -> Result<&Activated, StorageError> {
        self.inner.activated.get().map(|a| &**a).ok_or_else(|| {
            StorageError::Standby("epoch not set: call activate() before this operation".into())
        })
    }

    /// Activate this storage instance with the given lock epoch.
    ///
    /// Sets the epoch (write-once, panics on double call), copies the
    /// previous epoch's meta file to `bepository-{epoch}.toml`, and deletes
    /// old meta files. Must be called once after acquiring the distributed
    /// lock and before any read/write operations.
    #[tracing::instrument(level = "info", skip_all, fields(epoch = %epoch.as_base32()))]
    pub async fn activate(&self, epoch: Epoch) -> Result<(), StorageError> {
        // Single-writer setup: read whichever meta file exists, write it back
        // under the new epoch name, delete the old ones.
        let m = self.read_meta_unlocked().await?;

        // Forward format fence: refuse to activate (and thus to rewrite) a store
        // written by a newer format version. Must run before the unconditional
        // write_meta_to_disk / clean_meta below so an old binary never clobbers
        // information it does not understand. See meta::SUPPORTED_FORMAT_VERSION.
        // UnsupportedVersion is distinct from Corruption: the data is well-formed,
        // this binary is simply too old to honor it.
        if m.format_version > meta::SUPPORTED_FORMAT_VERSION {
            return Err(StorageError::UnsupportedVersion {
                found: m.format_version,
                supported: meta::SUPPORTED_FORMAT_VERSION,
            });
        }

        let activated = Arc::new(Activated {
            epoch,
            stores: tokio::sync::RwLock::new(HashMap::new()),
            state: RwLock::new(MetaState::from_meta(m.clone())),
            meta_lock: tokio::sync::Mutex::new(()),
            object_store: self.inner.object_store.clone(),
            cache_provider: self.inner.cache_provider.clone(),
            runtime: self.inner.runtime.clone(),
            db_cache: tokio::sync::OnceCell::new(),
            compactor_poll_interval: *self.inner.compactor_poll_interval.lock(),
        });
        self.inner
            .activated
            .set(activated)
            .map_err(|_| StorageError::Internal("activate() called more than once".into()))?;

        let act = self.activated()?;
        act.write_meta_to_disk(&m).await?;
        act.clean_meta().await
    }

    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.inner.object_store
    }

    /// Get or open a `DbReader` pinned to the given checkpoint, returning a
    /// cached `Arc<DbReader>`. Used by the snapshot-read surface.
    pub(crate) async fn snapshot_reader(
        &self,
        folder_sk: &FolderStorageKey,
        id: Uuid,
    ) -> Result<Arc<DbReader>, crate::snapshot::SnapshotError> {
        let key = (folder_sk.clone(), id);

        {
            let guard = self.inner.readers.lock();
            if let Some(reader) = guard.get(&key) {
                return Ok(reader.clone());
            }
        }

        let db_cache = if let Ok(act) = self.activated() {
            Some(act.get_shared_db_cache().await.map_err(|e| {
                crate::snapshot::SnapshotError::Io(format!("get shared db cache: {e}"))
            })?)
        } else {
            None
        };

        let mut builder =
            DbReaderBuilder::new(folder_sk.to_string(), self.inner.object_store.clone())
                .with_checkpoint_id(id)
                .with_filter_policies(crate::store_keys::make_filter_policies());
        if let Some(cache) = db_cache {
            builder = builder.with_db_cache(cache);
        }

        let new_reader = builder
            .build()
            .await
            .map_err(|e| crate::snapshot::SnapshotError::Io(format!("open DbReader: {e}")))?;

        // Double-check after reacquiring the lock: another task may have
        // installed a reader for the same key while we were opening ours.
        let (reader, to_close) = {
            let mut guard = self.inner.readers.lock();
            if let Some(existing) = guard.get(&key) {
                (existing.clone(), Some(new_reader))
            } else {
                let reader = Arc::new(new_reader);
                guard.insert(key, reader.clone());
                (reader, None)
            }
        };

        if let Some(r) = to_close {
            // We lost the race. Close our reader so its checkpoint pin and
            // background tasks are released — `DbReader` has no `Drop`.
            let _ = r.close().await;
        }
        Ok(reader)
    }

    /// Parse an already-fetched object store result into Meta.
    async fn parse_meta_result(result: object_store::GetResult) -> Result<Meta, StorageError> {
        let bytes = result
            .bytes()
            .await
            .map_err(|e| StorageError::TransientIo(format!("read meta: {e}")))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| StorageError::Corruption(format!("meta not valid UTF-8: {e}")))?;
        toml::from_str(text).map_err(|e| StorageError::Corruption(format!("parse meta TOML: {e}")))
    }

    /// Read the meta file at a specific path. Returns `Meta::default()` on not-found.
    async fn read_meta_at(
        object_store: &Arc<dyn ObjectStore>,
        path: &object_store::path::Path,
    ) -> Result<Meta, StorageError> {
        match object_store.get(path).await {
            Ok(result) => Self::parse_meta_result(result).await,
            Err(object_store::Error::NotFound { .. }) => Ok(Meta::default()),
            Err(e) => Err(StorageError::TransientIo(format!("read meta: {e}"))),
        }
    }

    /// Read `bepository-{epoch}.toml` at the current epoch. Requires `activate()`.
    ///
    /// Returns `Meta::default()` if the file doesn't exist yet. The caller
    /// must hold the distributed lock.
    pub fn read_meta(&self) -> Result<Meta, StorageError> {
        Ok(self.activated()?.read_meta())
    }

    /// Read meta without holding a lock, for read-only commands like `get-id`.
    ///
    /// Lists `bepository-*.toml` files and takes the lexicographic max.
    pub async fn read_meta_unlocked(&self) -> Result<Meta, StorageError> {
        let listing = self
            .inner
            .object_store
            .list_with_delimiter(None)
            .await
            .map_err(|e| StorageError::TransientIo(format!("list meta files: {e}")))?;
        let mut last: Option<&object_store::ObjectMeta> = None;
        for obj in &listing.objects {
            if let Some(name) = obj.location.filename()
                && name.starts_with(meta::META_PREFIX)
                && name.ends_with(meta::META_SUFFIX)
            {
                last = Some(match last {
                    Some(prev) if prev.location.as_ref() > obj.location.as_ref() => prev,
                    _ => obj,
                });
            }
        }
        let meta = match last {
            Some(obj) => Self::read_meta_at(&self.inner.object_store, &obj.location).await?,
            None => Meta::default(),
        };
        // Lock-free readers (get-id, checkpoint list) only warn on a newer
        // format version rather than failing — activation is the hard fence.
        if meta.format_version > meta::SUPPORTED_FORMAT_VERSION {
            tracing::warn!(
                found = meta.format_version,
                supported = meta::SUPPORTED_FORMAT_VERSION,
                "store was written by a newer bepository format version; \
                 upgrade this instance before writing to it"
            );
        }
        Ok(meta)
    }

    /// Atomically read-modify-write the meta TOML file.
    ///
    /// This is the **only** code path that should mutate persistent meta.
    /// It holds `meta_lock` for the entire cycle so concurrent callers
    /// cannot clobber each other's fields.
    ///
    /// The closure receives a mutable `Meta`, applies its changes, and
    /// returns an arbitrary value that is forwarded to the caller.
    async fn modify_meta<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&mut Meta) -> Result<T, StorageError>,
    {
        self.activated()?.modify_meta(f).await
    }

    /// Delete `bepository-*.toml` files from prior epochs.
    pub async fn clean_meta(&self) -> Result<(), StorageError> {
        self.activated()?.clean_meta().await
    }

    /// Get or open the FolderStore for a specific folder.
    async fn store_for_folder(&self, folder: &str) -> Result<Arc<FolderStore>, StorageError> {
        self.activated()?.store_for_folder(folder).await
    }

    /// Close all open folder databases and the meta database.
    ///
    /// `slatedb::Db` has no `Drop` impl, so without an explicit close the
    /// background compactor/flusher tasks are abandoned and unflushed writes
    /// are lost. Call this before the process exits.
    #[tracing::instrument(level = "info", skip_all, err)]
    pub async fn close(&self) -> Result<(), StorageError> {
        let mut folders_closed_count = 0;
        let mut first_error = None;

        if let Some(act) = self.inner.activated.get() {
            let mut stores = act.stores.write().await;
            for (_folder, store) in stores.drain() {
                if let Err(e) = store.close().await
                    && first_error.is_none()
                {
                    first_error = Some(e);
                }
                folders_closed_count += 1;
            }
        }

        // Close cached snapshot readers (best-effort; errors are ignored since
        // DbReader is read-only and has no in-flight writes to flush).
        let readers = std::mem::take(&mut *self.inner.readers.lock());
        for (_, reader) in readers {
            if let Ok(reader) = Arc::try_unwrap(reader) {
                let _ = reader.close().await;
            }
        }

        tracing::info!(folders_closed_count, "storage closed");

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Delete stale inbox entries from all registered folder stores.
    ///
    /// Must be called after `activate()` on startup, before processing any
    /// index updates. Removes inbox entries from previous epochs (crashed or
    /// preempted processes).
    ///
    /// Errors from listing folders or opening a store are fatal — they
    /// indicate a corrupted meta DB or storage, and continuing without GC
    /// could leave stale inbox entries that interfere with sync.
    pub async fn gc_inbox(&self) -> Result<(), StorageError> {
        let act = self.activated()?;
        let folders = act.list_folders()?;
        for (id, _, sk) in &folders {
            let store = act.store_for_folder(sk).await?;
            let n = store.gc_inbox(act.epoch).await?;
            if n > 0 {
                tracing::info!(
                    folder = id.as_str(),
                    removed = n,
                    "inbox GC cleaned stale entries"
                );
            }
        }
        Ok(())
    }

    /// Run the FSCK integrity checks on all registered folders.
    pub fn check_integrity(
        &self,
        level: crate::FsckLevel,
    ) -> impl Stream<Item = Result<FsckEvent, StorageError>> + Send + '_ {
        let this = self.clone();
        stream! {
            let folders = match this.list_folders() {
                Ok(f) => f,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            for (id, _, sk) in folders {
                yield Ok(FsckEvent::FolderStarted { id });
                let store = match this.store_for_folder(&sk).await {
                    Ok(s) => s,
                    Err(e) => {
                        yield Err(e);
                        continue;
                    }
                };
                let errors = match crate::fsck::check_folder_integrity(&store, level).await {
                    Ok(e) => e,
                    Err(e) => {
                        yield Err(e);
                        continue;
                    }
                };

                let errors_found = errors.len();
                for err in errors {
                    yield Ok(FsckEvent::FolderError {
                        id,
                        error: err,
                    });
                }
                yield Ok(FsckEvent::FolderFinished {
                    id,
                    errors_found,
                });
            }
        }
    }

    /// Trigger compaction GC on a folder's SlateDB instance.
    ///
    /// Requires `activate()` — the GC filter needs the current epoch to
    /// distinguish live inbox entries from stale ones.
    ///
    /// Snapshots the manifest via the admin handle and submits one
    /// full-merge spec per non-empty segment to the running compactor, then
    /// waits until those source SSTs/SRs have been consumed. Every key in
    /// each submitted spec passes through the GC filter, physically
    /// reclaiming orphaned blocks.
    #[tracing::instrument(level = "info", skip_all, fields(folder_id = %folder_id))]
    pub async fn compact(&self, folder_id: FolderId) -> Result<(), StorageError> {
        self.activated()?.compact(folder_id).await
    }

    /// Insert a file into the index (for test setup, mirrors MemoryStorage::insert_file).
    ///
    /// Auto-registers the folder if not already present.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn insert_file(&self, folder: FolderId, file: FileInfo) {
        let act = self.activated().expect("activate() first");
        if act.resolve_folder(folder).is_err() {
            let label = FolderLabel::from(format!("LabelFor{folder}"));
            act.register_folder(folder, &label)
                .await
                .expect("register folder for test");
        }
        let f = self.folder(folder).await.expect("open folder");
        <SlateFolder as StorageInspectorForTests>::insert_file(&f, file).await;
    }

    /// Retrieve the TLS identity. Requires `activate()`.
    /// The private key is wrapped in `SecretSlice<u8>` for safe handling.
    pub fn get_identity(&self) -> Result<Option<Identity>, StorageError> {
        self.activated()?.get_identity()
    }

    /// Retrieve the TLS identity without holding a lock.
    /// The private key is wrapped in `SecretSlice<u8>` for safe handling.
    pub async fn get_identity_unlocked(&self) -> Result<Option<Identity>, StorageError> {
        let m = self.read_meta_unlocked().await?;
        Self::extract_identity(&m)
    }

    fn extract_identity(m: &Meta) -> Result<Option<Identity>, StorageError> {
        match m.identity.as_ref() {
            Some(id) => {
                let cert = id
                    .cert_der_bytes()
                    .map_err(|e| StorageError::Corruption(format!("decode cert base64: {e}")))?;
                let key = id
                    .key_der_bytes()
                    .map_err(|e| StorageError::Corruption(format!("decode key base64: {e}")))?;

                let identity = Identity::from_der(cert, key)
                    .map_err(|e| StorageError::Corruption(format!("invalid identity: {e}")))?;
                Ok(Some(identity))
            }
            None => Ok(None),
        }
    }

    /// Store the TLS identity.
    pub async fn put_identity(
        &self,
        cert_der: &[u8],
        key_der: &SecretSlice<u8>,
    ) -> Result<(), StorageError> {
        let identity = MetaIdentity::from_der(cert_der, key_der);
        self.modify_meta(|m| {
            m.identity = Some(identity);
            Ok(())
        })
        .await
    }

    /// Compute the storage identifier for a folder from its base32 key.
    fn folder_storage_key(key: &str) -> FolderStorageKey {
        FolderStorageKey(format!("folder_{key}"))
    }

    /// Register a new folder by BEP ID and label. Returns the directory name (e.g. `folder_00000001`).
    ///
    /// Errors if a folder with this BEP ID is already registered.
    pub async fn register_folder(
        &self,
        id: FolderId,
        label: &FolderLabelRef,
    ) -> Result<FolderStorageKey, StorageError> {
        let sk = self.activated()?.register_folder(id, label).await?;
        tracing::info!(folder_id = %id, folder_label = %label, "folder registered");
        Ok(sk)
    }

    /// List all registered folders and their storage identifiers.
    pub fn list_folders(
        &self,
    ) -> Result<Vec<(FolderId, FolderLabel, FolderStorageKey)>, StorageError> {
        self.activated()?.list_folders()
    }

    /// Permanently remove a folder from storage.
    ///
    /// This deletes all object store keys under the folder's prefix and
    /// removes it from the metadata registry.
    pub async fn remove_folder(&self, id: FolderId) -> Result<(), StorageError> {
        let act = self.activated()?;
        let (sk, _) = act.resolve_folder(id)?;

        // Close and evict any cached store handle to stop background tasks.
        {
            let mut stores = act.stores.write().await;
            if let Some(store) = stores.remove(sk.as_str()) {
                store.close().await?;
            }
        }

        // Recursively delete all objects under the folder's prefix.
        let prefix = object_store::path::Path::from(sk.as_str());
        let mut objects = act.object_store.list(Some(&prefix));
        while let Some(obj) = objects.next().await {
            let obj = obj.map_err(|e| {
                StorageError::TransientIo(format!("list objects for deletion: {e}"))
            })?;
            act.object_store.delete(&obj.location).await.map_err(|e| {
                StorageError::TransientIo(format!("delete object {}: {e}", obj.location))
            })?;
        }

        // Remove the entry from the metadata registry.
        self.modify_meta(|m| {
            let key = m
                .folders
                .iter()
                .find(|(_, entry)| entry.id == id)
                .map(|(k, _)| k.clone());
            if let Some(k) = key {
                m.folders.remove(&k);
            }
            Ok(())
        })
        .await?;

        Ok(())
    }

    /// Populate checkpoint schedules with defaults if none are configured.
    ///
    /// Idempotent — only writes if the checkpoint map is empty.
    /// Requires `activate()`.
    pub async fn set_default_checkpoints(&self) -> Result<(), StorageError> {
        self.modify_meta(|m| {
            if m.checkpoint.is_empty() {
                m.checkpoint = Meta::default_checkpoints();
            }
            Ok(())
        })
        .await
    }

    /// Add or update a checkpoint schedule entry.
    ///
    /// Pass `None` to remove the entry. Requires `activate()`.
    pub async fn update_checkpoint_schedule(
        &self,
        interval: Duration,
        schedule: Option<CheckpointSchedule>,
    ) -> Result<(), StorageError> {
        self.modify_meta(|m| {
            match schedule {
                Some(s) => {
                    m.checkpoint.insert(interval, s);
                }
                None => {
                    m.checkpoint.remove(&interval);
                }
            }
            Ok(())
        })
        .await
    }

    /// Create a checkpoint on every open folder DB with the given name and TTL.
    ///
    /// Uses `CheckpointScope::All` to flush memtables first. Requires `activate()`.
    #[tracing::instrument(level = "info", skip_all, fields(interval_secs = interval.as_secs()), err)]
    pub async fn create_checkpoints(
        &self,
        interval: Duration,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let name = humantime::format_duration(interval).to_string();
        let opts = CheckpointOptions {
            lifetime: Some(ttl),
            name: Some(name.clone()),
            ..Default::default()
        };
        let folders = self.list_folders()?;
        let folders_count = folders.len();
        for (_, _, sk) in folders {
            self.store_for_folder(&sk)
                .await?
                .db
                .create_checkpoint(CheckpointScope::All, &opts)
                .await
                .map_err(|e| {
                    StorageError::TransientIo(format!(
                        "create checkpoint '{name}' for folder '{sk}': {e}"
                    ))
                })?;
        }
        tracing::info!(folders_count, "checkpoints created");
        Ok(())
    }

    /// Refresh the TTL of all existing checkpoints with the given name.
    ///
    /// Used when the schedule TTL is changed — existing checkpoints are updated
    /// to match the new TTL. Requires `activate()` (writes the SlateDB manifest).
    pub async fn refresh_checkpoints(
        &self,
        interval: Duration,
        new_ttl: Duration,
    ) -> Result<(), StorageError> {
        let name = humantime::format_duration(interval).to_string();
        for (_, _, sk) in self.list_folders()? {
            let admin = AdminBuilder::new(sk.to_string(), self.inner.object_store.clone()).build();
            let checkpoints = admin.list_checkpoints(Some(&name)).await.map_err(|e| {
                StorageError::TransientIo(format!("list checkpoints for folder '{sk}': {e}"))
            })?;
            for cp in checkpoints {
                admin
                    .refresh_checkpoint(cp.id, Some(new_ttl))
                    .await
                    .map_err(|e| {
                        StorageError::TransientIo(format!(
                            "refresh checkpoint {} for folder '{}': {e}",
                            cp.id, sk
                        ))
                    })?;
            }
        }
        Ok(())
    }

    /// List all checkpoint schedules and per-folder checkpoints without a lock.
    ///
    /// Returns the schedule map from meta and a list of `(label, dir, checkpoints)`.
    pub async fn list_checkpoints_unlocked(
        &self,
    ) -> Result<
        (
            BTreeMap<Duration, CheckpointSchedule>,
            Vec<(FolderLabel, FolderStorageKey, Vec<Checkpoint>)>,
        ),
        StorageError,
    > {
        let m = self.read_meta_unlocked().await?;
        let mut folder_checkpoints = Vec::new();
        for (key, entry) in &m.folders {
            let sk = Self::folder_storage_key(key);
            let admin = AdminBuilder::new(sk.to_string(), self.inner.object_store.clone()).build();
            let checkpoints = admin.list_checkpoints(None).await.map_err(|e| {
                StorageError::TransientIo(format!("list checkpoints for folder '{sk}': {e}"))
            })?;
            folder_checkpoints.push((entry.label.clone(), sk, checkpoints));
        }
        folder_checkpoints.sort_by(|a, b| a.0.cmp(&b.0));
        Ok((m.checkpoint, folder_checkpoints))
    }

    /// Return the ages of the most recent checkpoints for all known intervals.
    ///
    /// Performs one scan per folder to build the age map.
    pub async fn list_all_checkpoint_ages(
        &self,
    ) -> Result<HashMap<Duration, Duration>, StorageError> {
        let mut latest_timestamps: HashMap<Duration, chrono::DateTime<chrono::Utc>> =
            HashMap::new();
        let folders = self.list_folders_unlocked().await?;

        for (_, _, sk) in folders {
            let admin = AdminBuilder::new(sk.to_string(), self.inner.object_store.clone()).build();
            let checkpoints = admin.list_checkpoints(None).await.map_err(|e| {
                StorageError::TransientIo(format!("list checkpoints for folder '{sk}': {e}"))
            })?;

            for cp in checkpoints {
                if let Some(name) = cp.name.as_deref()
                    && let Ok(interval) = humantime::parse_duration(name)
                {
                    latest_timestamps
                        .entry(interval)
                        .and_modify(|t| *t = (*t).max(cp.create_time))
                        .or_insert(cp.create_time);
                }
            }
        }

        let now = chrono::Utc::now();
        Ok(latest_timestamps
            .into_iter()
            .filter_map(|(interval, time)| {
                now.signed_duration_since(time)
                    .to_std()
                    .ok()
                    .map(|age| (interval, age))
            })
            .collect())
    }

    /// Return the age of the most recent checkpoint with the given name across all folders.
    ///
    /// Returns `None` if no checkpoint with that name exists. Uses detached Admin
    /// so the DB need not be open and no lock is required.
    pub async fn most_recent_checkpoint_age(
        &self,
        interval: Duration,
    ) -> Result<Option<Duration>, StorageError> {
        let ages = self.list_all_checkpoint_ages().await?;
        Ok(ages.get(&interval).copied())
    }

    /// List folders without requiring the lock (for use in admin operations).
    pub async fn list_folders_unlocked(
        &self,
    ) -> Result<Vec<(FolderId, FolderLabel, FolderStorageKey)>, StorageError> {
        Ok(folder_triples_from_meta(&self.read_meta_unlocked().await?))
    }

    /// Insert a block (for test setup, mirrors MemoryStorage::insert_block).
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn insert_block(&self, folder: FolderId, name: &str, offset: i64, data: Bytes) {
        let f = self.folder(folder).await.expect("open folder");
        <SlateFolder as StorageInspectorForTests>::insert_block(&f, name, offset, data).await;
    }

    /// Get a file from the index (for test assertions).
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn get_file(&self, folder: FolderId, name: &str) -> Option<FileInfo> {
        let f = self.folder(folder).await.ok()?;
        <SlateFolder as StorageInspectorForTests>::get_file(&f, name).await
    }
}

/// Build the (id, label, storage key) tuple list directly from a `Meta` value.
/// Used wherever no `MetaState`/registry is available (e.g. unlocked reads).
fn folder_triples_from_meta(
    m: &crate::meta::Meta,
) -> Vec<(FolderId, FolderLabel, FolderStorageKey)> {
    let mut result: Vec<_> = m
        .folders
        .iter()
        .map(|(key, entry)| {
            (
                entry.id,
                entry.label.clone(),
                SlateStorage::folder_storage_key(key),
            )
        })
        .collect();
    result.sort_by_key(|(id, _, _)| *id);
    result
}

impl Activated {
    fn meta_path(&self) -> object_store::path::Path {
        let filename = format!(
            "{}{}{}",
            meta::META_PREFIX,
            self.epoch.as_base32(),
            meta::META_SUFFIX
        );
        object_store::path::Path::from(filename)
    }

    fn read_meta(&self) -> Meta {
        self.state.read().meta.clone()
    }

    async fn write_meta_to_disk(&self, m: &Meta) -> Result<(), StorageError> {
        let path = self.meta_path();
        // Enforce the supported format version centrally so no caller can
        // forget to set it. The clone is cheap (meta is small).
        let mut m = m.clone();
        m.format_version = meta::SUPPORTED_FORMAT_VERSION;
        let content = toml::to_string_pretty(&m)
            .map_err(|e| StorageError::Internal(format!("serialize meta: {e}")))?;
        self.object_store
            .put(&path, content.into())
            .await
            .map_err(|e| StorageError::TransientIo(format!("write meta: {e}")))?;
        Ok(())
    }

    async fn modify_meta<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&mut Meta) -> Result<T, StorageError>,
    {
        let _guard = self.meta_lock.lock().await;
        // Start from the cached value. Under single-writer (distributed lock),
        // this is identical to re-reading from disk.
        let mut new_meta = self.state.read().meta.clone();
        let result = f(&mut new_meta)?;
        // Commit to disk first; on failure the cache is untouched.
        self.write_meta_to_disk(&new_meta).await?;
        // Swap in-memory. This is the only place `state` is mutated.
        *self.state.write() = MetaState::from_meta(new_meta);
        Ok(result)
    }

    async fn clean_meta(&self) -> Result<(), StorageError> {
        let current_path = self.meta_path();
        let listing = self
            .object_store
            .list_with_delimiter(None)
            .await
            .map_err(|e| StorageError::TransientIo(format!("list meta files: {e}")))?;
        let mut deleted = 0;
        for obj in listing.objects {
            if obj.location != current_path
                && let Some(name) = obj.location.filename()
                && name.starts_with(meta::META_PREFIX)
                && name.ends_with(meta::META_SUFFIX)
            {
                self.object_store.delete(&obj.location).await.map_err(|e| {
                    StorageError::TransientIo(format!("delete old meta {}: {e}", obj.location))
                })?;
                deleted += 1;
            }
        }
        tracing::debug!(deleted = deleted, "cleaned old meta files");
        Ok(())
    }

    fn get_base_cache_dir(&self) -> Result<Option<PathBuf>, StorageError> {
        let Some(provider) = self.cache_provider.as_ref() else {
            return Ok(None);
        };

        let identity = self
            .get_identity()?
            .ok_or_else(|| StorageError::Internal("No identity found in storage".into()))?;
        let device_id = *identity.device_id();
        Ok(provider.get_cache_dir(&device_id))
    }

    async fn get_shared_db_cache(&self) -> Result<Arc<dyn DbCache>, StorageError> {
        self.db_cache
            .get_or_try_init(|| async {
                let dir = self.get_base_cache_dir()?;
                // Each folder used to have its own cache under `dir/folder_sk`.
                // We now share a single cache at `dir` to save file descriptors.
                // Old per-folder directories are currently orphaned on upgrade.
                Ok(make_block_cache(dir).await)
            })
            .await
            .cloned()
    }

    /// Open a SlateDB instance for `folder_sk` with the standard cold-archive
    /// configuration. Returns the wrapped `FolderStore`. Does **not** insert
    /// into the cache — callers decide the lifetime.
    async fn open_folder_store(&self, folder_sk: &str) -> Result<Arc<FolderStore>, StorageError> {
        let path = folder_sk.to_string();
        let db_cache = self.get_shared_db_cache().await?;

        let gc = Arc::new(CompactionState::new());
        let store_slot = Arc::new(StoreSlot::new());

        let supplier = Arc::new(GcFilterSupplier::new(
            path.clone(),
            store_slot.clone(),
            gc.clone(),
            self.epoch,
        ));

        let mut settings = make_db_settings();
        if let Some(poll) = self.compactor_poll_interval
            && let Some(c) = settings.compactor_options.as_mut()
        {
            c.poll_interval = poll;
        }

        // `l0_max_ssts` is the per-segment ceiling at which the flusher
        // refuses to dispatch the next immutable memtable. For the `b`
        // segment under sustained sync on a slow uplink the saturation
        // math is:
        //
        //   * a size-tiered compaction job for `b` triggers at
        //     `min_compaction_sources` (32) L0 SSTs and pins those 32
        //     source SSTs in `manifest.l0()` until the manifest commit;
        //   * the job rewrites ~32 × `l0_sst_size_bytes` ≈ 256 MiB; on a
        //     ~2.5 MB/s effective uplink that is ~100 s wall-clock;
        //   * during the in-flight window new memtable freezes keep
        //     landing one L0 SST per freeze into `b`. With 8 MiB
        //     freezes and ~2.5 MB/s upload the freeze cadence is also
        //     ~3 s, so ~32 new SSTs arrive before the sources are
        //     released.
        //
        // So under sustained sync `b`'s effective L0 grows to roughly
        // `2 * min_compaction_sources` (= 64) before the in-flight job
        // commits. If `l0_max_ssts` is tighter than that, every cycle
        // the flusher's per-segment `can_dispatch` refuses new imms;
        // writes stall on `max_unflushed_bytes` backpressure until the
        // commit lands. (Note: the debug_assert! below is compiled out in release,
        // so it's not a runtime guard).
        if let Some(opts) = settings.compactor_options.as_ref() {
            let scheduler = slatedb::config::SizeTieredCompactionSchedulerOptions::from(
                &opts.scheduler_options,
            );
            debug_assert!(
                settings.l0_max_ssts >= 2 * scheduler.min_compaction_sources,
                "l0_max_ssts ({}) must be >= 2 * min_compaction_sources ({}): a bd \
                 compaction pins its source SSTs until commit while new L0 SSTs \
                 continue to land",
                settings.l0_max_ssts,
                scheduler.min_compaction_sources,
            );
        }

        let mut compactor_builder = CompactorBuilder::new(path.clone(), self.object_store.clone())
            .with_runtime(self.runtime.clone())
            .with_compaction_filter_supplier(supplier)
            .with_scheduler_supplier(Arc::new(
                crate::compaction::BepCompactionSchedulerSupplier::new(store_slot.clone()),
            ))
            .with_filter_policies(crate::store_keys::make_filter_policies());
        if let Some(ref opts) = settings.compactor_options {
            compactor_builder = compactor_builder.with_options(opts.clone());
        }

        let db = Db::builder(path.clone(), self.object_store.clone())
            .with_gc_runtime(self.runtime.clone())
            .with_db_cache(db_cache)
            .with_settings(settings)
            .with_segment_extractor(Arc::new(crate::store_keys::BepSegmentExtractor))
            .with_filter_policies(crate::store_keys::make_filter_policies())
            .with_compactor_builder(compactor_builder)
            .build()
            .await
            .map_err(|e| StorageError::TransientIo(format!("open slatedb: {e}")))?;

        let store = Arc::new(FolderStore::new(db, gc).await?);
        store_slot.set(store.clone());
        Ok(store)
    }

    async fn store_for_folder(&self, folder: &str) -> Result<Arc<FolderStore>, StorageError> {
        {
            let stores = self.stores.read().await;
            if let Some(store) = stores.get(folder) {
                return Ok(store.clone());
            }
        }
        let mut stores = self.stores.write().await;
        if let Some(store) = stores.get(folder) {
            return Ok(store.clone());
        }

        let store = self.open_folder_store(folder).await?;
        tracing::debug!(folder_id = %folder, "opened SlateDB");
        stores.insert(folder.to_string(), store.clone());
        Ok(store)
    }

    async fn register_folder(
        &self,
        id: FolderId,
        label: &FolderLabelRef,
    ) -> Result<FolderStorageKey, StorageError> {
        let sk = self
            .modify_meta(|m| {
                if m.has_folder(id) {
                    return Err(StorageError::InvalidInput(format!(
                        "folder '{id}' already registered"
                    )));
                }
                let key = meta::folder_key(m.next_folder_key);
                m.next_folder_key += 1;
                m.folders.insert(
                    key.clone(),
                    FolderEntry {
                        id,
                        label: label.to_owned(),
                    },
                );
                Ok(SlateStorage::folder_storage_key(&key))
            })
            .await?;

        // Open the folder's SlateDB instance to ensure the prefix is created.
        let _store = self.store_for_folder(&sk).await?;
        Ok(sk)
    }

    fn list_folders(&self) -> Result<Vec<(FolderId, FolderLabel, FolderStorageKey)>, StorageError> {
        let state = self.state.read();
        let mut result: Vec<_> = state
            .registry
            .iter()
            .map(|(id, (sk, label))| (*id, label.clone(), sk.clone()))
            .collect();
        result.sort_by_key(|(id, _, _)| *id);
        Ok(result)
    }

    fn get_identity(&self) -> Result<Option<Identity>, StorageError> {
        let state = self.state.read();
        SlateStorage::extract_identity(&state.meta)
    }

    fn resolve_folder(
        &self,
        id: FolderId,
    ) -> Result<(FolderStorageKey, FolderLabel), StorageError> {
        let state = self.state.read();
        state
            .registry
            .get(&id)
            .map(|(sk, label)| (sk.clone(), label.clone()))
            .ok_or_else(|| StorageError::NotFound(format!("folder '{id}' not found in registry")))
    }

    async fn compact(&self, folder_id: FolderId) -> Result<(), StorageError> {
        let (sk, _) = self.resolve_folder(folder_id)?;
        // Ensure the DB is open so the background compactor is running and
        // will pick up the jobs we submit.
        let store = self.store_for_folder(sk.as_str()).await?;
        // Flush the active memtable to L0 so recent writes are part of the
        // manifest snapshot we build specs from. Without this, anything still
        // resident in the memtable is invisible to the compactor.
        store
            .db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .map_err(|e| StorageError::TransientIo(format!("flush before compaction: {e}")))?;
        let admin = AdminBuilder::new(sk.to_string(), self.object_store.clone()).build();

        // Submit one spec at a time and wait for its commit before building
        // the next. Two reasons:
        //
        // 1. Size-tiered's validator rejects a pure-L0 spec whose destination
        //    SR id is `< max(committed SR ids) + 1`. With multiple in-flight
        //    pure-L0 specs the compactor may process them in any order; once
        //    the first commits, the others' destinations become stale and
        //    fail validation. Serializing keeps each spec's destination
        //    correct at the time it is validated.
        //
        // 2. `max_concurrent_compactions = 1` already serializes execution,
        //    so we lose nothing by serializing submission too.
        let timeout = Duration::from_secs(3600);
        let start = std::time::Instant::now();
        loop {
            let view = admin
                .read_compactor_state_view()
                .await
                .map_err(|e| StorageError::TransientIo(format!("read compactor state: {e}")))?;
            let manifest = view.manifest();

            // Skip the default (unsegmented) tree: with our segment extractor
            // every row routes into a named segment, so the default tree is
            // empty by construction.
            let next_segment = manifest
                .segments()
                .iter()
                .find(|s| !s.l0().is_empty() || s.compacted().len() > 1);
            let Some(segment) = next_segment else {
                // Force the writer to pick up the manifest changes we just
                // committed. Without this, reads via the writer's Db handle
                // would observe stale L0 SSTs until the next periodic
                // manifest refresh (`manifest_poll_interval`).
                store
                    .db
                    .refresh_manifest()
                    .await
                    .map_err(|e| StorageError::TransientIo(format!("refresh manifest: {e}")))?;
                return Ok(());
            };

            // Source order required by size-tiered's validator:
            // L0 newest → oldest (the VecDeque's natural iteration), then
            // SRs highest id → lowest (the Vec's natural iteration).
            let mut pending_l0: HashSet<u128> = HashSet::new();
            let mut pending_sr: HashSet<u32> = HashSet::new();
            let mut sources: Vec<SourceId> = segment
                .l0()
                .iter()
                .map(|sst| {
                    pending_l0.insert(u128::from(sst.id));
                    SourceId::SstView(sst.id)
                })
                .collect();
            let min_sr = segment.compacted().iter().map(|sr| sr.id).min();
            for run in segment.compacted() {
                sources.push(SourceId::SortedRun(run.id));
                if Some(run.id) != min_sr {
                    pending_sr.insert(run.id);
                }
            }
            // Specs that include any SR must target the lowest source SR
            // (size-tiered's validate enforces this — the destination is
            // overwritten in place). Pure-L0 specs allocate a fresh id one
            // above every committed SR across all segments.
            let destination = match min_sr {
                Some(id) => id,
                None => manifest
                    .segments()
                    .iter()
                    .flat_map(|s| s.compacted().iter().map(|sr| sr.id))
                    .max()
                    .map_or(0, |id| id.saturating_add(1)),
            };
            let spec = CompactionSpec::for_segment(segment.prefix().clone(), sources, destination);
            admin
                .submit_compaction(spec)
                .await
                .map_err(|e| StorageError::TransientIo(format!("submit compaction: {e}")))?;

            // Wait until this spec commits. L0 source SSTs always vanish
            // from the manifest after commit; source SR ids other than the
            // destination also vanish. SlateDB does not yet expose a
            // per-job completion signal, so the poll is a stopgap.
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let view = admin
                    .read_compactor_state_view()
                    .await
                    .map_err(|e| StorageError::TransientIo(format!("read compactor state: {e}")))?;
                let still_pending = view.manifest().segments().iter().any(|seg| {
                    seg.l0()
                        .iter()
                        .any(|sst| pending_l0.contains(&u128::from(sst.id)))
                        || seg.compacted().iter().any(|sr| pending_sr.contains(&sr.id))
                });
                if !still_pending {
                    break;
                }
                if start.elapsed() > timeout {
                    return Err(StorageError::TransientIo(
                        "compaction timed out after 1h".into(),
                    ));
                }
            }
        }
    }
}

#[async_trait]
impl Storage for SlateStorage {
    type Folder = SlateFolder;
    type StorageKey = FolderStorageKey;

    async fn folder(&self, id: FolderId) -> Result<SlateFolder, StorageError> {
        let act = self.activated()?;
        let (sk, label) = act.resolve_folder(id)?;
        let store = act.store_for_folder(&sk).await?;
        Ok(SlateFolder {
            id,
            label,
            store,
            epoch: Some(act.epoch),
        })
    }

    async fn list_folders(
        &self,
    ) -> Result<Vec<(FolderId, FolderLabel, FolderStorageKey)>, StorageError> {
        self.activated()?.list_folders()
    }

    async fn ensure_folders(
        &self,
        folders: &[(FolderId, FolderLabel)],
    ) -> Result<Vec<bool>, StorageError> {
        if folders.is_empty() {
            return Ok(Vec::new());
        }

        let act = self.activated()?;

        // Single modify_meta call: register new folders and update labels for existing ones.
        let created = act
            .modify_meta(|m| {
                let mut created = vec![false; folders.len()];
                for (i, (id, label)) in folders.iter().enumerate() {
                    if let Some(entry) = m.folders.values_mut().find(|e| e.id == *id) {
                        entry.label = label.to_owned();
                    } else {
                        let key = meta::folder_key(m.next_folder_key);
                        m.next_folder_key += 1;
                        m.folders.insert(
                            key,
                            FolderEntry {
                                id: *id,
                                label: label.to_owned(),
                            },
                        );
                        created[i] = true;
                    }
                }
                Ok(created)
            })
            .await?;

        // Open SlateDB instances for newly created folders (ensures prefix is created).
        for (i, (id, _)) in folders.iter().enumerate() {
            if created[i] {
                let (sk, _) = act.resolve_folder(*id)?;
                act.store_for_folder(&sk).await?;
            }
        }

        Ok(created)
    }
}

/// Per-folder handle for [`SlateStorage`].
///
/// Obtained via [`SlateStorage::folder`]. Cheaply cloneable — shares the
/// underlying [`FolderStore`] via `Arc`.
#[derive(Clone)]
pub struct SlateFolder {
    id: FolderId,
    label: FolderLabel,
    store: Arc<FolderStore>,
    epoch: Option<Epoch>,
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl StorageInspectorForTests for SlateFolder {
    type Epoch = Epoch;

    /// Insert a file directly into the index (for test setup).
    async fn insert_file(&self, file: FileInfo) {
        self.store.put_file(&file).await.expect("put file");
    }

    /// Get a file from the index (for test assertions).
    async fn get_file(&self, name: &str) -> Option<FileInfo> {
        self.store.get_file(name).await.ok().flatten()
    }

    /// Get a file from the inbox/staging area.
    async fn get_inbox_file(&self, epoch: Epoch, name: &str) -> Option<FileInfo> {
        self.store.get_inbox_file(epoch, name).await.ok().flatten()
    }

    /// Insert a block into the index (for test setup).
    async fn insert_block(&self, name: &str, offset: i64, data: Bytes) {
        // Look up the file to find the block hash matching this offset.
        let mut file_info = None;
        if let Some(epoch) = self.epoch {
            file_info = self.store.get_inbox_file(epoch, name).await.ok().flatten();
        }
        if file_info.is_none() {
            file_info = self.store.get_file(name).await.ok().flatten();
        }

        if let Some(fi) = file_info {
            for block in &fi.blocks {
                if block.offset == offset && block.hash.len() == 32 {
                    self.store
                        .store_block(self.epoch, name, &block.hash, &data)
                        .await
                        .expect("store block");
                    return;
                }
            }
        }
        // Fallback: compute hash from data.
        use sha2::{Digest, Sha256};
        let hash = Sha256::digest(&data);
        self.store
            .store_block(self.epoch, name, &hash, &data)
            .await
            .expect("store block");
    }

    /// Get raw block data for a file at a specific offset.
    async fn get_block(&self, name: &str, offset: i64) -> Option<Bytes> {
        // Look up the file to find the block hash matching this offset.
        let fi = self.store.get_file(name).await.ok()??;
        for block in &fi.blocks {
            if block.offset == offset {
                return self.store.read_block(name, &block.hash).await.ok();
            }
        }
        None
    }
}

impl SlateFolder {
    /// Write raw bytes to a key in the underlying DB. For testing only.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn put_raw(&self, key: Vec<u8>, value: Vec<u8>) {
        use slatedb::config::PutOptions;
        use slatedb::config::WriteOptions;
        // `await_durable: false` — the test caller wraps this with whatever
        // durability it actually needs (close-the-store, explicit flush, or
        // compaction round-trip). Waiting for WAL durability *per put_raw
        // call* costs up to one `flush_interval` (60 s) per call against the
        // ambient `make_db_settings()`, since the tiny writes never trip the
        // size-based flush. With multiple `put_raw` calls in a row the test
        // would spend minutes parked on the WAL-flush ticker.
        let write_opts = WriteOptions {
            await_durable: false,
            ..Default::default()
        };
        self.store
            .db
            .put_with_options(key, value, &PutOptions::default(), &write_opts)
            .await
            .expect("put_raw");
    }

    /// Read raw bytes for a key from the underlying DB. For testing only.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.store
            .db
            .get(key)
            .await
            .expect("get_raw")
            .map(|b| b.to_vec())
    }

    /// Return the current epoch (test helper).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn epoch(&self) -> Option<bepository_lock::Epoch> {
        self.epoch
    }

    /// Return the underlying store (test helper).
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(dead_code)]
    pub(crate) fn store(&self) -> &Arc<FolderStore> {
        &self.store
    }

    fn require_epoch(&self) -> Result<Epoch, StorageError> {
        self.epoch.ok_or_else(|| {
            StorageError::Standby("epoch not set: call activate() before this operation".into())
        })
    }

    /// Stage `remote_file` in the inbox under the current epoch and return
    /// the `NeedBlocks` outcome. Centralizes the three call sites in `apply_update`.
    async fn stage_and_request_blocks(
        &self,
        epoch: Epoch,
        remote_file: &FileInfo,
    ) -> Result<UpdateResult, StorageError> {
        self.store.stage_file(epoch, remote_file).await?;
        Ok(UpdateResult::NeedBlocks(remote_file.clone()))
    }
}

#[async_trait]
impl StorageFolder for SlateFolder {
    fn id(&self) -> FolderId {
        self.id
    }

    fn label(&self) -> &FolderLabelRef {
        &self.label
    }

    async fn index(
        &self,
        since: Sequence,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FileInfo, StorageError>> + Send>>, StorageError>
    {
        let files = if since == Sequence::ZERO {
            self.store.all_files().await?
        } else {
            self.store.files_since(since.get()).await?
        };
        Ok(Box::pin(stream::iter(files.into_iter().map(Ok))))
    }

    async fn read_block(
        &self,
        name: &str,
        _offset: i64,
        _size: i32,
        hash: &[u8],
    ) -> Result<Bytes, StorageError> {
        self.store.read_block(name, hash).await
    }

    async fn apply_update(
        &self,
        remote_file: &FileInfo,
        _remote_device: &DeviceId,
    ) -> Result<UpdateResult, StorageError> {
        validate_file_name(&remote_file.name)?;
        let epoch = self.require_epoch()?;
        let local_file = self.store.get_file(&remote_file.name).await?;

        // No local entry: always stage and request blocks.
        let Some(local) = local_file else {
            return self.stage_and_request_blocks(epoch, remote_file).await;
        };

        // Either side missing a version vector: treat as "stage and request".
        let (Some(lv), Some(rv)) = (local.version.as_ref(), remote_file.version.as_ref()) else {
            return self.stage_and_request_blocks(epoch, remote_file).await;
        };

        match compare(lv, rv).map_err(|e| StorageError::Corruption(e.to_string()))? {
            Ordering::Greater | Ordering::Equal => Ok(UpdateResult::NoAction),
            Ordering::Concurrent => Ok(UpdateResult::Concurrent {
                local,
                remote: remote_file.clone(),
            }),
            Ordering::Less if blocks_equal(&local, remote_file) => {
                // Metadata-only change — commit directly without staging.
                let seq = self.store.put_file(remote_file).await?;
                let mut fi = remote_file.clone();
                fi.sequence = seq;
                Ok(UpdateResult::Applied(fi))
            }
            Ordering::Less => self.stage_and_request_blocks(epoch, remote_file).await,
        }
    }

    async fn complete_file(
        &self,
        name: &str,
        expected_version: Option<&bepository_bep::proto::bep::Vector>,
    ) -> Result<Option<FileInfo>, StorageError> {
        let epoch = self.require_epoch()?;
        self.store
            .complete_file(epoch, name, expected_version)
            .await
    }

    async fn resolve_conflict(
        &self,
        winner: &FileInfo,
        loser: &FileInfo,
        loser_path: Option<&str>,
    ) -> Result<(), StorageError> {
        self.store.put_file(winner).await?;
        if let Some(path) = loser_path {
            let mut loser_copy = loser.clone();
            loser_copy.name = path.to_string();
            self.store.put_file(&loser_copy).await?;
        }
        Ok(())
    }

    async fn local_sequence(&self) -> Result<Sequence, StorageError> {
        let meta = self.store.get_index_meta().await?;
        Ok(Sequence(meta.max_sequence))
    }

    async fn remote_state(
        &self,
        device: &DeviceId,
    ) -> Result<bepository_bep::storage::RemoteIndexState, StorageError> {
        let proto = self.store.get_remote_state(device).await?;
        Ok(bepository_bep::storage::RemoteIndexState {
            index_id: proto.index_id,
            max_sequence: Sequence(proto.max_sequence),
        })
    }

    async fn set_remote_state(
        &self,
        device: &DeviceId,
        state: bepository_bep::storage::RemoteIndexState,
    ) -> Result<(), StorageError> {
        let proto = crate::proto::storage::RemoteIndexState {
            index_id: state.index_id,
            max_sequence: state.max_sequence.get(),
        };
        self.store.put_remote_state(device, &proto).await?;
        Ok(())
    }

    async fn store_block(
        &self,
        name: &str,
        _offset: i64,
        hash: &[u8],
        data: Bytes,
    ) -> Result<(), StorageError> {
        self.store.store_block(self.epoch, name, hash, &data).await
    }

    async fn reuse_block(
        &self,
        name: &str,
        _offset: i64,
        hash: &[u8],
        size: i32,
    ) -> Result<bool, StorageError> {
        if size < 4096 {
            return Ok(false);
        }
        self.store.reuse_block(self.epoch, name, hash).await
    }

    async fn has_block(
        &self,
        file: &str,
        offset: i64,
        hash: &[u8],
        size: i32,
    ) -> Result<bool, StorageError> {
        let is_inline = size < 4096;
        if is_inline {
            match self.store.get_file_to_update(self.epoch, file).await {
                Ok((file_info, _, _)) => {
                    for block in &file_info.blocks {
                        if block.offset == offset && block.hash == hash {
                            return Ok(block.inline_data.is_some());
                        }
                    }
                    Ok(false)
                }
                Err(StorageError::NotFound(_)) => Ok(false),
                Err(e) => Err(e),
            }
        } else {
            self.store.has_block(hash).await
        }
    }
}

/// Creates a shared block cache for cold/archival workloads.
///
/// When `cache_dir` is `Some`, builds a Foyer hybrid cache (16 MiB memory +
/// 512 MiB disk) rooted at that directory. The disk tier persists bloom
/// filters and index blocks across process restarts, eliminating cold-start
/// round-trips on slow/metered connections.
///
/// When `cache_dir` is `None` (tests, testserver), falls back to a 16 MiB
/// in-memory-only Foyer cache — identical to the previous behavior.
///
/// NOTE: Do not combine this with `ObjectStoreCacheOptions` — per slatedb
/// docs, coexistence causes disk write amplification.
async fn make_block_cache(cache_dir: Option<PathBuf>) -> Arc<dyn DbCache> {
    if let Some(dir) = cache_dir {
        let cache = HybridCacheBuilder::new()
            .with_name("slatedb")
            .memory(16 * 1024 * 1024) // 16 MiB memory tier
            .with_weighter(|_, v: &CachedEntry| v.size())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(BlockEngineConfig::new(
                FsDeviceBuilder::new(dir)
                    .with_capacity(512 * 1024 * 1024)
                    .build()
                    .unwrap(),
            ))
            .build()
            .await
            .expect("build foyer hybrid cache");
        Arc::new(FoyerHybridCache::new_with_cache(cache))
    } else {
        Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: 16 * 1024 * 1024, // 16 MiB
            shards: 4,
        })) as Arc<dyn DbCache>
    }
}

/// Creates per-folder SlateDB settings for cold/archival workloads.
///
/// Optimized for slow (≥10 Mbps) uplinks with a 60s compactor cadence.
///
/// Background:
/// - `l0_sst_size_bytes`: Bounds foreground stall duration (stall = size / speed).
/// - `max_unflushed_bytes`: Burst capacity before back-pressure pauses the writer.
fn make_db_settings() -> Settings {
    Settings {
        // WAL flush interval. Size-based flush still fires at `l0_sst_size_bytes`.
        flush_interval: Some(Duration::from_secs(60)),
        // Memtable freeze threshold. Sized to 8 MiB.
        l0_sst_size_bytes: 8 * 1024 * 1024,
        // Memory ceiling for frozen memtables. Allows ~8 memtables in flight (64 MiB).
        max_unflushed_bytes: 64 * 1024 * 1024,
        // Manifest poll interval. Bounds the worst-case wake-up latency
        // after the in-process compactor commits a manifest update that
        // drops L0 SSTs: the flusher only learns about that drop via the
        // periodic re-read. 10 s is a conservative middle ground while we
        // think about a proper fix (see OVERVIEW.md "Caveats").
        manifest_poll_interval: Duration::from_secs(10),
        // Serializes L0 flushes to avoid bandwidth contention and timeouts on slow links.
        l0_flush_parallelism: 1,
        // Per-segment L0 ceiling at which the flusher stops dispatching imms.
        // Must clear the bd worst case of `2 * min_compaction_sources` (= 64
        // under the slow-uplink saturation math; see `open_folder_store`).
        // 512 leaves ample margin for unusual write bursts; the memory cost
        // is only the in-RAM filter+index per SST, a few MB total.
        l0_max_ssts: 512,
        compactor_options: Some(CompactorOptions {
            // Battery floor: each tick triggers a GCS manifest GET that keeps
            // the radio awake even when there is nothing to compact.
            poll_interval: Duration::from_secs(60),
            // Serial compaction: bounds CPU and network bursts. Fine for
            // cold/archival; L0 headroom above absorbs flushes that arrive
            // during a compaction cycle.
            max_concurrent_compactions: 1,
            // Bound parallel L0 fetches during compaction. Two is enough to
            // overlap download with merge work without saturating the link.
            max_fetch_tasks: 2,
            // L1+ output chunk size. Compactor uploads are background work,
            // so per-SST upload time only affects compaction wall-clock, not
            // writer latency — pick the largest size that still completes
            // each upload within a single GCS retry window on the slow link
            // (~107 s @ 10 Mbps; ~11 s @ 100 Mbps). The 256 MiB default
            // exceeds 3 min on a slow link and risks transient timeouts.
            max_sst_size: 128 * 1024 * 1024,
            // Raise size-tiered scheduler's L0 trigger from the default 4 to 32.
            // For the `b` segment, rows are written in monotonically increasing
            // seqno order, so its L0 SSTs have disjoint key ranges and reads only
            // ever consult one SST at a time — compaction buys it nothing for read
            // amplification, only for GC of orphaned rows and manifest size.
            // Compacting every 4 L0 SSTs (~32 MiB) under a sustained sync starves
            // memtable flushes; 32 fires roughly 8x less often while staying well
            // below the `l0_max_ssts` per-segment backpressure ceiling.
            // (Note: superseded by `BepCompactionScheduler` for metadata segments,
            // which do a full-merge at `METADATA_MIN_L0 = 4`).
            // `max_compaction_sources` must be >= `min_compaction_sources`,
            // otherwise `clamp_min` accepts the pick and `clamp_max` then
            // truncates it back below `min`, after which the per-tree
            // backpressure check rejects almost every proposal (estimated
            // result size shrinks until any existing sorted run trips the
            // "next SR's compactable run is full" guard). Default max is 8,
            // so the lone min=32 override left the scheduler unable to make
            // progress on the `b` segment once a few SRs had accumulated.
            scheduler_options: {
                let mut m = std::collections::HashMap::new();
                m.insert("min_compaction_sources".to_string(), "32".to_string());
                m.insert("max_compaction_sources".to_string(), "64".to_string());
                m
            },
            ..CompactorOptions::default()
        }),
        ..Default::default()
    }
}

/// Reject file names that could escape the folder namespace.
fn validate_file_name(name: &str) -> Result<(), StorageError> {
    if name.is_empty() {
        return Err(StorageError::InvalidInput("empty file name".into()));
    }
    if name.starts_with('/') {
        return Err(StorageError::InvalidInput(
            "absolute file path rejected".into(),
        ));
    }
    if name.contains('\0') {
        return Err(StorageError::InvalidInput("null byte in file name".into()));
    }
    for component in name.split('/') {
        if component.is_empty() {
            return Err(StorageError::InvalidInput(
                "empty path component rejected".into(),
            ));
        }
        if component == ".." {
            return Err(StorageError::InvalidInput("path traversal rejected".into()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_file_name() {
        assert!(validate_file_name("a/b/c.txt").is_ok());
        assert!(validate_file_name("readme.txt").is_ok());

        assert!(validate_file_name("").is_err());
        assert!(validate_file_name("/abs/path").is_err());
        assert!(validate_file_name("a/\0/b").is_err());
        assert!(validate_file_name("a/../b").is_err());

        // Should reject empty path components (//)
        assert!(validate_file_name("a//b").is_err());
        // Should reject trailing slash
        assert!(validate_file_name("a/").is_err());
        // Should reject leading slash (already handled, but split('/') also catches it)
        assert!(validate_file_name("/a").is_err());
    }
}
