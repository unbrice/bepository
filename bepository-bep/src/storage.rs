// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::Stream;
use std::pin::Pin;

use crate::device_id::DeviceId;
use crate::error::StorageError;
use crate::ids::{FolderId, FolderLabel, FolderLabelRef};
use crate::proto::bep::FileInfo;

/// The synchronization state of a remote peer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteIndexState {
    /// An opaque identifier from the peer indicating if their database was reset.
    pub index_id: u64,
    /// The highest sequence number we have seen from this peer.
    pub max_sequence: Sequence,
}

/// Returns `true` if `a` and `b` have identical block lists (same hashes and sizes).
///
/// Used to distinguish metadata-only updates (no block transfer needed) from
/// content changes.
#[must_use]
pub fn blocks_equal(a: &FileInfo, b: &FileInfo) -> bool {
    a.blocks.len() == b.blocks.len()
        && a.blocks
            .iter()
            .zip(b.blocks.iter())
            .all(|(ba, bb)| ba.hash == bb.hash && ba.size == bb.size)
}

/// A monotonically increasing sequence number assigned to index entries.
///
/// Each folder maintains its own local sequence counter. Sequence numbers
/// are used to track which index entries a peer has already seen, enabling
/// incremental index updates on reconnection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Sequence(pub i64);

impl Sequence {
    pub const ZERO: Sequence = Sequence(0);

    #[must_use]
    pub fn get(self) -> i64 {
        self.0
    }
}

impl From<i64> for Sequence {
    fn from(v: i64) -> Self {
        Self(v)
    }
}

impl From<Sequence> for i64 {
    fn from(s: Sequence) -> Self {
        s.0
    }
}

/// Outcome of applying a remote file update.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum UpdateResult {
    /// Remote version dominates or file is new locally.
    /// Engine should fetch the blocks listed in FileInfo.
    NeedBlocks(FileInfo),

    /// Local version dominates or is equal. No action needed.
    NoAction,

    /// Remote version accepted, but block content is identical to what
    /// we already have (e.g. only metadata or version vector changed).
    /// Index is updated; no block transfer needed.
    Applied(FileInfo),

    /// Concurrent edit — neither version vector dominates.
    /// The caller (engine) is responsible for resolving the conflict
    /// via a [`ConflictResolver`](crate::ConflictResolver) and persisting the outcome
    /// via [`StorageFolder::resolve_conflict`].
    Concurrent { local: FileInfo, remote: FileInfo },
}

/// Handle to a resolved folder within a storage backend.
///
/// Obtained via [`Storage::folder`]. All per-folder operations
/// (index queries, block storage, conflict resolution) go through this
/// trait. Implementations are cheaply cloneable — cloning shares the
/// underlying database handle.
#[async_trait]
pub trait StorageFolder: Clone + Send + Sync + 'static {
    /// The folder ID this handle was resolved from.
    fn id(&self) -> FolderId;

    /// The folder label this handle was resolved from.
    fn label(&self) -> &FolderLabelRef;

    /// File index entries since a given sequence number.
    ///
    /// Pass [`Sequence::ZERO`] to get the full index.
    async fn index(
        &self,
        since: Sequence,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FileInfo, StorageError>> + Send>>, StorageError>;

    /// Read a block of a file (for responding to Request messages).
    async fn read_block(
        &self,
        name: &str,
        offset: i64,
        size: i32,
        hash: &[u8],
    ) -> Result<Bytes, StorageError>;

    /// Apply a remote file update. Compares version vectors, resolves
    /// conflicts, persists the result, and returns the outcome.
    ///
    /// Implementations should compare block hashes when the remote version
    /// dominates: if all blocks match the local copy, return
    /// [`UpdateResult::Applied`] instead of [`UpdateResult::NeedBlocks`].
    async fn apply_update(
        &self,
        file: &FileInfo,
        remote_device: &DeviceId,
    ) -> Result<UpdateResult, StorageError>;

    /// Current local sequence number for this folder.
    async fn local_sequence(&self) -> Result<Sequence, StorageError>;

    /// Last known remote state for a device in this folder.
    async fn remote_state(&self, device: &DeviceId) -> Result<RemoteIndexState, StorageError>;

    /// Persist the remote state for a device in this folder.
    async fn set_remote_state(
        &self,
        device: &DeviceId,
        state: RemoteIndexState,
    ) -> Result<(), StorageError>;

    /// Persist the outcome of a conflict resolution.
    ///
    /// Called by the engine after a [`ConflictResolver`](crate::ConflictResolver)
    /// decides the winner. The implementation stores the winner at its original
    /// path. If `loser_path` is `Some(path)`, the loser is also stored at that
    /// path; if `None`, the loser is discarded without a backup.
    async fn resolve_conflict(
        &self,
        winner: &FileInfo,
        loser: &FileInfo,
        loser_path: Option<&str>,
    ) -> Result<(), StorageError>;

    /// Persist a block received from a peer.
    ///
    /// Called by the engine when a Response to a block Request is received.
    /// The `hash` identifies the block within the file; `data` is the raw
    /// block content.
    async fn store_block(
        &self,
        name: &str,
        offset: i64,
        hash: &[u8],
        data: Bytes,
    ) -> Result<(), StorageError>;

    /// Signal that all blocks for a file have been received.
    ///
    /// The implementation atomically promotes the file from staging (inbox)
    /// to the committed index. Must be idempotent — calling on an already
    /// committed file or a file with no staged entry is a no-op.
    ///
    /// Returns the committed [`FileInfo`] with its locally-assigned sequence
    /// number on success, or `None` if this was a no-op (not staged, or
    /// version mismatch).
    async fn complete_file(
        &self,
        name: &str,
        expected_version: Option<&crate::proto::bep::Vector>,
    ) -> Result<Option<FileInfo>, StorageError>;

    /// Check whether a block with the given hash and size is already stored,
    /// and if so, record that it is now also used by the given file.
    ///
    /// Returns `true` if the block was found and linked (no fetch needed),
    /// or `false` if the block is missing and must be requested from a peer.
    ///
    /// This is used by the sync engine to skip downloading blocks that are
    /// already present locally (e.g. after a file move or rename). For backends
    /// with GC, this method must ensure the block is protected from deletion
    /// as long as the new file exists.
    async fn reuse_block(
        &self,
        name: &str,
        offset: i64,
        hash: &[u8],
        size: i32,
    ) -> Result<bool, StorageError>;

    /// Check whether a block with the given hash and size is already stored.
    ///
    /// Used to skip requesting blocks the backend already has (e.g. after a
    /// file move/rename).
    async fn has_block(
        &self,
        file: &str,
        offset: i64,
        hash: &[u8],
        size: i32,
    ) -> Result<bool, StorageError>;
}

/// Helper trait for test inspection and setup.
///
/// Provides read-access to internal storage state and methods to pre-populate
/// storage during tests. Gated to ensure zero impact on production code.
#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
pub trait StorageInspectorForTests: StorageFolder {
    /// The epoch type used to key inbox entries.
    ///
    /// Use `()` for in-memory backends that have no epoch concept,
    /// and `bepository_lock::Epoch` for SlateDB-backed storage.
    type Epoch;

    /// Directly insert a file into the committed index.
    async fn insert_file(&self, file: FileInfo);

    /// Directly insert raw block data.
    async fn insert_block(&self, name: &str, offset: i64, data: Bytes);

    /// Get a file from the committed index.
    async fn get_file(&self, name: &str) -> Option<FileInfo>;

    /// Get a file from the inbox/staging area.
    async fn get_inbox_file(&self, epoch: Self::Epoch, name: &str) -> Option<FileInfo>;

    /// Get raw block data for a file at a specific offset.
    async fn get_block(&self, name: &str, offset: i64) -> Option<Bytes>;
}

/// Storage backend trait — decouples BEP protocol logic from storage implementation.
///
/// Resolves folder labels to [`StorageFolder`] handles. The engine calls
/// [`folder`](Self::folder) once per folder and then operates on the returned
/// handle, avoiding repeated label resolution.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// The per-folder handle type returned by [`folder`](Self::folder).
    type Folder: StorageFolder;

    /// The backend-specific storage identifier for a folder.
    ///
    /// For in-memory backends this is the BEP [`FolderId`] itself.
    /// For SlateDB-backed storage this is the `folder_<BASE32>` directory
    /// name used as the path prefix within the object store.
    type StorageKey: Clone + Send + Sync + 'static;

    /// Resolve a BEP folder ID to a folder handle.
    async fn folder(&self, id: FolderId) -> Result<Self::Folder, StorageError>;

    /// Return all known folder IDs and their storage keys.
    ///
    /// Used at connection start to build an accurate initial ClusterConfig that
    /// includes folders registered during prior connections, not just the folders
    /// the engine was created with.  The default implementation returns an empty
    /// list, suitable for in-memory backends that don't track folder registration.
    async fn list_folders(
        &self,
    ) -> Result<Vec<(FolderId, FolderLabel, Self::StorageKey)>, StorageError> {
        Ok(Vec::new())
    }

    /// Ensure all listed folders exist, registering new ones and updating labels for existing ones.
    ///
    /// Returns a parallel `Vec<bool>` where `true` means the folder was newly created
    /// and `false` means it already existed.
    ///
    /// Backends that require explicit registration should override this to perform all
    /// registrations in a single atomic metadata write. The default implementation is a no-op
    /// (returns all `false`), suitable for in-memory backends that accept any folder id.
    async fn ensure_folders(
        &self,
        folders: &[(FolderId, FolderLabel)],
    ) -> Result<Vec<bool>, StorageError> {
        Ok(vec![false; folders.len()])
    }
}
