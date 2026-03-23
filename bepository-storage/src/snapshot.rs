// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Read-only snapshot filesystem abstraction.
//!
//! `SnapshotFs` provides a checkpoint-scoped, read-only view of a folder's
//! file tree. Each storage backend provides its own concrete [`SnapshotRef`]
//! type; the trait only exposes what consumers need to see.

use async_trait::async_trait;
use bepository_bep::ids::FolderLabelRef;
use bytes::Bytes;
use chrono::{DateTime, Utc};

/// Consumer-visible view of a (folder, checkpoint) pair.
///
/// Returned by [`SnapshotFs::list_snapshots`] and passed opaquely back to the
/// other methods. Implementations may embed private routing data in their
/// concrete type.
pub trait SnapshotRef: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Human-readable folder label (e.g. `"photos"`).
    fn folder_label(&self) -> &FolderLabelRef;
    /// UTC timestamp when the checkpoint was created.
    fn create_time(&self) -> DateTime<Utc>;
}

/// A single entry returned by [`SnapshotFs::read_dir`] or
/// [`SnapshotFs::file_metadata`].
#[derive(Debug)]
pub enum FsEntry {
    File {
        name: String,
        size: u64,
        modified: DateTime<Utc>,
    },
    Dir {
        name: String,
    },
}

impl FsEntry {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            FsEntry::File { name, .. } | FsEntry::Dir { name } => name,
        }
    }

    #[must_use]
    pub fn is_dir(&self) -> bool {
        matches!(self, FsEntry::Dir { .. })
    }
}

/// Errors returned by `SnapshotFs` methods.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("not found")]
    NotFound,
    #[error("path is not a file")]
    NotAFile,
    #[error("path is not a directory")]
    NotADir,
    #[error("io error: {0}")]
    Io(String),
}

/// Read-only, checkpoint-scoped view of a folder's file tree.
///
/// All methods are lock-free: they use `DbReader` instances (pinned to a
/// checkpoint) and `read_meta_unlocked`. Safe to call without holding the
/// distributed lock.
///
/// The associated type `Ref` is the backend's concrete snapshot handle. Each
/// implementation embeds whatever routing data it needs privately in that type.
#[async_trait]
pub trait SnapshotFs: Send + Sync + 'static {
    /// The concrete snapshot handle type provided by this implementation.
    type Ref: SnapshotRef;

    /// List all available (folder, checkpoint) pairs, newest-first within each
    /// folder.
    async fn list_snapshots(&self) -> Result<Vec<Self::Ref>, SnapshotError>;

    /// List immediate children of `path` within the snapshot.
    ///
    /// `path` is a `/`-separated relative path with no leading slash.
    /// An empty string means the root of the file tree.
    /// Returns `FsEntry::File` or `FsEntry::Dir` entries.
    /// Deleted files are excluded.
    async fn read_dir(&self, snap: &Self::Ref, path: &str) -> Result<Vec<FsEntry>, SnapshotError>;

    /// Return metadata for a single path within the snapshot.
    ///
    /// Returns `SnapshotError::NotFound` if the path does not exist as a file
    /// or virtual directory. Returns `FsEntry::Dir` for virtual directories
    /// (directories implied by file path separators, with no explicit index
    /// entry of their own).
    async fn file_metadata(&self, snap: &Self::Ref, path: &str) -> Result<FsEntry, SnapshotError>;

    /// Read up to `len` bytes from `path` starting at `offset`.
    ///
    /// Returns fewer bytes than requested only at end-of-file.
    async fn read_bytes(
        &self,
        snap: &Self::Ref,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Bytes, SnapshotError>;
}
